const http = require('http');
const { WebSocketServer } = require('ws');
const fs = require('fs');
const path = require('path');

const PORT = process.env.PORT || 10000;
const publicDir = path.join(__dirname, 'public');

const MIME = {
  '.html': 'text/html', '.js': 'text/javascript', '.css': 'text/css',
  '.json': 'application/json', '.png': 'image/png', '.jpg': 'image/jpeg',
};

const server = http.createServer((req, res) => {
  let urlPath = req.url.split('?')[0];
  if (urlPath === '/') urlPath = '/index.html';
  const filePath = path.join(publicDir, urlPath);
  if (!filePath.startsWith(publicDir)) { res.writeHead(403); res.end(); return; }
  fs.readFile(filePath, (err, data) => {
    if (err) { res.writeHead(404); res.end('Not found'); return; }
    const ext = path.extname(filePath).toLowerCase();
    res.writeHead(200, { 'Content-Type': MIME[ext] || 'application/octet-stream' });
    res.end(data);
  });
});

const wss = new WebSocketServer({ server, path: '/ws' });

const rooms = new Map();
let nextPlayerId = 1;

function genRoomId() {
  const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZ';
  let id = '';
  for (let i = 0; i < 4; i++) id += chars[Math.floor(Math.random() * chars.length)];
  if (rooms.has(id)) return genRoomId();
  return id;
}

function broadcast(room, msg) {
  room.players.forEach(p => { if (p.ws && p.ws.readyState === 1) p.ws.send(JSON.stringify(msg)); });
}

function broadcastState(room) {
  const snapshot = {
    id: room.id,
    state: room.state,
    players: room.players.map(p => ({
      id: p.id, name: p.name, isDM: p.isDM, customRole: p.customRole || '',
      ready: p.ready || false, stats: p.stats || null,
      sideQuest: p.sideQuest || null
    })),
    scenario: room.scenario,
    log: room.log,
    maxPlayers: room.maxPlayers,
    turnOrder: room.turnOrder || [],
    currentTurn: room.currentTurn || 0,
    statPoints: room.statPoints,
    outlines: room.outlines || null,
    votes: room.votes || {},
    publicNotes: room.publicNotes || '',
  };
  broadcast(room, { type: 'state', data: snapshot });
}

wss.on('connection', (ws) => {
  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }
    let room = null, me = null;
    for (const r of rooms.values()) {
      const p = r.players.find(x => x.ws === ws);
      if (p) { room = r; me = p; break; }
    }

    switch (msg.type) {
      case 'create': {
        const id = genRoomId();
        rooms.set(id, {
          id, state: 'lobby', players: [], log: [],
          maxPlayers: msg.maxPlayers || 4,
          apiKey: msg.apiKey || '',
          aiBaseUrl: 'https://api.deepseek.com',
          aiModel: 'deepseek-chat',
          outlines: null, votes: {},
          statPoints: 10,
          turnOrder: [], currentTurn: 0,
          publicNotes: '',
        });
        const dm = { id: nextPlayerId++, name: msg.name || 'DM', isDM: true, ws, ready: true, stats: null };
        rooms.get(id).players.push(dm);
        ws.send(JSON.stringify({ type: 'created', roomId: id, myId: dm.id }));
        broadcastState(rooms.get(id));
        break;
      }
      case 'join': {
        room = rooms.get(msg.roomId);
        if (!room) { ws.send(JSON.stringify({ type: 'error', msg: '房间不存在' })); return; }
        if (room.players.length >= room.maxPlayers) { ws.send(JSON.stringify({ type: 'error', msg: '房间满了' })); return; }
        const p = {
          id: nextPlayerId++, name: msg.name || '玩家', isDM: false, ws,
          customRole: msg.role || '', ready: false, stats: null, sideQuest: null
        };
        room.players.push(p);
        room.log.push({ type: 'system', text: '👋 ' + p.name + ' 加入了房间', time: Date.now() });
        ws.send(JSON.stringify({ type: 'joined', roomId: room.id, myId: p.id }));
        broadcastState(room);
        break;
      }
      case 'player_ready': {
        if (!me || me.isDM) return;
        me.ready = !me.ready;
        broadcastState(room);
        break;
      }
      case 'set_stats': {
        if (!me) return;
        me.stats = msg.stats;
        broadcastState(room);
        break;
      }
      case 'set_stat_points': {
        if (!me || !me.isDM) return;
        room.statPoints = msg.points || 10;
        broadcastState(room);
        break;
      }
      case 'start_outlines': {
        if (!me || !me.isDM) return;
        room.state = 'outline';
        room.outlines = {};
        room.log.push({ type: 'system', text: '✏️ 大家写自己想玩的大纲，提交后投票', time: Date.now() });
        broadcastState(room);
        break;
      }
      case 'submit_outline': {
        if (!me || me.isDM) return;
        room.outlines[me.id] = { name: me.name, text: msg.text };
        room.log.push({ type: 'system', text: '📝 ' + me.name + ' 提交了大纲', time: Date.now() });
        const players = room.players.filter(p => !p.isDM);
        if (Object.keys(room.outlines).length >= players.length) {
          room.state = 'outline_voting';
          room.votes = {};
        }
        broadcastState(room);
        break;
      }
      case 'vote_outline': {
        if (!me) return;
        room.votes[me.id] = msg.outlineId;
        broadcastState(room);
        break;
      }
      case 'gen_opening': {
        if (!me || !me.isDM) return;
        const counts = {};
        Object.values(room.votes).forEach(i => counts[i] = (counts[i]||0)+1);
        let bestId = null, max = 0;
        Object.entries(counts).forEach(([i, c]) => { if(c > max) { max = c; bestId = i; } });
        const selected = room.outlines[bestId];
        room.log.push({ type: 'system', text: '🎲 选中了 ' + selected.name + ' 的大纲，生成开场白...', time: Date.now() });
        broadcastState(room);
        const pCount = room.players.filter(p => !p.isDM).length;
        generateFromOutline(room, selected.text, pCount).then(() => {
          // 随机回合顺序
          const playerIds = room.players.filter(p => !p.isDM).map(p => p.id);
          for (let i = playerIds.length - 1; i > 0; i--) {
            const j = Math.floor(Math.random() * (i + 1));
            [playerIds[i], playerIds[j]] = [playerIds[j], playerIds[i]];
          }
          room.turnOrder = playerIds;
          room.currentTurn = 0;
          room.state = 'playing';
          const firstName = room.players.find(p => p.id === playerIds[0])?.name || '';
          room.log.push({ type: 'system', text: '🔄 回合顺序：' + playerIds.map(id => room.players.find(p=>p.id===id)?.name).join(' → ') + '，轮到 ' + firstName + '！', time: Date.now() });
          broadcastState(room);
        }).catch(err => {
          room.log.push({ type: 'system', text: '生成失败: ' + err.message, time: Date.now() });
          broadcastState(room);
        });
        break;
      }
      case 'next_turn': {
        if (!me || !me.isDM) return;
        room.currentTurn = (room.currentTurn + 1) % room.turnOrder.length;
        const nextId = room.turnOrder[room.currentTurn];
        const nextName = room.players.find(p => p.id === nextId)?.name || '';
        room.log.push({ type: 'system', text: '🔄 轮到 ' + nextName + ' 了', time: Date.now() });
        broadcastState(room);
        break;
      }
      case 'dm_scene': {
        if (!me || !me.isDM) return;
        room.log.push({ type: 'dm', text: msg.text, time: Date.now() });
        broadcastState(room);
        break;
      }
      case 'player_action': {
        if (!me || me.isDM) return;
        room.log.push({ type: 'action', player: me.name, text: msg.text, time: Date.now() });
        broadcastState(room);
        if (room.apiKey) {
          judgeAction(room, me, msg.text).catch(() => {});
        }
        break;
      }
      case 'dice': {
        if (!me) return;
        const sides = msg.sides || 20;
        const roll = Math.floor(Math.random() * sides) + 1;
        room.log.push({ type: 'dice', player: me.name, sides, roll, time: Date.now() });
        broadcastState(room);
        if (room.apiKey && !me.isDM) {
          afterDice(room, me, sides, roll).catch(() => {});
        }
        break;
      }
      case 'update_notes': {
        room.publicNotes = msg.text || '';
        broadcastState(room);
        break;
      }
    }
  });

  ws.on('close', () => {
    for (const room of rooms.values()) {
      const p = room.players.find(x => x.ws === ws);
      if (p) { p.ws = null; broadcastState(room); break; }
    }
  });
});

async function judgeAction(room, player, text) {
  const recent = room.log.slice(-10).map(l => {
    if (l.type === 'dm' || l.type === 'dm_reply') return '【DM】' + l.text;
    if (l.type === 'action') return '【' + l.player + '】' + l.text;
    return '';
  }).filter(Boolean).join('\n');

  const pstats = player.stats || { str: 5, agi: 5, int: 5, wil: 5, con: 5 };
  const prompt = `你是跑团DM。玩家【${player.name}】说：${text}

他的属性：力量${pstats.str} 敏捷${pstats.agi} 智力${pstats.int} 理智${pstats.wil} 体质${pstats.con}

最近剧情：
${recent}

判断：这个行动需要掷骰吗？需要的话难度多少（d20要大于多少才算成功）？
只输出JSON：{"needDice":true/false,"difficulty":数字,"reason":"一句话理由"}`;

  try {
    const res = await fetch(room.aiBaseUrl + '/chat/completions', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Authorization': 'Bearer ' + room.apiKey },
      body: JSON.stringify({ model: room.aiModel, messages: [{ role: 'user', content: prompt }], temperature: 0.3, max_tokens: 200 }),
    });
    const data = await res.json();
    let t = data.choices[0].message.content.trim();
    const m = t.match(/\{[\s\S]*\}/);
    if (m) {
      const j = JSON.parse(m[0]);
      if (j.needDice) {
        room.log.push({ type: 'system', text: '🎲 需要检定！难度：d20大于' + j.difficulty + ' （' + j.reason + '）', time: Date.now() });
        room.pendingDice = { difficulty: j.difficulty, playerId: player.id };
      } else {
        room.log.push({ type: 'system', text: '✅ 不需要掷骰：' + j.reason, time: Date.now() });
      }
    }
  } catch {}
  broadcastState(room);
}

async function afterDice(room, player, sides, roll) {
  const pending = room.pendingDice;
  if (!pending || pending.playerId !== player.id) return;
  const success = roll > pending.difficulty;
  const recent = room.log.slice(-10).map(l => {
    if (l.type === 'dm' || l.type === 'dm_reply') return '【DM】' + l.text;
    if (l.type === 'action') return '【' + l.player + '】' + l.text;
    if (l.type === 'dice') return '【掷骰】' + l.player + ' d' + l.sides + '=' + l.roll;
    return '';
  }).filter(Boolean).join('\n');

  const pstats = player.stats || { str: 5, agi: 5, int: 5, wil: 5, con: 5 };
  const prompt = `你是跑团DM。玩家【${player.name}】掷了d${sides}=${roll}，难度需要${pending.difficulty}，结果${success?'成功':'失败'}。
属性：力量${pstats.str} 敏捷${pstats.agi} 智力${pstats.int} 理智${pstats.wil} 体质${pstats.con}

最近剧情：
${recent}

用大白话写结果（100字内），告诉玩家发生了什么。不要文艺腔。`;

  try {
    const res = await fetch(room.aiBaseUrl + '/chat/completions', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Authorization': 'Bearer ' + room.apiKey },
      body: JSON.stringify({ model: room.aiModel, messages: [{ role: 'user', content: prompt }], temperature: 0.8, max_tokens: 300 }),
    });
    const data = await res.json();
    const result = data.choices[0].message.content.trim();
    // 发给DM审核，不直接发出去
    const dm = room.players.find(p => p.isDM);
    if (dm && dm.ws) {
      dm.ws.send(JSON.stringify({ type: 'dm_review', text: result, player: player.name }));
    }
  } catch {}
  room.pendingDice = null;
  broadcastState(room);
}

async function generateFromOutline(room, outlineText, playerCount) {
  if (!room.apiKey) throw new Error('未设置API Key');
  const prompt = `你是跑团DM。玩家选了这个大纲：${outlineText}

为${playerCount}人跑团生成开场白和故事背景。
要求：
- 不要写"你们来到了..."这种介绍式开场
- 直接从事件中间开始，一上来就有事发生
- 大白话，不要文艺腔

输出JSON：
{
  "title": "剧本名称",
  "background": "故事背景（150字）",
  "mainGoal": "共同目标（一句话，所有人都要完成的主线）",
  "opening": "开场白（100字，直接从事件中间开始）",
  "characters": [
    {"name":"角色名","role":"具体身份","description":"你是谁","goal":"你想干什么","sideQuest":"个人支线任务（一个秘密目标）"}
  ],
  "npcs": [{"name":"NPC名","description":"描述","relation":"关系"}],
  "clues": ["线索1","线索2","线索3"]
}
characters正好${playerCount}个，每个都有独立的sideQuest支线任务。`;

  const res = await fetch(room.aiBaseUrl + '/chat/completions', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Authorization': 'Bearer ' + room.apiKey },
    body: JSON.stringify({ model: room.aiModel, messages: [{ role: 'user', content: prompt }], temperature: 0.9, max_tokens: 3000 }),
  });
  if (!res.ok) throw new Error('API错误');
  const data = await res.json();
  let text = data.choices[0].message.content.trim();
  // 清理控制字符
  text = text.replace(/[\x00-\x1F\x7F]/g, '');
  const match = text.match(/\{[\s\S]*\}/);
  if (match) text = match[0];
  room.scenario = JSON.parse(text);
  // 把支线任务分给每个玩家
  room.players.forEach(p => {
    if (p.isDM) return;
    const idx = room.players.filter(x => !x.isDM).indexOf(p);
    p.sideQuest = room.scenario.characters?.[idx]?.sideQuest || null;
  });
  room.log.unshift({ type: 'dm', text: room.scenario.opening || '', time: Date.now() });
}

server.listen(PORT, () => {
  console.log(`骰语 DiceTale v1.5.0 已启动`);
});
