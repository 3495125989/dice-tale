const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const ROOT = path.join(__dirname, 'public');

// ========== 房间状态管理 ==========
const rooms = new Map(); // roomId -> room

function genRoomId() {
  const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
  let id = '';
  for (let i = 0; i < 4; i++) id += chars[Math.floor(Math.random() * chars.length)];
  return rooms.has(id) ? genRoomId() : id;
}

function createRoom() {
  const id = genRoomId();
  rooms.set(id, {
    id,
    state: 'lobby', // lobby | voting | preview | playing | ended
    players: [],
    scenario: null,
    log: [],
    turn: 0,
    maxPlayers: 4,
    apiKey: '',
    aiBaseUrl: 'https://api.deepseek.com',
    aiModel: 'deepseek-chat',
    options: null,
    votes: {},
    outlines: null,
    statPoints: 10,
  });
  return rooms.get(id);
}

function broadcast(room, msg) {
  const data = JSON.stringify(msg);
  room.players.forEach(p => {
    if (p.ws && p.ws.readyState === 1) p.ws.send(data);
  });
}

function broadcastState(room) {
  const snapshot = {
    id: room.id,
    state: room.state,
    players: room.players.map(p => ({
      id: p.id, name: p.name, isDM: p.isDM, character: p.character, customRole: p.customRole || '', ready: p.ready || false, stats: p.stats || null
    })),
    scenario: room.scenario,
    log: room.log,
    maxPlayers: room.maxPlayers,
    turn: room.turn,
    statPoints: room.statPoints,
    outlines: room.outlines || null,
    votes: room.votes || {},
  };
  broadcast(room, { type: 'state', data: snapshot });
}

// ========== HTTP 静态服务 ==========
const MIME = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'application/javascript',
  '.css': 'text/css',
  '.json': 'application/json',
  '.png': 'image/png',
  '.jpg': 'image/jpeg',
  '.svg': 'image/svg+xml',
};

const server = http.createServer((req, res) => {
  let urlPath = req.url.split('?')[0];
  if (urlPath === '/') urlPath = '/index.html';
  const filePath = path.join(ROOT, urlPath);
  if (!filePath.startsWith(ROOT)) { res.writeHead(403); res.end(); return; }
  fs.readFile(filePath, (err, data) => {
    if (err) { res.writeHead(404); res.end('Not Found'); return; }
    const ext = path.extname(filePath);
    res.writeHead(200, { 'Content-Type': MIME[ext] || 'application/octet-stream' });
    res.end(data);
  });
});

// ========== WebSocket ==========
const wss = new WebSocketServer({ server, path: '/ws' });
let nextPid = 1;

wss.on('connection', (ws) => {
  let room = null;
  let me = null;

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

    switch (msg.type) {
      // 创建房间
      case 'create': {
        room = createRoom();
        room.apiKey = msg.apiKey || '';
        room.aiBaseUrl = msg.aiBaseUrl || room.aiBaseUrl;
        room.aiModel = msg.aiModel || room.aiModel;
        room.maxPlayers = msg.maxPlayers || 4;
        me = { ws, id: nextPid++, name: msg.name || 'DM', isDM: true, character: null };
        room.players.push(me);
        ws.send(JSON.stringify({ type: 'created', roomId: room.id, myId: me.id }));
        broadcastState(room);
        break;
      }
      // 加入房间
      case 'join': {
        room = rooms.get(msg.roomId);
        if (!room) { ws.send(JSON.stringify({ type: 'error', msg: '房间不存在' })); return; }
        if (room.players.length >= room.maxPlayers) { ws.send(JSON.stringify({ type: 'error', msg: '房间已满' })); return; }
        me = { ws, id: nextPid++, name: msg.name || '玩家', isDM: false, customRole: msg.role || '', character: null };
        room.players.push(me);
        ws.send(JSON.stringify({ type: 'joined', roomId: room.id, myId: me.id }));
        broadcastState(room);
        break;
      }
      // 设置角色卡
      case 'set_character': {
        if (me) me.character = msg.character;
        broadcastState(room);
        break;
      }
      // 设置属性
      case 'set_stats': {
        if (me && !me.isDM) { me.stats = msg.stats; }
        broadcastState(room);
        break;
      }
      // DM设置总点数
      case 'set_stat_points': {
        if (me && me.isDM) { room.statPoints = msg.points; }
        broadcastState(room);
        break;
      }
      // DM描述场景
      case 'dm_scene': {
        if (!me || !me.isDM) return;
        room.log.push({ type: 'dm', text: msg.text, time: Date.now() });
        broadcastState(room);
        break;
      }
      // 玩家行动 → 自动AI处理
      case 'player_action': {
        if (!me || me.isDM) return;
        room.log.push({ type: 'action', player: me.name, text: msg.text, time: Date.now() });
        broadcastState(room);
        // 自动调AI判断
        if (room.apiKey) {
          aiProcessAction(room, me, msg.text).catch(err => {
            room.log.push({ type: 'system', text: 'AI处理失败: ' + err.message, time: Date.now() });
            broadcastState(room);
          });
        }
        break;
      }
      // DM回复/裁定
      case 'dm_reply': {
        if (!me || !me.isDM) return;
        room.log.push({ type: 'dm_reply', text: msg.text, time: Date.now() });
        broadcastState(room);
        break;
      }
      // 掷骰 → 自动AI根据结果续写
      case 'dice': {
        if (!me) return;
        const sides = msg.sides || 20;
        const roll = Math.floor(Math.random() * sides) + 1;
        room.log.push({
          type: 'dice',
          player: me.name,
          sides, roll,
          time: Date.now()
        });
        broadcastState(room);
        // 掷骰后自动AI续写
        if (room.apiKey) {
          aiDiceResult(room, me, sides, roll).catch(() => {});
        }
        break;
      }
      // 开始游戏
      case 'start': {
        if (!me || !me.isDM) return;
        room.state = 'playing';
        room.log.push({ type: 'system', text: '🎮 游戏开始！', time: Date.now() });
        broadcastState(room);
        break;
      }
      // 玩家点准备
      case 'player_ready': {
        if (!me || me.isDM) return;
        me.ready = !me.ready;
        broadcastState(room);
        break;
      }
      // DM开始（大家准备好后）
      case 'start_outlines': {
        if (!me || !me.isDM) return;
        room.state = 'outline';
        room.outlines = {};
        room.log.push({ type: 'system', text: '✏️ 大家写自己想玩的大纲，提交后投票', time: Date.now() });
        broadcastState(room);
        break;
      }
      // 玩家提交大纲
      case 'submit_outline': {
        if (!me || me.isDM) return;
        room.outlines[me.id] = { name: me.name, text: msg.text };
        room.log.push({ type: 'system', text: '📝 ' + me.name + ' 提交了大纲', time: Date.now() });
        // 检查是否都提交了
        const players = room.players.filter(p => !p.isDM);
        if (Object.keys(room.outlines).length >= players.length) {
          room.state = 'outline_voting';
          room.votes = {};
        }
        broadcastState(room);
        break;
      }
      // 投票选大纲
      case 'vote_outline': {
        if (!me) return;
        room.votes[me.id] = msg.outlineId;
        broadcastState(room);
        break;
      }
      // DM根据选中大纲生成开场白
      case 'gen_opening': {
        if (!me || !me.isDM) return;
        // 统计票数
        const counts = {};
        Object.values(room.votes).forEach(i => counts[i] = (counts[i]||0)+1);
        let bestId = null, max = 0;
        Object.entries(counts).forEach(([i, c]) => { if(c > max) { max = c; bestId = i; } });
        const selected = room.outlines[bestId];
        room.log.push({ type: 'system', text: '🎲 选中了 ' + selected.name + ' 的大纲，正在生成开场白...', time: Date.now() });
        broadcastState(room);
        const pCount = room.players.filter(p => !p.isDM).length;
        generateFromOutline(room, selected.text, pCount).then(() => {
          room.state = 'playing';
          broadcastState(room);
        }).catch(err => {
          room.log.push({ type: 'system', text: '生成失败: ' + err.message, time: Date.now() });
          broadcastState(room);
        });
        break;
      }
        const voted = Object.keys(room.votes).length;
      // 结束游戏
      case 'end': {
        if (!me || !me.isDM) return;
        room.state = 'ended';
        broadcastState(room);
        break;
      }
      // AI生成剧本
      case 'ai_scenario': {
        if (!me || !me.isDM) return;
        const players = room.players.filter(p => !p.isDM).length;
        generateScenario(room, msg.theme, players).then(() => {
          broadcastState(room);
        }).catch(err => {
          ws.send(JSON.stringify({ type: 'error', msg: 'AI生成失败: ' + err.message }));
        });
        break;
      }
      // AI DM回应
      case 'ai_dm_reply': {
        if (!me || !me.isDM) return;
        aiDMReply(room, msg.playerName, msg.actionText).then(reply => {
          room.log.push({ type: 'dm_reply', text: reply, time: Date.now() });
          broadcastState(room);
        }).catch(err => {
          ws.send(JSON.stringify({ type: 'error', msg: 'AI DM失败: ' + err.message }));
        });
        break;
      }
    }
  });

  ws.on('close', () => {
    // 玩家断开连接不删除房间，只标记离线，防止Render免费版不稳定导致房间丢失
    if (room && me) {
      me.ws = null;
      broadcastState(room);
    }
  });

  // 心跳：每30秒ping一次，防止Render断开空闲连接
  const heartbeat = setInterval(() => {
    if (ws.readyState === 1) {
      try { ws.ping(); } catch {}
    }
  }, 30000);
  ws.on('close', () => clearInterval(heartbeat));
});

// ========== AI 剧本生成 ==========
async function generateScenario(room, theme, playerCount) {
  if (!room.apiKey) throw new Error('未设置 API Key');

  const prompt = `你是一个TRPG跑团主持人。请为一个${playerCount}人跑团生成一个剧本。

主题方向：${theme || '自由创作'}

请输出JSON格式（不要有多余文字）：
{
  "title": "剧本名称",
  "background": "故事背景描述（200字左右）",
  "setting": "当前场景描述（100字左右）",
  "characters": [
    {"name": "角色名", "role": "角色定位（如：调查员/幸存者/嫌疑人）", "description": "角色背景与秘密（100字左右）", "goal": "角色的个人目标"}
  ],
  "npcs": [
    {"name": "NPC名", "description": "NPC描述", "relation": "与玩家的关系"}
  ],
  "clues": ["线索1描述", "线索2描述", "线索3描述"],
  "winCondition": "胜利条件",
  "twist": "剧情转折（DM专用，不告诉玩家）"
}

请确保characters数组正好有${playerCount}个角色。`;

  const res = await fetch(room.aiBaseUrl + '/chat/completions', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': 'Bearer ' + room.apiKey,
    },
    body: JSON.stringify({
      model: room.aiModel,
      messages: [{ role: 'user', content: prompt }],
      temperature: 0.8,
      max_tokens: 3000,
    }),
  });

  if (!res.ok) throw new Error('API错误 ' + res.status);
  const data = await res.json();
  let text = data.choices[0].message.content.trim();
  // 提取 JSON
  const match = text.match(/\{[\s\S]*\}/);
  if (match) text = match[0];
  const scenario = JSON.parse(text);
  room.scenario = scenario;
  room.log.push({ type: 'system', text: `剧本《${scenario.title}》已生成`, time: Date.now() });
}

async function aiDMReply(room, playerName, actionText) {
  if (!room.apiKey) throw new Error('未设置 API Key');

  const logContext = room.log.slice(-10).map(l => {
    if (l.type === 'dm') return `【主持人】${l.text}`;
    if (l.type === 'action') return `【${l.player}】${l.text}`;
    if (l.type === 'dm_reply') return `【主持人】${l.text}`;
    if (l.type === 'dice') return `【${l.player}】掷出 d${l.sides} = ${l.roll}`;
    return '';
  }).filter(Boolean).join('\n');

  const prompt = `你是一个TRPG跑团主持人(DM)。请根据当前剧情，回应玩家的行动。

当前剧本背景：${room.scenario?.background || '无'}
当前场景：${room.scenario?.setting || '无'}

最近剧情：
${logContext}

玩家【${playerName}】的行动：${actionText}

请以DM视角描述结果（100-200字），推动剧情发展。如果行动需要判定，可以暗示是否成功。直接输出描述内容，不要说"作为DM"之类的话。`;

  const res = await fetch(room.aiBaseUrl + '/chat/completions', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': 'Bearer ' + room.apiKey,
    },
    body: JSON.stringify({
      model: room.aiModel,
      messages: [{ role: 'user', content: prompt }],
      temperature: 0.9,
      max_tokens: 500,
    }),
  });

  if (!res.ok) throw new Error('API错误 ' + res.status);
  const data = await res.json();
  return data.choices[0].message.content.trim();
}

// 玩家行动后AI自动判断
async function aiProcessAction(room, player, actionText) {
  if (!room.apiKey) return;
  const pstats = player.stats || { str:5, agi:5, int:5, wil:5, con:5 };
  const logContext = room.log.slice(-8).map(l => {
    if (l.type === 'dm') return `【主持人】${l.text}`;
    if (l.type === 'action') return `【${l.player}】${l.text}`;
    if (l.type === 'dm_reply') return `【主持人】${l.text}`;
    if (l.type === 'dice') return `【${l.player}】掷出 d${l.sides}=${l.roll}`;
    return '';
  }).filter(Boolean).join('\n');

  const prompt = `你是TRPG跑团DM。玩家【${player.name}】采取了行动：${actionText}

玩家属性：力量${pstats.str} 敏捷${pstats.agi} 智力${pstats.int} 意志${pstats.wil} 体质${pstats.con}
当前场景：${room.scenario?.setting || '未知'}
最近剧情：
${logContext}

请判断这个行动并输出JSON：
{
  "needRoll": true/false,
  "difficulty": 数值(需要d20大于等于多少，8-15),
  "statCheck": "需要哪个属性检查(力量/敏捷/智力/意志/体质)或null",
  "statNeeded": 数值(该属性需要多少才能自动成功),
  "dmText": "DM要描述的内容(如果不需要掷骰就直接写结果，如果需要掷骰就描述玩家尝试做什么)",
  "options": ["A选项","B选项","C选项"]
}

规则：
- 简单的事（看一眼、说话）needRoll=false
- 有难度的事（撬锁、追踪、战斗）needRoll=true，difficulty看难度
- 如果玩家属性够高（>=statNeeded），可以不用掷骰直接成功
- options给2-3个玩家接下来可以做的事`;

  const res = await fetch(room.aiBaseUrl + '/chat/completions', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Authorization': 'Bearer ' + room.apiKey },
    body: JSON.stringify({ model: room.aiModel, messages: [{ role: 'user', content: prompt }], temperature: 0.8, max_tokens: 500 }),
  });
  if (!res.ok) return;
  const data = await res.json();
  let text = data.choices[0].message.content.trim();
  const match = text.match(/\{[\s\S]*\}/);
  if (match) text = match[0];
  let result;
  try { result = JSON.parse(text); } catch { return; }

  if (result.needRoll) {
    // 需要掷骰：DM描述+提示检定
    room.log.push({ type: 'dm_reply', text: result.dmText || '', time: Date.now() });
    room.log.push({ type: 'system', text: `🎲 需要 d20 检定（难度：${result.difficulty}）${result.statCheck ? ' · ' + result.statCheck : ''}`, time: Date.now() });
    room.pendingDice = { difficulty: result.difficulty, stat: result.statCheck, player: player.name };
  } else {
    // 不需要掷骰直接描述
    room.log.push({ type: 'dm_reply', text: result.dmText || '', time: Date.now() });
  }
  if (result.options?.length) {
    room.log.push({ type: 'system', text: '💡 可选：' + result.options.map((o,i)=>String.fromCharCode(65+i)+'. '+o).join('  '), time: Date.now() });
  }
  broadcastState(room);
}

// 掷骰后AI根据结果续写
async function aiDiceResult(room, player, sides, roll) {
  if (!room.apiKey) return;
  const pending = room.pendingDice;
  const success = pending ? roll >= pending.difficulty : roll >= 10;
  const pstats = player.stats || { str:5, agi:5, int:5, wil:5, con:5 };
  const logContext = room.log.slice(-8).map(l => {
    if (l.type === 'dm') return `【主持人】${l.text}`;
    if (l.type === 'action') return `【${l.player}】${l.text}`;
    if (l.type === 'dm_reply') return `【主持人】${l.text}`;
    return '';
  }).filter(Boolean).join('\n');

  const prompt = `你是TRPG跑团DM。玩家【${player.name}】掷了d${sides}=${roll}，难度需要${pending?.difficulty||10}，结果${success?'成功':'失败'}。
玩家属性：力量${pstats.str} 敏捷${pstats.agi} 智力${pstats.int} 意志${pstats.wil} 体质${pstats.con}
最近剧情：
${logContext}

请根据成功/失败描述结果（100-200字，大白话不要文艺腔）。成功就写顺利完成了什么，失败就写出了什么意外。直接输出描述。`;

  const res = await fetch(room.aiBaseUrl + '/chat/completions', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Authorization': 'Bearer ' + room.apiKey },
    body: JSON.stringify({ model: room.aiModel, messages: [{ role: 'user', content: prompt }], temperature: 0.9, max_tokens: 500 }),
  });
  if (!res.ok) return;
  const data = await res.json();
  room.log.push({ type: 'dm_reply', text: data.choices[0].message.content.trim(), time: Date.now() });
  room.pendingDice = null;
  broadcastState(room);
}

// 生成3个剧本选项
async function genOptions(room, playerCount) {
  if (!room.apiKey) throw new Error('未设置API Key');
  const prompt = `为一个${playerCount}人跑团生成3个不同题材的剧本选项。
每个选项给一个简短大纲（50字内）。输出JSON格式：
[{"title":"剧本名","theme":"题材","outline":"简短大纲50字"}]
3个选项题材要完全不同（比如：悬疑/恐怖/奇幻）。`;

  const res = await fetch(room.aiBaseUrl + '/chat/completions', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Authorization': 'Bearer ' + room.apiKey },
    body: JSON.stringify({ model: room.aiModel, messages: [{ role: 'user', content: prompt }], temperature: 1.0, max_tokens: 500 }),
  });
  if (!res.ok) throw new Error('API错误');
  const data = await res.json();
  let text = data.choices[0].message.content.trim();
  const match = text.match(/\[[\s\S]*\]/);
  if (match) text = match[0];
  room.options = JSON.parse(text);
}

// 根据玩家大纲生成开场白和背景
async function generateFromOutline(room, outlineText, playerCount) {
  if (!room.apiKey) throw new Error('未设置API Key');
  const prompt = `你是TRPG跑团DM。玩家们选了这个大纲：${outlineText}

请基于这个大纲，为${playerCount}人跑团生成开场白和故事背景。

重要要求：
- 不要写"你们来到了..."这种介绍式开场
- 直接从故事中间开始，一上来就有事情发生
- 大白话，不要文艺腔，不要抽象概念
- 开场白要制造紧张感/好奇心，让玩家想立刻行动

输出JSON：
{
  "title": "剧本名称",
  "background": "故事背景（150字，写清楚发生了什么事）",
  "opening": "开场白（100字，直接从事件中间开始，不要介绍场景）",
  "characters": [{
    "name": "角色名",
    "role": "具体身份",
    "description": "你是谁、为什么在这里（80字）",
    "goal": "你想干什么"
  }],
  "npcs": [{"name":"NPC名","description":"描述","relation":"关系"}],
  "clues": ["线索1","线索2","线索3"],
  "winCondition": "怎么算赢"
}
characters数组正好${playerCount}个。`;

  const res = await fetch(room.aiBaseUrl + '/chat/completions', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Authorization': 'Bearer ' + room.apiKey },
    body: JSON.stringify({ model: room.aiModel, messages: [{ role: 'user', content: prompt }], temperature: 0.9, max_tokens: 3000 }),
  });
  if (!res.ok) throw new Error('API错误');
  const data = await res.json();
  let text = data.choices[0].message.content.trim();
  const match = text.match(/\{[\s\S]*\}/);
  if (match) text = match[0];
  room.scenario = JSON.parse(text);
  // 把开场白加到剧情流最前面
  room.log.unshift({ type: 'dm', text: room.scenario.opening || room.scenario.setting || '', time: Date.now() });
}

server.listen(PORT, () => {
  console.log(`骰语 DiceTale v1.0.0 已启动`);
  console.log(`本地访问: http://localhost:${PORT}`);
  console.log(`好友访问: http://<你的IP>:${PORT}`);
});
