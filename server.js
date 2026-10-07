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
    options: null,        // 剧本选项 [{title, theme, outline}]
    votes: {},            // {playerId: optionIndex}
    approveVotes: {},     // {playerId: true/false}
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
      id: p.id, name: p.name, isDM: p.isDM, character: p.character
    })),
    scenario: room.scenario,
    log: room.log,
    maxPlayers: room.maxPlayers,
    turn: room.turn,
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
        me = { ws, id: nextPid++, name: msg.name || '玩家', isDM: false, character: null };
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
      // DM生成剧本选项
      case 'generate_options': {
        if (!me || !me.isDM) return;
        const pCount = room.players.filter(p => !p.isDM).length;
        room.log.push({ type: 'system', text: '🎲 AI正在生成3个剧本选项...', time: Date.now() });
        broadcastState(room);
        genOptions(room, pCount).then(() => {
          room.state = 'voting';
          room.votes = {};
          broadcastState(room);
        }).catch(err => {
          room.log.push({ type: 'system', text: '生成失败: ' + err.message, time: Date.now() });
          broadcastState(room);
        });
        break;
      }
      // 玩家投票选剧本
      case 'vote_scenario': {
        if (!me || me.isDM) return;
        room.votes[me.id] = msg.optionIndex;
        // 如果所有玩家都投了，统计结果
        const players = room.players.filter(p => !p.isDM);
        const voted = Object.keys(room.votes).length;
        if (voted >= players.length) {
          // 统计票数
          const counts = {};
          Object.values(room.votes).forEach(i => counts[i] = (counts[i]||0)+1);
          let best = 0, max = 0;
          Object.entries(counts).forEach(([i, c]) => { if(c > max) { max = c; best = parseInt(i); } });
          // 生成完整剧本
          room.log.push({ type: 'system', text: `📖 投票结果：选中了《${room.options[best].title}》，正在生成完整剧本...`, time: Date.now() });
          broadcastState(room);
          generateScenarioFromOption(room, room.options[best], players.length).then(() => {
            room.state = 'preview';
            room.approveVotes = {};
            broadcastState(room);
          }).catch(err => {
            room.log.push({ type: 'system', text: '剧本生成失败: ' + err.message, time: Date.now() });
            broadcastState(room);
          });
        } else {
          broadcastState(room);
        }
        break;
      }
      // 玩家投票接受/重roll
      case 'approve_scenario': {
        if (!me || me.isDM) return;
        room.approveVotes[me.id] = msg.approve; // true=接受, false=重roll
        const players = room.players.filter(p => !p.isDM);
        const voted = Object.keys(room.approveVotes).length;
        if (voted >= players.length) {
          const approved = Object.values(room.approveVotes).filter(v => v).length;
          if (approved > players.length / 2) {
            room.state = 'playing';
            room.log.push({ type: 'system', text: '✅ 大家接受了剧本，游戏开始！', time: Date.now() });
          } else {
            room.state = 'lobby';
            room.options = null;
            room.scenario = null;
            room.votes = {};
            room.approveVotes = {};
            room.log.push({ type: 'system', text: '🔄 大家想重roll，DM重新生成选项', time: Date.now() });
          }
        }
        broadcastState(room);
        break;
      }
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

// 玩家行动后AI自动判断：要不要掷骰？直接写结果？
async function aiProcessAction(room, player, actionText) {
  if (!room.apiKey) return;
  const logContext = room.log.slice(-8).map(l => {
    if (l.type === 'dm') return `【主持人】${l.text}`;
    if (l.type === 'action') return `【${l.player}】${l.text}`;
    if (l.type === 'dm_reply') return `【主持人】${l.text}`;
    if (l.type === 'dice') return `【${l.player}】掷出 d${l.sides}=${l.roll}`;
    return '';
  }).filter(Boolean).join('\n');

  const prompt = `你是TRPG跑团DM。玩家【${player.name}】采取了行动：${actionText}

当前场景：${room.scenario?.setting || '未知'}
最近剧情：
${logContext}

请判断这个行动：
1. 如果是简单/自动成功的事（观察、说话、走路、拿东西），直接输出DM描述（100字内），以"DM:"开头
2. 如果需要运气判定（战斗、撬锁、追踪、说服、躲陷阱），输出"NEED_ROLL"，然后说明需要什么检定（比如"需要d20敏捷检定"），以"ROLL:"开头

只输出一行，格式：
- DM:描述内容（自动成功时）
- ROLL:需要什么检定（需要掷骰时）`;

  const res = await fetch(room.aiBaseUrl + '/chat/completions', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Authorization': 'Bearer ' + room.apiKey },
    body: JSON.stringify({ model: room.aiModel, messages: [{ role: 'user', content: prompt }], temperature: 0.8, max_tokens: 300 }),
  });
  if (!res.ok) throw new Error('AI错误');
  const data = await res.json();
  const text = data.choices[0].message.content.trim();

  if (text.startsWith('ROLL:')) {
    // 需要掷骰：提示玩家掷骰
    room.log.push({ type: 'system', text: `🎲 ${text.replace('ROLL:', '').trim()} — ${player.name} 请掷骰`, time: Date.now() });
  } else {
    // 自动成功：直接写DM回复
    room.log.push({ type: 'dm_reply', text: text.replace(/^DM:/, '').trim(), time: Date.now() });
  }
  broadcastState(room);
}

// 掷骰后AI根据结果续写
async function aiDiceResult(room, player, sides, roll) {
  if (!room.apiKey) return;
  const lastAction = [...room.log].reverse().find(l => l.type === 'action' && l.player === player.name);
  const logContext = room.log.slice(-8).map(l => {
    if (l.type === 'dm') return `【主持人】${l.text}`;
    if (l.type === 'action') return `【${l.player}】${l.text}`;
    if (l.type === 'dm_reply') return `【主持人】${l.text}`;
    if (l.type === 'system') return `【系统】${l.text}`;
    return '';
  }).filter(Boolean).join('\n');

  const prompt = `你是TRPG跑团DM。玩家【${player.name}】掷了d${sides}，结果是 ${roll}。
他之前的行动是：${lastAction ? lastAction.text : '未知'}

最近剧情：
${logContext}

请根据掷骰结果描述行动结果（100-200字）。点数高就成功，点数低就失败或出意外。直接输出描述，不要加前缀。`;

  const res = await fetch(room.aiBaseUrl + '/chat/completions', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Authorization': 'Bearer ' + room.apiKey },
    body: JSON.stringify({ model: room.aiModel, messages: [{ role: 'user', content: prompt }], temperature: 0.9, max_tokens: 500 }),
  });
  if (!res.ok) return;
  const data = await res.json();
  room.log.push({ type: 'dm_reply', text: data.choices[0].message.content.trim(), time: Date.now() });
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

// 根据选中的选项生成完整剧本
async function generateScenarioFromOption(room, option, playerCount) {
  if (!room.apiKey) throw new Error('未设置API Key');
  const prompt = `你是TRPG跑团主持人。请为一个${playerCount}人跑团生成完整剧本。
题材方向：${option.theme}
剧本名参考：${option.title}
大纲参考：${option.outline}

请输出JSON格式（不要有多余文字）：
{
  "title": "剧本名称",
  "background": "故事背景描述（200字左右）",
  "setting": "当前场景描述（100字左右）",
  "characters": [{
    "name": "角色名",
    "role": "具体身份（不要写'冒险者'，要写比如：失忆侦探/退休警察/失踪者的妹妹/神秘术士）",
    "description": "这个角色的背景故事（100字左右，要让玩家读完就知道自己是谁、为什么在这里、有什么秘密）",
    "goal": "具体的个人目标（不要写'活下去'，要写比如：找到失踪的妹妹/洗清杀人嫌疑/揭开自己失忆的真相）"
  }],
  "npcs": [{"name":"NPC名","description":"NPC描述","relation":"与玩家的关系"}],
  "clues": ["线索1","线索2","线索3"],
  "winCondition": "胜利条件",
  "twist": "剧情转折（DM专用）"
}

重要要求：
1. characters数组正好${playerCount}个角色
2. 每个角色的role必须是具体身份，不能是"冒险者/幸存者"这种笼统词
3. description要写一段完整的小故事，玩家读完要知道自己是谁
4. goal必须和剧情相关，不能是"活下去"这种空话`;

  const res = await fetch(room.aiBaseUrl + '/chat/completions', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Authorization': 'Bearer ' + room.apiKey },
    body: JSON.stringify({ model: room.aiModel, messages: [{ role: 'user', content: prompt }], temperature: 0.8, max_tokens: 3000 }),
  });
  if (!res.ok) throw new Error('API错误');
  const data = await res.json();
  let text = data.choices[0].message.content.trim();
  const match = text.match(/\{[\s\S]*\}/);
  if (match) text = match[0];
  room.scenario = JSON.parse(text);
}

server.listen(PORT, () => {
  console.log(`骰语 DiceTale v1.0.0 已启动`);
  console.log(`本地访问: http://localhost:${PORT}`);
  console.log(`好友访问: http://<你的IP>:${PORT}`);
});
