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
    state: 'lobby', // lobby | playing | ended
    players: [],    // {ws, id, name, isDM, character}
    scenario: null, // AI生成的剧本
    log: [],        // 剧情流
    turn: 0,
    maxPlayers: 4,
    apiKey: '',
    aiBaseUrl: 'https://api.deepseek.com',
    aiModel: 'deepseek-chat',
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
      // 玩家行动
      case 'player_action': {
        if (!me || me.isDM) return;
        room.log.push({ type: 'action', player: me.name, text: msg.text, time: Date.now() });
        broadcastState(room);
        break;
      }
      // DM回复/裁定
      case 'dm_reply': {
        if (!me || !me.isDM) return;
        room.log.push({ type: 'dm_reply', text: msg.text, time: Date.now() });
        broadcastState(room);
        break;
      }
      // 掷骰
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
        break;
      }
      // 开始游戏
      case 'start': {
        if (!me || !me.isDM) return;
        room.state = 'playing';
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
    if (room && me) {
      room.players = room.players.filter(p => p !== me);
      if (room.players.length === 0) {
        rooms.delete(room.id);
      } else {
        broadcastState(room);
      }
    }
  });
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

server.listen(PORT, () => {
  console.log(`骰语 DiceTale v1.0.0 已启动`);
  console.log(`本地访问: http://localhost:${PORT}`);
  console.log(`好友访问: http://<你的IP>:${PORT}`);
});
