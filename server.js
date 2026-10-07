const http = require('http');
const { WebSocketServer } = require('ws');
const fs = require('fs');
const path = require('path');

const PORT = process.env.PORT || 10000;
const publicDir = path.join(__dirname, 'public');
const MIME = { '.html':'text/html','.js':'text/javascript','.css':'text/css','.json':'application/json' };

const server = http.createServer((req, res) => {
  let p = req.url.split('?')[0];
  if (p === '/') p = '/index.html';
  const fp = path.join(publicDir, p);
  if (!fp.startsWith(publicDir)) { res.writeHead(403); res.end(); return; }
  fs.readFile(fp, (err, data) => {
    if (err) { res.writeHead(404); res.end('Not found'); return; }
    res.writeHead(200, { 'Content-Type': MIME[path.extname(fp).toLowerCase()] || 'text/plain' });
    res.end(data);
  });
});

const wss = new WebSocketServer({ server, path: '/ws' });
const rooms = new Map();
let nextId = 1;

function genRoomId() {
  const c = 'ABCDEFGHJKLMNPQRSTUVWXYZ';
  let id = '';
  for (let i = 0; i < 4; i++) id += c[Math.floor(Math.random() * c.length)];
  return rooms.has(id) ? genRoomId() : id;
}
function broadcast(room, msg) {
  room.players.forEach(p => { if (p.ws && p.ws.readyState === 1) p.ws.send(JSON.stringify(msg)); });
}
function broadcastState(room) {
  broadcast(room, {
    type: 'state',
    data: {
      id: room.id, state: room.state,
      players: room.players.map(p => ({ id: p.id, name: p.name, isDM: p.isDM, customRole: p.customRole||'', ready: !!p.ready, stats: p.stats||null, sideQuest: p.sideQuest||null })),
      scenario: room.scenario, log: room.log, maxPlayers: room.maxPlayers,
      turnOrder: room.turnOrder||[], currentTurn: room.currentTurn||0,
      statPoints: room.statPoints, outlines: room.outlines||null, votes: room.votes||{},
      publicNotes: room.publicNotes||'', pendingDice: room.pendingDice||null
    }
  });
}

wss.on('connection', (ws) => {
  ws.on('message', (raw) => {
    let msg; try { msg = JSON.parse(raw); } catch { return; }
    let room = null, me = null;
    for (const r of rooms.values()) {
      const p = r.players.find(x => x.ws === ws);
      if (p) { room = r; me = p; break; }
    }

    switch (msg.type) {
      case 'create': {
        const id = genRoomId();
        rooms.set(id, {
          id, state: 'lobby', players: [], log: [], maxPlayers: msg.maxPlayers||4,
          apiKey: msg.apiKey||'', aiBaseUrl: 'https://api.deepseek.com', aiModel: 'deepseek-chat',
          outlines: null, votes: {}, statPoints: 10, turnOrder: [], currentTurn: 0,
          publicNotes: '', pendingDice: null
        });
        const dm = { id: nextId++, name: msg.name||'DM', isDM: true, ws, ready: true, stats: null };
        rooms.get(id).players.push(dm);
        ws.send(JSON.stringify({ type: 'created', roomId: id, myId: dm.id }));
        broadcastState(rooms.get(id));
        break;
      }
      case 'join': {
        room = rooms.get(msg.roomId);
        if (!room) { ws.send(JSON.stringify({ type: 'error', msg: '房间不存在' })); return; }
        if (room.players.length >= room.maxPlayers) { ws.send(JSON.stringify({ type: 'error', msg: '房间满了' })); return; }
        const p = { id: nextId++, name: msg.name||'玩家', isDM: false, ws, customRole: msg.role||'', ready: false, stats: null, sideQuest: null };
        room.players.push(p);
        room.log.push({ type: 'system', text: '👋 ' + p.name + ' 加入了', time: Date.now() });
        ws.send(JSON.stringify({ type: 'joined', roomId: room.id, myId: p.id }));
        broadcastState(room);
        break;
      }
      case 'player_ready': { if (me && !me.isDM) { me.ready = !me.ready; broadcastState(room); } break; }
      case 'set_stats': { if (me) { me.stats = msg.stats; broadcastState(room); } break; }
      case 'set_stat_points': { if (me && me.isDM) { room.statPoints = msg.points||10; broadcastState(room); } break; }
      case 'start_outlines': {
        if (me && me.isDM) { room.state = 'outline'; room.outlines = {}; room.log.push({type:'system',text:'✏️ 大家写大纲',time:Date.now()}); broadcastState(room); }
        break;
      }
      case 'submit_outline': {
        if (me && !me.isDM) {
          room.outlines[me.id] = { name: me.name, text: msg.text };
          room.log.push({type:'system',text:'📝 '+me.name+' 提交了大纲',time:Date.now()});
          const players = room.players.filter(p=>!p.isDM);
          if (Object.keys(room.outlines).length >= players.length) { room.state = 'outline_voting'; room.votes = {}; }
          broadcastState(room);
        }
        break;
      }
      case 'vote_outline': { if (me) { room.votes[me.id] = msg.outlineId; broadcastState(room); } break; }
      case 'confirm_outline': {
        if (me && me.isDM) {
          // 统计票数
          const counts = {};
          Object.values(room.votes).forEach(i => counts[i] = (counts[i]||0)+1);
          let bestId = null, max = 0;
          Object.entries(counts).forEach(([i, c]) => { if(c > max) { max = c; bestId = i; } });
          const selected = room.outlines[bestId];
          room.state = 'generating';
          room.log.push({type:'system',text:'🎲 选中了 '+selected.name+' 的大纲，生成开场白...',time:Date.now()});
          broadcastState(room);
          const pCount = room.players.filter(p=>!p.isDM).length;
          generateFromOutline(room, selected.text, pCount).then(() => {
            const pids = room.players.filter(p=>!p.isDM).map(p=>p.id);
            for (let i = pids.length-1; i > 0; i--) { const j = Math.floor(Math.random()*(i+1)); [pids[i],pids[j]] = [pids[j],pids[i]]; }
            room.turnOrder = pids; room.currentTurn = 0; room.state = 'playing';
            room.log.push({type:'system',text:'🔄 回合顺序：'+pids.map(id=>room.players.find(p=>p.id===id)?.name).join(' → '),time:Date.now()});
            broadcastState(room);
          }).catch(err => {
            room.state = 'outline_voting';
            room.log.push({type:'system',text:'生成失败: '+err.message,time:Date.now()});
            broadcastState(room);
          });
        }
        break;
      }
      case 'next_turn': {
        if (me && me.isDM) {
          room.currentTurn = (room.currentTurn + 1) % room.turnOrder.length;
          const nid = room.turnOrder[room.currentTurn];
          const nn = room.players.find(p=>p.id===nid)?.name || '';
          room.log.push({type:'system',text:'🔄 轮到 '+nn+' 了',time:Date.now()});
          broadcastState(room);
        }
        break;
      }
      case 'dm_scene': { if (me && me.isDM) { room.log.push({type:'dm',text:msg.text,time:Date.now()}); broadcastState(room); } break; }
      case 'player_action': {
        if (me && !me.isDM) {
          room.log.push({type:'action',player:me.name,text:msg.text,time:Date.now()});
          broadcastState(room);
          if (room.apiKey) judgeAction(room, me, msg.text).catch(()=>{});
        }
        break;
      }
      case 'dice': {
        if (me) {
          const sides = msg.sides || 20;
          const roll = Math.floor(Math.random()*sides)+1;
          room.log.push({type:'dice',player:me.name,sides,roll,time:Date.now()});
          broadcastState(room);
          if (room.apiKey && !me.isDM) afterDice(room, me, sides, roll).catch(()=>{});
        }
        break;
      }
      case 'set_difficulty': {
        if (me && me.isDM && room.pendingDice) { room.pendingDice.difficulty = msg.difficulty; broadcastState(room); }
        break;
      }
      case 'regenerate_result': {
        if (me && me.isDM) {
          // 重新生成最后一条掷骰结果的描述
          const lastDice = [...room.log].reverse().find(l => l.type === 'dice');
          if (!lastDice) break;
          const player = room.players.find(p => p.name === lastDice.player);
          if (!player) break;
          const ps = player.stats || {str:5,agi:5,int:5,wil:5,con:5};
          const recent = room.log.slice(-15).map(l => {
            if (l.type==='dm'||l.type==='dm_reply') return '【DM】'+l.text;
            if (l.type==='action') return '【'+l.player+'】'+l.text;
            if (l.type==='dice') return '【掷骰】'+l.player+'='+l.roll;
            return '';
          }).filter(Boolean).join('\n');
          const prompt = `你是跑团DM。玩家【${player.name}】掷了d${lastDice.sides}=${lastDice.roll}。
属性：力量${ps.str} 敏捷${ps.agi} 智力${ps.int} 理智${ps.wil} 体质${ps.con}
最近剧情（注意之前谁拿了什么道具）：${recent}
大白话重写结果（80字内），告诉玩家发生了什么。`;
          fetch(room.aiBaseUrl+'/chat/completions',{method:'POST',headers:{'Content-Type':'application/json','Authorization':'Bearer '+room.apiKey},body:JSON.stringify({model:room.aiModel,messages:[{role:'user',content:prompt}],temperature:0.9,max_tokens:300})})
          .then(r=>r.json()).then(d=>{
            const result=d.choices[0].message.content.trim();
            const dm=room.players.find(p=>p.isDM);
            if(dm&&dm.ws)dm.ws.send(JSON.stringify({type:'dm_review',text:result,player:player.name}));
          }).catch(()=>{});
        }
        break;
      }
      case 'update_notes': { room.publicNotes = msg.text||''; broadcastState(room); break; }
    }
  });
  ws.on('close', () => {
    for (const room of rooms.values()) {
      const p = room.players.find(x=>x.ws===ws);
      if (p) { p.ws = null; broadcastState(room); break; }
    }
  });
});

async function judgeAction(room, player, text) {
  const recent = room.log.slice(-10).map(l => {
    if (l.type==='dm'||l.type==='dm_reply') return '【DM】'+l.text;
    if (l.type==='action') return '【'+l.player+'】'+l.text;
    return '';
  }).filter(Boolean).join('\n');
  const ps = player.stats || {str:5,agi:5,int:5,wil:5,con:5};
  const prompt = `你是跑团DM。玩家【${player.name}】说：${text}
属性：力量${ps.str} 敏捷${ps.agi} 智力${ps.int} 理智${ps.wil} 体质${ps.con}
最近剧情：${recent}
判断需要掷骰吗？难度多少（d20大于多少成功）？
只输出JSON：{"needDice":true/false,"difficulty":数字,"reason":"一句话"}`;
  try {
    const res = await fetch(room.aiBaseUrl+'/chat/completions',{method:'POST',headers:{'Content-Type':'application/json','Authorization':'Bearer '+room.apiKey},body:JSON.stringify({model:room.aiModel,messages:[{role:'user',content:prompt}],temperature:0.3,max_tokens:200})});
    const data = await res.json();
    let t = data.choices[0].message.content.trim().replace(/[\x00-\x1F\x7F]/g,'');
    const m = t.match(/\{[\s\S]*\}/);
    if (m) {
      const j = JSON.parse(m[0]);
      if (j.needDice) {
        room.pendingDice = { difficulty: j.difficulty, playerId: player.id };
        room.log.push({type:'system',text:'🎲 需要检定！难度：d20大于'+j.difficulty+'（'+j.reason+'）',time:Date.now()});
      } else {
        room.log.push({type:'system',text:'✅ 不需要掷骰：'+j.reason,time:Date.now()});
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
    if (l.type==='dm'||l.type==='dm_reply') return '【DM】'+l.text;
    if (l.type==='action') return '【'+l.player+'】'+l.text;
    if (l.type==='dice') return '【掷骰】'+l.player+'='+l.roll;
    return '';
  }).filter(Boolean).join('\n');
  const ps = player.stats || {str:5,agi:5,int:5,wil:5,con:5,hp:10};
  const prompt = `你是跑团DM。玩家【${player.name}】掷了d${sides}=${roll}，难度${pending.difficulty}，结果${success?'成功':'失败'}。
当前属性：力量${ps.str} 敏捷${ps.agi} 智力${ps.int} 理智${ps.wil} 体质${ps.con} 体力${ps.hp}
最近剧情：${recent}
大白话写结果（80字内），告诉玩家发生了什么。
同时根据这次行动随机调整属性（平衡，不要一直加）：
返回JSON格式：{"text":"结果描述","str":0,"agi":0,"int":0,"wil":0,"con":0,"hp":0}
数值范围-2到+2，根据行动内容合理调整。`;
  try {
    const res = await fetch(room.aiBaseUrl+'/chat/completions',{method:'POST',headers:{'Content-Type':'application/json','Authorization':'Bearer '+room.apiKey},body:JSON.stringify({model:room.aiModel,messages:[{role:'user',content:prompt}],temperature:0.8,max_tokens:400})});
    const data = await res.json();
    const raw = data.choices[0].message.content.trim();
    const m = raw.match(/\{[\s\S]*\}/);
    if (m) {
      try {
        const j = JSON.parse(m[0]);
        ['str','agi','int','wil','con','hp'].forEach(k => {
          if (j[k] && player.stats) player.stats[k] = Math.max(1, Math.min(15, player.stats[k] + j[k]));
        });
        const labels={str:'力量',agi:'敏捷',int:'智力',wil:'理智',con:'体质',hp:'体力'};
        const changes = Object.keys(j).filter(k=>k!=='text'&&j[k]).map(k=>labels[k]+(j[k]>0?'+':'')+j[k]);
        const result = j.text + (changes.length?'\n📊 属性变化：'+changes.join('，'):'');
        const dm = room.players.find(p=>p.isDM);
        if (dm && dm.ws) dm.ws.send(JSON.stringify({type:'dm_review',text:result,player:player.name}));
        room.pendingDice = null;
        broadcastState(room);
        return;
      } catch {}
    }
    const dm = room.players.find(p=>p.isDM);
    if (dm && dm.ws) dm.ws.send(JSON.stringify({type:'dm_review',text:raw,player:player.name}));
  } catch {}
  room.pendingDice = null;
  broadcastState(room);
}

async function generateFromOutline(room, outlineText, playerCount) {
  if (!room.apiKey) throw new Error('未设置API Key');
  const prompt = `你是跑团DM。大纲：${outlineText}
为${playerCount}人跑团生成开场白。要求：直接从事件中间开始，大白话，不要介绍式开场。
输出JSON：
{"title":"剧本名","background":"背景150字","mainGoal":"共同目标一句话","opening":"开场白100字","characters":[{"name":"角色名","role":"身份","description":"你是谁","goal":"目标","sideQuest":"支线任务"}],"npcs":[{"name":"NPC","description":"描述","relation":"关系"}],"clues":["线索1","线索2","线索3"]}
characters正好${playerCount}个。`;
  const res = await fetch(room.aiBaseUrl+'/chat/completions',{method:'POST',headers:{'Content-Type':'application/json','Authorization':'Bearer '+room.apiKey},body:JSON.stringify({model:room.aiModel,messages:[{role:'user',content:prompt}],temperature:0.9,max_tokens:3000})});
  if (!res.ok) throw new Error('API错误');
  const data = await res.json();
  let text = data.choices[0].message.content.trim().replace(/[\x00-\x1F\x7F]/g,'');
  const m = text.match(/\{[\s\S]*\}/);
  if (m) text = m[0];
  room.scenario = JSON.parse(text);
  room.players.forEach(p => {
    if (p.isDM) return;
    const idx = room.players.filter(x=>!x.isDM).indexOf(p);
    p.sideQuest = room.scenario.characters?.[idx]?.sideQuest || null;
  });
  room.log.unshift({type:'dm',text:room.scenario.opening||'',time:Date.now()});
}

server.listen(PORT, () => console.log('骰语 v1.5.0 启动'));
