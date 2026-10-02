/*
 * 3D 第一人称射击 · 联机大厅服务器
 * ------------------------------------------------------------
 * 一个文件同时干两件事：
 *   1) 静态 HTTP 服务：把 index.html 发给浏览器（必须走 http://，否则 ES Module / importmap 会被拦）
 *   2) WebSocket 中继：大厅（昵称 / 房间列表 / 创建·加入·离开）+ 房主-加入者的对局消息转发
 *
 * 用法：
 *   node server.js            # 默认 8000 端口
 *   node server.js 9000       # 指定端口
 *   PORT=9000 node server.js  # 环境变量也行
 *
 * 玩法：一台机器跑本文件（房主），双方浏览器都打开 http://<这台机器的IP>:8000
 *      房主建房间拿到 4 位房间号 → 对方在大厅输入房间号加入 → 房主点开始
 */
'use strict';

const http = require('http');
const fs = require('fs');
const os = require('os');
const path = require('path');

const PORT = Number(process.argv[2] || process.env.PORT || 8000);
const HOST = process.env.HOST || '0.0.0.0';
const HEARTBEAT_MS = 15000;   // 心跳：探测断线（手机切后台/拔网线）
const CLIENT_STALE_MS = 45000; // 心跳无响应的宽限时间

/* ============================ 静态文件 ============================ */
function readIndex() {
  try { return fs.readFileSync(path.join(__dirname, 'index.html')); }
  catch (e) { return null; }
}

const server = http.createServer(function (req, res) {
  const url = (req.url || '/').split('?')[0];
  if (req.method !== 'GET' && req.method !== 'HEAD') { res.writeHead(405); return res.end('Method Not Allowed'); }

  if (url === '/' || url === '/index.html' || url === '/game.html') {
    const html = readIndex();
    if (!html) { res.writeHead(500, { 'Content-Type': 'text/plain; charset=utf-8' }); return res.end('找不到 index.html（请把它和 server.js 放在同一目录）'); }
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-store' });
    return res.end(req.method === 'HEAD' ? undefined : html);
  }
  if (url === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
    return res.end(JSON.stringify({ ok: true, rooms: rooms.size, clients: clients.size, uptime: Math.round(process.uptime()) }));
  }
  res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' });
  res.end('404 Not Found —— 本服务器只提供 / 与 /index.html');
});

/* ============================ WebSocket 握手 ============================ */
const GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';

function acceptKey(key) {
  return require('crypto').createHash('sha1').update(key + GUID).digest('base64');
}

server.on('upgrade', function (req, socket) {
  if (process.env.DSH_WS_DEBUG) {
    console.log('[DBG upgrade] url=' + req.url + ' upgrade=' + req.headers.upgrade + ' key=' + req.headers['sec-websocket-key']);
  }
  let key;
  try { key = req.headers['sec-websocket-key']; } catch (e) { key = null; }
  if (!key || (req.headers.upgrade || '').toLowerCase() !== 'websocket') {
    socket.destroy();
    return;
  }
  socket.write(
    'HTTP/1.1 101 Switching Protocols\r\n' +
    'Upgrade: websocket\r\n' +
    'Connection: Upgrade\r\n' +
    'Sec-WebSocket-Accept: ' + acceptKey(key) + '\r\n\r\n'
  );
  socket.setNoDelay(true);
  attachClient(socket);
});

/* ============================ 极简 WebSocket 帧协议 ============================ */
function attachClient(socket) {
  const client = {
    socket: socket,
    id: 0,              // 登录时分配，房间内唯一；对局消息靠它认人
    name: '',
    roomId: null,
    loggedIn: false,
    closed: false,
    lastSeen: Date.now(),
    buf: Buffer.alloc(0),
    fragOp: 0,
    frags: []
  };
  clients.add(client);

  socket.on('data', function (chunk) { onData(client, chunk); });
  socket.on('close', function () { dropClient(client, 'close'); });
  socket.on('error', function () { dropClient(client, 'error'); });
  /*
   * 'end' 必须单独处理：http.Server 升级出来的 socket 是 allowHalfOpen=true，
   * 对端只发 FIN（不发 WebSocket close 帧）时只会触发 'end'，永远不触发 'close'，
   * 于是这条连接会一直挂在 clients 里、房间也一直被占着。
   * 浏览器崩溃 / 强杀进程 / 拔网线 / 电脑睡眠 走的都是这条路径。
   * WebSocket 里半关闭 = 对方再也不会发消息了，直接按掉线处理。
   */
  socket.on('end', function () { dropClient(client, 'end'); });
  socket.setTimeout(0);
}

function onData(client, chunk) {
  client.lastSeen = Date.now();
  client.buf = client.buf.length ? Buffer.concat([client.buf, chunk]) : chunk;

  for (;;) {
    const buf = client.buf;
    if (buf.length < 2) return;

    const fin = (buf[0] & 0x80) !== 0;
    const opcode = buf[0] & 0x0f;
    const masked = (buf[1] & 0x80) !== 0;
    let len = buf[1] & 0x7f;
    let offset = 2;

    if (len === 126) {
      if (buf.length < 4) return;
      len = buf.readUInt16BE(2);
      offset = 4;
    } else if (len === 127) {
      if (buf.length < 10) return;
      const hi = buf.readUInt32BE(2);
      const lo = buf.readUInt32BE(6);
      len = hi * 4294967296 + lo;
      offset = 10;
    }
    if (len > 4 * 1024 * 1024) { dropClient(client, 'too-large'); return; } // 4MB 上限，防止塞爆内存

    const maskLen = masked ? 4 : 0;
    if (buf.length < offset + maskLen + len) return;

    const mask = masked ? buf.slice(offset, offset + 4) : null;
    offset += maskLen;
    let payload = buf.slice(offset, offset + len);
    if (mask) {
      const out = Buffer.allocUnsafe(payload.length);
      for (let i = 0; i < payload.length; i++) out[i] = payload[i] ^ mask[i & 3];
      payload = out;
    }
    client.buf = buf.slice(offset + len);

    /* --- 控制帧 --- */
    if (opcode === 0x8) { dropClient(client, 'bye'); return; }
    if (opcode === 0x9) { sendFrame(client, 0xA, payload); continue; } // ping -> pong
    if (opcode === 0xA) { continue; }                                  // pong

    /* --- 数据帧（支持分片） --- */
    if (opcode === 0x0) {
      client.frags.push(payload);
    } else if (opcode === 0x1 || opcode === 0x2) {
      client.fragOp = opcode;
      client.frags = [payload];
    } else {
      continue;
    }
    if (!fin) continue;

    const full = Buffer.concat(client.frags);
    client.frags = [];
    if (client.fragOp !== 0x1) continue; // 只处理文本帧

    let msg;
    try { msg = JSON.parse(full.toString('utf8')); } catch (e) { continue; }
    if (msg && typeof msg === 'object') handleMessage(client, msg);
  }
}

function sendFrame(client, opcode, payload) {
  if (!client || client.closed || !client.socket || client.socket.destroyed) return;
  const data = Buffer.isBuffer(payload) ? payload : Buffer.from(String(payload), 'utf8');
  const len = data.length;
  let header;
  if (len < 126) {
    header = Buffer.allocUnsafe(2);
    header[0] = 0x80 | opcode;
    header[1] = len;
  } else if (len < 65536) {
    header = Buffer.allocUnsafe(4);
    header[0] = 0x80 | opcode;
    header[1] = 126;
    header.writeUInt16BE(len, 2);
  } else {
    header = Buffer.allocUnsafe(10);
    header[0] = 0x80 | opcode;
    header[1] = 127;
    header.writeUInt32BE(Math.floor(len / 4294967296), 2);
    header.writeUInt32BE(len >>> 0, 6);
  }
  try { client.socket.write(Buffer.concat([header, data])); } catch (e) { dropClient(client, 'write'); }
}

function send(client, obj) { sendFrame(client, 0x1, JSON.stringify(obj)); }

/* ============================ 房间 & 大厅状态 ============================ */
const clients = new Set();
const rooms = new Map(); // id -> { id, members:[client...], maxPlayers, state, createdAt }
let nextClientId = 0;
const MAX_PLAYERS = 4;   // 每间房的人数上限（含房主）

function isHostOf(room, client) { return !!room && room.members[0] === client; }

function makeRoomId() {
  for (let i = 0; i < 500; i++) {
    const id = String(Math.floor(1000 + Math.random() * 9000)); // 4 位数字，和界面「4 位房间号」对齐
    if (!rooms.has(id)) return id;
  }
  return String(Date.now()).slice(-6);
}

function roomPublic(room) {
  const host = room.members[0] || null;
  return {
    id: room.id,
    hostName: host ? host.name : '房主',
    hostId: host ? host.id : 0,
    players: room.members.length,
    maxPlayers: room.maxPlayers,
    state: room.state,
    members: room.members.map(function (m) { return { id: m.id, name: m.name }; })
  };
}

function roomBroadcast(room, obj, exceptClient) {
  for (let i = 0; i < room.members.length; i++) {
    if (room.members[i] === exceptClient) continue;
    send(room.members[i], obj);
  }
}

function broadcastLobby() {
  const list = [];
  rooms.forEach(function (r) {
    if (r.state !== 'closed') list.push(roomPublic(r));
  });
  list.sort(function (a, b) { return Number(a.id) - Number(b.id); });
  clients.forEach(function (c) { if (c.loggedIn) send(c, { type: 'lobbyState', rooms: list }); });
}

function sendRoomState(room, state) {
  room.state = state;
  broadcastLobby();
}

function cleanName(raw) {
  let n = String(raw == null ? '' : raw).replace(/[\\\/<>"'&]/g, '').trim();
  if (!n) n = '玩家' + Math.floor(1000 + Math.random() * 9000);
  const arr = Array.from(n);
  if (arr.length > 12) n = arr.slice(0, 12).join('');
  return n;
}

function leaveRoom(client, reason) {
  if (!client.roomId) return;
  const room = rooms.get(client.roomId);
  client.roomId = null;
  if (!room) return;
  const idx = room.members.indexOf(client);
  if (idx < 0) return;
  room.members.splice(idx, 1);

  if (idx === 0) {
    // 房主走了 => 房间解散（房间里的对局状态只有房主有，没法交接）
    room.state = 'closed';
    rooms.delete(room.id);
    room.members.forEach(function (m) {
      m.roomId = null;
      send(m, { type: 'roomClosed' });
      send(m, { type: 'error', message: reason || '房主已离开，房间已解散' });
    });
    log('房间 ' + room.id + ' 解散（房主 ' + (client.name || '?') + ' 离开）');
  } else {
    if (room.state === 'playing') room.state = 'waiting';
    roomBroadcast(room, { type: 'peerLeft', id: client.id, name: client.name, room: roomPublic(room) });
    log('房间 ' + room.id + ' 玩家离开（' + (client.name || '?') + '，剩 ' + room.members.length + ' 人）');
  }
  broadcastLobby();
}

function dropClient(client, why) {
  if (client.closed) return;
  client.closed = true;
  clients.delete(client);
  try { if (client.socket && !client.socket.destroyed) client.socket.destroy(); } catch (e) {}
  leaveRoom(client, why === 'close' ? '对方已断开连接' : '对方连接异常，房间已解散');
}

/* ============================ 消息路由 ============================ */
function handleMessage(client, msg) {
  switch (msg.type) {
    /* --- 登录：客户端连上后第一件事 --- */
    case 'login': {
      if (!client.id) client.id = ++nextClientId;
      client.name = cleanName(msg.name);
      client.loggedIn = true;
      send(client, { type: 'loginOk', name: client.name, id: client.id });
      broadcastLobby();
      return;
    }

    /* --- 大厅房间列表 --- */
    case 'getLobby': {
      broadcastLobby();
      return;
    }

    /* --- 建房 --- */
    case 'createRoom': {
      if (!client.loggedIn) { send(client, { type: 'error', message: '请先登录（刷新页面重试）' }); return; }
      if (client.roomId) leaveRoom(client, '你已离开上一个房间');
      const room = {
        id: makeRoomId(),
        members: [client],      // members[0] 恒为房主
        maxPlayers: MAX_PLAYERS,
        state: 'waiting',
        createdAt: Date.now()
      };
      rooms.set(room.id, room);
      client.roomId = room.id;
      send(client, { type: 'roomCreated', room: roomPublic(room) });
      broadcastLobby();
      log('房间 ' + room.id + ' 创建（房主 ' + client.name + '）');
      return;
    }

    /* --- 加入房间 --- */
    case 'joinRoom': {
      const id = String(msg.roomId == null ? '' : msg.roomId).trim();
      const room = rooms.get(id);
      if (!client.loggedIn) { send(client, { type: 'error', message: '请先登录' }); return; }
      if (!room) { send(client, { type: 'roomFull', reason: '房间 ' + id + ' 不存在或已解散' }); broadcastLobby(); return; }
      if (room.members.indexOf(client) >= 0) { client.roomId = room.id; send(client, { type: 'roomJoined', room: roomPublic(room) }); return; }
      if (room.members.length >= room.maxPlayers) { send(client, { type: 'roomFull', reason: '房间已满（每间最多 ' + room.maxPlayers + ' 人）' }); broadcastLobby(); return; }
      if (room.state !== 'waiting') { send(client, { type: 'roomFull', reason: '该房间已开局' }); broadcastLobby(); return; }

      if (client.roomId) leaveRoom(client, '你已离开上一个房间');
      room.members.push(client);
      client.roomId = room.id;
      send(client, { type: 'roomJoined', room: roomPublic(room) });
      roomBroadcast(room, { type: 'peerJoined', id: client.id, name: client.name, room: roomPublic(room) }, client);
      broadcastLobby();
      log('房间 ' + room.id + ' 玩家进入（' + client.name + '，共 ' + room.members.length + ' 人）');
      return;
    }

    /* --- 离开房间 --- */
    case 'leaveRoom': {
      leaveRoom(client, null);
      return;
    }

    /* --- 房主切换房间状态 waiting / playing --- */
    case 'roomState': {
      const room = rooms.get(client.roomId);
      if (!isHostOf(room, client)) return;
      sendRoomState(room, msg.state === 'playing' ? 'playing' : 'waiting');
      return;
    }

    /* --- 对局消息转发（房主权威）---
     * 房主发的 -> 广播给房间里其他所有人（快照、伤害、复活、开局、结算）
     * 其他人发的 -> 只发给房主（位置上报、开火、请求重开）
     * 带上 from / id，房主才知道这条消息是谁的。 */
    case 'roomMsg': {
      const room = rooms.get(client.roomId);
      if (!room || room.members.indexOf(client) < 0) return;
      const host = room.members[0];
      const fromHost = (client === host);
      for (let i = 0; i < room.members.length; i++) {
        const m = room.members[i];
        if (m === client) continue;
        if (fromHost || m === host) send(m, { type: 'roomMsg', from: client.id, data: msg.data });
      }
      return;
    }

    default:
      return;
  }
}

/* ============================ 心跳：清理掉线连接 ============================ */
const heartbeat = setInterval(function () {
  const now = Date.now();
  clients.forEach(function (c) {
    if (now - c.lastSeen > CLIENT_STALE_MS) { dropClient(c, 'timeout'); return; }
    sendFrame(c, 0x9, Buffer.alloc(0)); // ping
  });
}, HEARTBEAT_MS);
heartbeat.unref && heartbeat.unref();

/* ============================ 启动 ============================ */
function localAddresses() {
  const out = [];
  const ifaces = os.networkInterfaces();
  Object.keys(ifaces).forEach(function (name) {
    (ifaces[name] || []).forEach(function (info) {
      if (info.family === 'IPv4' && !info.internal) out.push({ name: name, address: info.address });
    });
  });
  return out;
}

function log(s) {
  const t = new Date().toTimeString().slice(0, 8);
  console.log('[' + t + '] ' + s);
}

server.on('error', function (err) {
  if (err && err.code === 'EADDRINUSE') {
    console.error('\n端口 ' + PORT + ' 已被占用。换一个端口再试：node server.js 8001\n');
  } else {
    console.error('服务器错误：', err && err.message);
  }
  process.exit(1);
});

server.listen(PORT, HOST, function () {
  console.log('');
  console.log('  3D 第一人称射击 · 联机大厅服务器已启动');
  console.log('  ------------------------------------------------');
  console.log('  本机（房主自己）：http://127.0.0.1:' + PORT);
  const addrs = localAddresses();
  if (addrs.length) {
    console.log('  局域网（把下面地址发给朋友，同一个 WiFi 就能直接进）：');
    addrs.forEach(function (a) { console.log('      http://' + a.address + ':' + PORT + '   [' + a.name + ']'); });
  } else {
    console.log('  没检测到局域网 IP：先用 http://127.0.0.1:' + PORT + ' 自己测，朋友联机需要同一个网络');
  }
  console.log('  WebSocket 中继：ws://<上面的地址>:' + PORT + '（页面会自动连，不用手填）');
  console.log('  ------------------------------------------------');
  console.log('  玩法：所有人打开上面地址 → 模式选「联机混战」→ 一人创建房间拿到 4 位房间号');
  console.log('        → 其他人在大厅点【加入】或输入房间号（每间最多 ' + MAX_PLAYERS + ' 人）→ 房主点【开始游戏】');
  console.log('  按 Ctrl+C 关闭服务器（关掉后大家都联不上了）');
  console.log('');
});
