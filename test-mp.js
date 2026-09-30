/*
 * server.js 自动联机自测（不依赖任何第三方库）
 *   node test-mp.js
 * 流程：拉起 server.js -> 两个客户端登录 -> 建房/加入 -> 双向中继 -> 离开/解散 -> 退出
 */
'use strict';

const { spawn } = require('child_process');
const crypto = require('crypto');
const net = require('net');
const path = require('path');

const PORT = Number(process.env.TEST_PORT || 8123);
const HOST = '127.0.0.1';

function wsConnect(port) {
  return new Promise(function (resolve, reject) {
    const key = crypto.randomBytes(16).toString('base64');
    const sock = net.connect(port, HOST, function () {
      sock.write(
        'GET / HTTP/1.1\r\n' +
        'Host: ' + HOST + ':' + port + '\r\n' +
        'Upgrade: websocket\r\nConnection: Upgrade\r\n' +
        'Sec-WebSocket-Key: ' + key + '\r\nSec-WebSocket-Version: 13\r\n\r\n'
      );
    });
    sock.once('error', function (e) { reject(e); });
    let buf = Buffer.alloc(0);
    let handshaken = false;
    let settled = false;
    const inbox = [];
    const waiters = [];
    let api = null;   // 在 data 回调里会先用到，所以提前声明

    function deliver(msg) {
      if (waiters.length) waiters.shift()(msg);
      else inbox.push(msg);
    }

    sock.on('data', function (chunk) {
      if (process.env.DSH_TEST_DEBUG) console.log('    [ws in] ' + JSON.stringify(chunk.toString('utf8').slice(0, 100)));
      buf = Buffer.concat([buf, chunk]);
      if (!handshaken) {
        const i = buf.indexOf('\r\n\r\n');
        if (i < 0) return;
        const head = buf.slice(0, i).toString();
        if (!/ 101 /.test(head)) {
          if (!settled) { settled = true; reject(new Error('握手失败：' + head.split('\r\n')[0])); }
          return;
        }
        handshaken = true;
        buf = buf.slice(i + 4);
        if (!settled) { settled = true; resolve(api); }   // 只有真收到 101 才算连上
      }
      for (;;) {
        if (buf.length < 2) return;
        const opcode = buf[0] & 0x0f;
        let len = buf[1] & 0x7f;
        let off = 2;
        if (len === 126) { if (buf.length < 4) return; len = buf.readUInt16BE(2); off = 4; }
        else if (len === 127) { if (buf.length < 10) return; len = buf.readUInt32BE(6); off = 10; }
        if (buf.length < off + len) return;
        const payload = buf.slice(off, off + len);
        buf = buf.slice(off + len);
        if (opcode === 0x1) {
          try { deliver(JSON.parse(payload.toString('utf8'))); } catch (e) {}
        }
      }
    });

    function send(obj) {
      const data = Buffer.from(JSON.stringify(obj), 'utf8');
      const mask = crypto.randomBytes(4);
      let header;
      if (data.length < 126) { header = Buffer.allocUnsafe(2); header[1] = 0x80 | data.length; }
      else { header = Buffer.allocUnsafe(4); header[1] = 0x80 | 126; header.writeUInt16BE(data.length, 2); }
      header[0] = 0x81;
      const masked = Buffer.allocUnsafe(data.length);
      for (let i = 0; i < data.length; i++) masked[i] = data[i] ^ mask[i & 3];
      sock.write(Buffer.concat([header, mask, masked]));
    }

    function next(timeoutMs) {
      if (inbox.length) return Promise.resolve(inbox.shift());
      return new Promise(function (res, rej) {
        const t = setTimeout(function () { rej(new Error('等待服务器消息超时')); }, timeoutMs || 3000);
        waiters.push(function (m) { clearTimeout(t); res(m); });
      });
    }

    sock.once('close', function () {
      if (!settled) { settled = true; reject(new Error('连接在握手完成前被关闭')); }
    });

    api = {
      send: send,
      next: next,
      close: function () { try { sock.destroy(); } catch (e) {} },
      /** 一直读到匹配的消息（忽略中途的 lobbyState 广播） */
      until: async function (pred, timeoutMs) {
        const deadline = Date.now() + (timeoutMs || 4000);
        for (;;) {
          const left = deadline - Date.now();
          if (left <= 0) throw new Error('未等到期望的消息');
          const m = await next(left);
          if (pred(m)) return m;
        }
      }
    };
  });
}

function sleep(ms) { return new Promise(function (r) { setTimeout(r, ms); }); }

(async function main() {
  const server = spawn(process.execPath, [path.join(__dirname, 'server.js'), String(PORT)], {
    stdio: ['ignore', 'pipe', 'pipe']
  });
  server.stdout.on('data', function (d) { process.stdout.write('[server] ' + d); });
  server.stderr.on('data', function (d) { process.stdout.write('[server!] ' + d); });

  let failed = 0;
  function check(label, ok, extra) {
    console.log((ok ? '  ✓ ' : '  ✗ ') + label + (extra ? '  → ' + extra : ''));
    if (!ok) failed++;
  }

  try {
    // 等服务器监听
    for (let i = 0; i < 40; i++) {
      const up = await new Promise(function (res) {
        const s = net.connect(PORT, HOST);
        s.once('connect', function () { s.destroy(); res(true); });
        s.once('error', function () { res(false); });
      });
      if (up) break;
      await sleep(150);
    }

    // 1) HTTP 页面（agent:false = 用完就关连接，别让后面的 WebSocket 复用这条 keep-alive 连接）
    const page = await new Promise(function (res) {
      const http = require('http');
      const req = http.get({ host: HOST, port: PORT, path: '/', agent: false }, function (r) {
        let body = '';
        r.on('data', function (c) { body += c; });
        r.on('end', function () { res({ code: r.statusCode, body: body }); });
      });
      req.on('error', function (e) { res({ code: 0, body: String(e.message) }); });
    });
    check('HTTP 首页返回游戏页面', page.code === 200 && page.body.indexOf('联机大厅') >= 0, 'HTTP ' + page.code + ' / ' + page.body.length + ' 字节');

    // 2) 两个客户端握手 + 登录
    const host = await wsConnect(PORT);
    console.log('  · 1 号客户端已连接');
    const guest = await wsConnect(PORT);
    console.log('  · 2 号客户端已连接');
    check('两个浏览器客户端完成 WebSocket 握手', !!host && !!guest);

    host.send({ type: 'login', name: '房主Alpha' });
    guest.send({ type: 'login', name: '加入者Beta' });
    const hLogin = await host.until(function (m) { return m.type === 'loginOk'; });
    const gLogin = await guest.until(function (m) { return m.type === 'loginOk'; });
    check('登录并下发昵称', hLogin.name === '房主Alpha' && gLogin.name === '加入者Beta', hLogin.name + ' / ' + gLogin.name);

    // 3) 建房
    host.send({ type: 'createRoom' });
    const created = await host.until(function (m) { return m.type === 'roomCreated'; });
    const roomId = created.room.id;
    check('创建房间并拿到 4 位房间号', /^\d{4,6}$/.test(roomId) && created.room.players === 1, '房间号 ' + roomId);

    // 4) 大厅列表能看到这个房间
    const lobby = await guest.until(function (m) { return m.type === 'lobbyState' && m.rooms && m.rooms.some(function (r) { return r.id === roomId; }); });
    const row = lobby.rooms.filter(function (r) { return r.id === roomId; })[0];
    check('大厅房间列表可见（房主名 / 人数 / 状态）', row.hostName === '房主Alpha' && row.players === 1 && row.state === 'waiting', row.hostName + ' ' + row.players + '/' + row.maxPlayers + ' ' + row.state);

    // 5) 加入房间 + 房主收到 peerJoined
    guest.send({ type: 'joinRoom', roomId: roomId });
    const joined = await guest.until(function (m) { return m.type === 'roomJoined'; });
    const peerJoined = await host.until(function (m) { return m.type === 'peerJoined'; });
    check('加入者进入房间 + 房主收到 peerJoined', joined.room.id === roomId && peerJoined.playerName === '加入者Beta');

    // 6) roomState = playing（房主点「开始游戏」时发的）
    host.send({ type: 'roomState', state: 'playing' });
    const playing = await guest.until(function (m) { return m.type === 'lobbyState' && m.rooms.some(function (r) { return r.id === roomId && r.state === 'playing'; }); });
    check('开局后大厅状态变为「对战中」', !!playing);

    // 7) 对局消息双向中继
    guest.send({ type: 'roomMsg', data: { type: 'me', x: 1.5, z: 2.5, yaw: 0.3, weapon: 'smg' } });
    const meMsg = await host.until(function (m) { return m.type === 'roomMsg'; });
    check('加入者 → 房主：位置/朝向/武器转发', meMsg.data && meMsg.data.type === 'me' && meMsg.data.weapon === 'smg');

    host.send({ type: 'roomMsg', data: { type: 'snap', host: { x: 0, y: 1.6, z: 0, yaw: 0, alive: true }, enemies: [{ i: 0, x: 5, z: 5, alive: true, spawnT: 1 }] } });
    const snap = await guest.until(function (m) { return m.type === 'roomMsg' && m.data.type === 'snap'; });
    check('房主 → 加入者：敌人快照转发（房主权威）', !!(snap.data.enemies && snap.data.enemies.length === 1));

    host.send({ type: 'roomMsg', data: { type: 'kill', headshot: true } });
    const kill = await guest.until(function (m) { return m.type === 'roomMsg' && m.data.type === 'kill'; });
    check('击杀/受击/结束等对局消息转发', kill.data.headshot === true);

    // 8) 第三人挤不进满员房间
    const third = await wsConnect(PORT);
    third.send({ type: 'login', name: '路人Gamma' });
    await third.until(function (m) { return m.type === 'loginOk'; });
    third.send({ type: 'joinRoom', roomId: roomId });
    const full = await third.until(function (m) { return m.type === 'roomFull' || m.type === 'roomJoined'; });
    check('满员房间拒绝第三人', full.type === 'roomFull', full.reason || '');
    third.close();

    // 9) 加入者离开 -> 房主收到 peerLeft
    guest.send({ type: 'leaveRoom' });
    const peerLeft = await host.until(function (m) { return m.type === 'peerLeft'; });
    check('加入者离开，房主收到 peerLeft', !!peerLeft);

    // 10) 房主离开 -> 房间解散，大厅里消失
    const watcher = await wsConnect(PORT);
    watcher.send({ type: 'login', name: '旁观者Delta' });
    await watcher.until(function (m) { return m.type === 'loginOk'; });
    host.send({ type: 'leaveRoom' });
    const gone = await watcher.until(function (m) { return m.type === 'lobbyState' && !m.rooms.some(function (r) { return r.id === roomId; }); });
    check('房主离开后房间自动解散并从未大厅列表移除', !!gone);

    host.close(); guest.close(); watcher.close();
  } catch (e) {
    failed++;
    console.log('  ✗ 测试异常：' + (e && e.message));
  } finally {
    await sleep(200);
    server.kill();
    console.log('');
    console.log(failed === 0 ? '全部联机流程自测通过 ✅' : ('有 ' + failed + ' 项未通过 ❌'));
    process.exit(failed === 0 ? 0 : 1);
  }
})();
