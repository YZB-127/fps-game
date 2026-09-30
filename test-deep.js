/*
 * server.js 深度压力 / 边界自测（现有 test-mp.js 只覆盖了主流程）
 *   node test-deep.js
 * 覆盖：大消息(64位长度) / 分片重组 / 控制帧插入 / 心跳存活 / 掉线清理 /
 *       并发登录 / 房间泄漏 / 畸形输入 / 异常断开 / 空房间转发 / 房间号唯一性
 */
'use strict';

const { spawn } = require('child_process');
const crypto = require('crypto');
const net = require('net');
const path = require('path');
const http = require('http');

const PORT = Number(process.env.DEEP_PORT || 8135);
const HOST = '127.0.0.1';

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

/* ---------------- 极简 WS 客户端（可控分片 / 控制帧） ---------------- */
function wsConnect(port) {
  return new Promise(function (resolve, reject) {
    const key = crypto.randomBytes(16).toString('base64');
    const sock = net.connect(port, HOST, function () {
      sock.write(
        'GET / HTTP/1.1\r\nHost: ' + HOST + ':' + port + '\r\n' +
        'Upgrade: websocket\r\nConnection: Upgrade\r\n' +
        'Sec-WebSocket-Key: ' + key + '\r\nSec-WebSocket-Version: 13\r\n\r\n'
      );
    });
    sock.once('error', e => { if (!settled) { settled = true; reject(e); } });
    let buf = Buffer.alloc(0), handshaken = false, settled = false;
    let api = null;
    const inbox = [], waiters = [];
    let autoPong = true;
    let gotPing = 0;
    let closed = false;

    function deliver(m) { waiters.length ? waiters.shift()(m) : inbox.push(m); }

    function rawFrame(opcode, payload, fin) {
      const data = Buffer.isBuffer(payload) ? payload : Buffer.from(String(payload), 'utf8');
      const mask = crypto.randomBytes(4);
      let header;
      if (data.length < 126) { header = Buffer.allocUnsafe(2); header[1] = 0x80 | data.length; }
      else if (data.length < 65536) { header = Buffer.allocUnsafe(4); header[1] = 0x80 | 126; header.writeUInt16BE(data.length, 2); }
      else { header = Buffer.allocUnsafe(10); header[1] = 0x80 | 127; header.writeUInt32BE(Math.floor(data.length / 4294967296), 2); header.writeUInt32BE(data.length >>> 0, 6); }
      header[0] = (fin === false ? 0x00 : 0x80) | opcode;
      const masked = Buffer.allocUnsafe(data.length);
      for (let i = 0; i < data.length; i++) masked[i] = data[i] ^ mask[i & 3];
      sock.write(Buffer.concat([header, mask, masked]));
    }

    sock.on('data', function (chunk) {
      buf = Buffer.concat([buf, chunk]);
      if (!handshaken) {
        const i = buf.indexOf('\r\n\r\n');
        if (i < 0) return;
        const head = buf.slice(0, i).toString();
        if (!/ 101 /.test(head)) { if (!settled) { settled = true; reject(new Error('握手失败: ' + head.split('\r\n')[0])); } return; }
        handshaken = true; buf = buf.slice(i + 4);
        if (!settled) { settled = true; resolve(api); }
      }
      for (;;) {
        if (buf.length < 2) return;
        const opcode = buf[0] & 0x0f;
        let len = buf[1] & 0x7f, off = 2;
        if (len === 126) { if (buf.length < 4) return; len = buf.readUInt16BE(2); off = 4; }
        else if (len === 127) { if (buf.length < 10) return; len = buf.readUInt32BE(2) * 4294967296 + buf.readUInt32BE(6); off = 10; }
        if (buf.length < off + len) return;
        const payload = buf.slice(off, off + len);
        buf = buf.slice(off + len);
        if (opcode === 0x9) { gotPing++; if (autoPong) rawFrame(0xA, payload); }        // ping -> pong
        else if (opcode === 0x8) { closed = true; }
        else if (opcode === 0x1) { try { deliver(JSON.parse(payload.toString('utf8'))); } catch (e) { deliver({ __bad: payload.toString('utf8').slice(0, 80) }); } }
      }
    });

    function next(timeoutMs) {
      if (inbox.length) return Promise.resolve(inbox.shift());
      return new Promise(function (res, rej) {
        const t = setTimeout(() => rej(new Error('等待消息超时')), timeoutMs || 3000);
        waiters.push(m => { clearTimeout(t); res(m); });
      });
    }

    sock.once('close', () => { closed = true; if (!settled) { settled = true; reject(new Error('握手前被关闭')); } });

    api = {
      raw: rawFrame,
      send: o => rawFrame(0x1, JSON.stringify(o)),
      sendRawText: s => rawFrame(0x1, s),
      next,
      get pingCount() { return gotPing; },
      get isClosed() { return closed; },
      setAutoPong: v => { autoPong = v; },
      close: () => { try { sock.destroy(); } catch (e) {} },
      halfClose: () => { try { sock.end(); } catch (e) {} },   // 只发 FIN，不发 WebSocket close 帧
      destroyNow: () => { try { sock.resetAndDestroy ? sock.resetAndDestroy() : sock.destroy(); } catch (e) {} },
      until: async function (pred, timeoutMs) {
        const deadline = Date.now() + (timeoutMs || 4000);
        for (;;) {
          const left = deadline - Date.now();
          if (left <= 0) throw new Error('未等到期望消息');
          const m = await next(left);
          if (pred(m)) return m;
        }
      },
      login: async function (name) {
        api.send({ type: 'login', name: name });
        return api.until(m => m.type === 'loginOk');
      },
      drain: function () { inbox.length = 0; }
    };
  });
}

/* ---------------- 测试主体 ---------------- */
let failed = 0, passed = 0;
function check(label, ok, extra) {
  console.log((ok ? '  ✓ ' : '  ✗ ') + label + (extra ? '  → ' + extra : ''));
  ok ? passed++ : failed++;
}

(async function main() {
  const server = spawn(process.execPath, [path.join(__dirname, 'server.js'), String(PORT)], { stdio: ['ignore', 'pipe', 'pipe'] });
  let serverErr = '';
  server.stderr.on('data', d => { serverErr += d.toString(); });
  server.stdout.on('data', () => {});

  const only = process.env.DEEP_ONLY || '';

  try {
    for (let i = 0; i < 40; i++) {
      const up = await new Promise(res => { const s = net.connect(PORT, HOST); s.once('connect', () => { s.destroy(); res(true); }); s.once('error', () => res(false)); });
      if (up) break;
      await sleep(150);
    }

    if (!only || only === 'large') {
      console.log('\n【1】大消息（>64KB，走 8 字节长度分支）');
      const a = await wsConnect(PORT), b = await wsConnect(PORT);
      await a.login('大字A'); await b.login('大字B');
      a.send({ type: 'createRoom' });
      const room = (await a.until(m => m.type === 'roomCreated')).room.id;
      b.send({ type: 'joinRoom', roomId: room });
      await b.until(m => m.type === 'roomJoined');
      a.drain(); b.drain();
      const big = 'x'.repeat(200 * 1024);           // 200KB
      a.send({ type: 'roomMsg', data: { type: 'snap', blob: big } });
      const got = await b.until(m => m.type === 'roomMsg' && m.data && m.data.type === 'snap', 6000);
      check('200KB 消息房主→加入者完整中继', got.data.blob.length === 200 * 1024, got.data.blob.length + ' 字节');
      b.send({ type: 'roomMsg', data: { type: 'me', blob: big } });
      const got2 = await a.until(m => m.type === 'roomMsg' && m.data && m.data.type === 'me', 6000);
      check('200KB 消息加入者→房主完整中继', got2.data.blob.length === 200 * 1024, got2.data.blob.length + ' 字节');
      a.close(); b.close();
    }

    if (!only || only === 'frag') {
      console.log('\n【2】分片重组 + 分片间插入控制帧（RFC6455 要求支持）');
      const a = await wsConnect(PORT), b = await wsConnect(PORT);
      await a.login('分片A'); await b.login('分片B');
      a.send({ type: 'createRoom' });
      const room = (await a.until(m => m.type === 'roomCreated')).room.id;
      b.send({ type: 'joinRoom', roomId: room });
      await b.until(m => m.type === 'roomJoined');
      a.drain(); b.drain();

      const full = JSON.stringify({ type: 'roomMsg', data: { type: 'kill', note: 'FRAGMENTED-PAYLOAD-OK' } });
      const half = Math.floor(full.length / 2);
      b.raw(0x1, full.slice(0, half), false);          // 首片 fin=0
      b.raw(0x9, 'mid');                               // 片间插 ping（合法）
      b.raw(0x0, full.slice(half), true);              // 续片 fin=1
      const got = await a.until(m => m.type === 'roomMsg' && m.data && m.data.note === 'FRAGMENTED-PAYLOAD-OK', 5000);
      check('两片分片消息被正确重组并转发', !!got, '分片点 ' + half);

      // 未分片的正常消息在分片机制之后仍要工作
      b.send({ type: 'roomMsg', data: { type: 'kill', note: 'PLAIN-AFTER-FRAG' } });
      const plain = await a.until(m => m.type === 'roomMsg' && m.data && m.data.note === 'PLAIN-AFTER-FRAG', 5000);
      check('分片之后普通消息仍正常（fragOp 状态未污染）', !!plain);
      a.close(); b.close();
    }

    if (!only || only === 'robust') {
      console.log('\n【3】畸形 / 恶意输入不能搞崩服务器');
      const bad = await wsConnect(PORT);
      await bad.login('捣乱者');
      bad.sendRawText('这不是JSON{{{');
      bad.raw(0x2, Buffer.from([1, 2, 3, 4]));         // 二进制帧
      bad.raw(0x0, '孤儿续片');                         // 没有首片的续片
      bad.send({ type: 'roomMsg', data: { type: 'me' } });   // 不在房间里发对局消息
      bad.send({ type: 'joinRoom', roomId: '不存在的房间' });
      bad.send({ type: 'roomState', state: 'playing' });     // 不是房主
      bad.send({ type: 'unknownType', foo: 1 });
      bad.send({ type: 'login', name: '<img src=x onerror=alert(1)>非常非常非常非常长的昵称' });
      const okName = await bad.until(m => m.type === 'loginOk', 3000);
      check('畸形输入后服务器仍存活并响应', !!okName, '清洗后昵称="' + okName.name + '"（长度 ' + Array.from(okName.name).length + '）');
      check('昵称 XSS 字符被剥离', !/[<>"'&\\\/]/.test(okName.name), okName.name);

      const alive = await new Promise(res => {
        http.get({ host: HOST, port: PORT, path: '/health', agent: false }, r => {
          let s = ''; r.on('data', c => s += c); r.on('end', () => res(s));
        }).on('error', e => res('ERR ' + e.message));
      });
      check('服务器进程未崩溃（/health 正常）', /"ok":true/.test(alive), alive);
      bad.close();
    }

    if (!only || only === 'concurrent') {
      console.log('\n【4】并发登录 + 房间号唯一性 + 房间回收（无泄漏）');
      const cs = [];
      for (let i = 0; i < 30; i++) cs.push(await wsConnect(PORT));
      await Promise.all(cs.map((c, i) => c.login('并发' + i)));
      check('30 个客户端同时登录成功', true);

      const ids = new Set();
      for (let i = 0; i < 30; i++) {
        cs[i].drain();
        cs[i].send({ type: 'createRoom' });
        const r = await cs[i].until(m => m.type === 'roomCreated', 4000);
        ids.add(r.room.id);
      }
      check('30 个房间号互不重复', ids.size === 30, '唯一 ' + ids.size + '/30');

      const h1 = await new Promise(res => {
        http.get({ host: HOST, port: PORT, path: '/health', agent: false }, r => { let s = ''; r.on('data', c => s += c); r.on('end', () => res(JSON.parse(s))); }).on('error', () => res(null));
      });
      check('房间全部登记（rooms=30）', h1 && h1.rooms === 30, JSON.stringify(h1));

      // 全部断开 -> 房间必须全部回收
      cs.forEach(c => c.close());
      await sleep(600);
      const h2 = await new Promise(res => {
        http.get({ host: HOST, port: PORT, path: '/health', agent: false }, r => { let s = ''; r.on('data', c => s += c); r.on('end', () => res(JSON.parse(s))); }).on('error', () => res(null));
      });
      check('全部断开后房间自动回收（rooms=0）', h2 && h2.rooms === 0, JSON.stringify(h2));
      check('全部断开后连接回收（clients=0）', h2 && h2.clients === 0, JSON.stringify(h2));
    }

    if (!only || only === 'abrupt') {
      console.log('\n【5】异常断开 / 对局中房主跑路');
      const a = await wsConnect(PORT), b = await wsConnect(PORT);
      await a.login('房主X'); await b.login('加入者Y');
      a.send({ type: 'createRoom' });
      const room = (await a.until(m => m.type === 'roomCreated')).room.id;
      b.send({ type: 'joinRoom', roomId: room });
      await b.until(m => m.type === 'roomJoined');
      a.send({ type: 'roomState', state: 'playing' });
      await sleep(120);
      b.drain();
      a.destroyNow();                                   // 房主直接 RST，不发 close 帧
      const closedMsg = await b.until(m => m.type === 'roomClosed' || m.type === 'error', 5000);
      check('房主异常断开，加入者收到 roomClosed', closedMsg.type === 'roomClosed', closedMsg.type);

      const a2 = await wsConnect(PORT), b2 = await wsConnect(PORT);
      await a2.login('房主Z'); await b2.login('加入者W');
      a2.send({ type: 'createRoom' });
      const r2 = (await a2.until(m => m.type === 'roomCreated')).room.id;
      b2.send({ type: 'joinRoom', roomId: r2 });
      await b2.until(m => m.type === 'roomJoined');
      a2.drain(); b2.drain();
      b2.destroyNow();                                  // 加入者 RST
      const leftMsg = await a2.until(m => m.type === 'peerLeft', 5000);
      check('加入者异常断开，房主收到 peerLeft', leftMsg.type === 'peerLeft');

      // 加入者掉线后房间应回到 waiting，别人可以补位
      const c2 = await wsConnect(PORT);
      await c2.login('补位V');
      c2.send({ type: 'joinRoom', roomId: r2 });
      const rejoined = await c2.until(m => m.type === 'roomJoined' || m.type === 'roomFull', 4000);
      check('加入者掉线后房间回到 waiting，新人可补位', rejoined.type === 'roomJoined', rejoined.reason || '');
      a2.close(); b.close(); c2.close();
    }

    if (!only || only === 'rejoin') {
      console.log('\n【6】重复操作 / 状态机边界');
      const a = await wsConnect(PORT), b = await wsConnect(PORT);
      await a.login('重复A'); await b.login('重复B');
      a.send({ type: 'createRoom' });
      const r1 = (await a.until(m => m.type === 'roomCreated')).room.id;
      // 房主重复建房：旧房间必须解散
      a.send({ type: 'createRoom' });
      const r2 = (await a.until(m => m.type === 'roomCreated')).room.id;
      check('房主重复建房会解散旧房间并给新号', r1 !== r2, r1 + ' -> ' + r2);

      b.send({ type: 'joinRoom', roomId: r1 });
      const dead = await b.until(m => m.type === 'roomFull' || m.type === 'roomJoined', 4000);
      check('加入已解散的旧房间被正确拒绝', dead.type === 'roomFull', dead.reason || '');

      b.send({ type: 'joinRoom', roomId: r2 });
      await b.until(m => m.type === 'roomJoined');
      b.drain();
      b.send({ type: 'joinRoom', roomId: r2 });        // 重复加入同一房间
      const again = await b.until(m => m.type === 'roomJoined', 4000);
      check('已在房内重复点加入 → 幂等返回 roomJoined', !!again);

      // 加入者 leave 后大厅状态回 waiting
      b.send({ type: 'leaveRoom' });
      await sleep(150);
      b.send({ type: 'getLobby' });
      const lob = await b.until(m => m.type === 'lobbyState' && m.rooms.some(r => r.id === r2), 4000);
      const row = lob.rooms.filter(r => r.id === r2)[0];
      check('加入者离开后房间状态回 waiting 且可再加入', row.state === 'waiting' && row.players === 1, row.state + ' ' + row.players + '/' + row.maxPlayers);
      a.close(); b.close();
    }

    if (!only || only === 'halfclose') {
      console.log('\n【7】半关闭（FIN，无 close 帧）必须立刻回收 —— 回归测试');
      console.log('     http.Server 升级的 socket 是 allowHalfOpen=true，只发 FIN 不触发 close 事件；');
      console.log('     浏览器崩溃 / 强杀进程 / 拔网线 / 睡眠 走的就是这条路。');
      const a = await wsConnect(PORT), b = await wsConnect(PORT);
      await a.login('崩溃房主'); await b.login('留在房里的加入者');
      a.send({ type: 'createRoom' });
      const room = (await a.until(m => m.type === 'roomCreated')).room.id;
      b.send({ type: 'joinRoom', roomId: room });
      await b.until(m => m.type === 'roomJoined');
      b.drain();
      const before = await new Promise(res => {
        http.get({ host: HOST, port: PORT, path: '/health', agent: false }, r => { let s = ''; r.on('data', c => s += c); r.on('end', () => res(JSON.parse(s))); }).on('error', () => res(null));
      });

      const t0 = Date.now();
      a.halfClose();                                     // 只发 FIN
      let notified = null;
      try { await b.until(m => m.type === 'roomClosed', 5000); notified = Date.now(); } catch (e) {}
      check('半关闭后加入者 5 秒内收到 roomClosed', !!notified, notified ? ((notified - t0) / 1000).toFixed(2) + ' 秒' : '超时未收到');

      await sleep(300);
      const h = await new Promise(res => {
        http.get({ host: HOST, port: PORT, path: '/health', agent: false }, r => { let s = ''; r.on('data', c => s += c); r.on('end', () => res(JSON.parse(s))); }).on('error', () => res(null));
      });
      check('半关闭的连接与房间立即从服务器状态里消失',
        !!h && h.rooms === before.rooms - 1 && h.clients === before.clients - 1,
        '断开前 ' + JSON.stringify(before) + ' → 断开后 ' + JSON.stringify(h));
      b.close();
    }

    if (!only || only === 'heartbeat') {
      console.log('\n【8】心跳：正常客户端不能被误踢 / 哑客户端必须被清理（约 65s）');
      const good = await wsConnect(PORT);
      await good.login('正常挂机');
      const mute = await wsConnect(PORT);
      await mute.login('哑巴客户端');
      mute.setAutoPong(false);                          // 不回 pong（但连接本身不断开）
      const p0 = good.pingCount;
      await sleep(65000);                               // 45s 宽限 + 15s 巡检间隔 = 最坏 60s
      check('正常客户端收到服务器 ping（心跳在发）', good.pingCount > p0 + 1, '收到 ' + good.pingCount + ' 次 ping');
      check('正常客户端 65s 后仍在线（未被误踢）', !good.isClosed, 'closed=' + good.isClosed);
      check('不回 pong 的客户端被心跳清理', mute.isClosed, 'closed=' + mute.isClosed);
      try { good.send({ type: 'getLobby' }); } catch (e) {}
      good.close();
    }
  } catch (e) {
    failed++;
    console.log('  ✗ 测试异常：' + (e && e.stack ? e.stack.split('\n')[0] : e));
  } finally {
    await sleep(200);
    server.kill();
    if (serverErr.trim()) console.log('\n[server stderr]\n' + serverErr.trim());
    console.log('\n================================');
    console.log('通过 ' + passed + ' 项，失败 ' + failed + ' 项' + (failed === 0 ? ' ✅' : ' ❌'));
    process.exit(failed === 0 ? 0 : 1);
  }
})();
