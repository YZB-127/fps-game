/*
 * 聚焦实验：客户端断开后，服务器是否回收连接/房间？
 *   node test-reclaim.js
 */
'use strict';
const { spawn } = require('child_process');
const crypto = require('crypto');
const net = require('net');
const path = require('path');
const http = require('http');

const PORT = 8146, HOST = '127.0.0.1';
const sleep = ms => new Promise(r => setTimeout(r, ms));

function wsConnect(port) {
  return new Promise(function (resolve, reject) {
    const key = crypto.randomBytes(16).toString('base64');
    const sock = net.connect(port, HOST, function () {
      sock.write('GET / HTTP/1.1\r\nHost: ' + HOST + ':' + port + '\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n' +
        'Sec-WebSocket-Key: ' + key + '\r\nSec-WebSocket-Version: 13\r\n\r\n');
    });
    sock.once('error', e => reject(e));
    let buf = Buffer.alloc(0), hs = false, settled = false, api = null, isClosed = false;
    const inbox = [], waiters = [];
    function deliver(m) { waiters.length ? waiters.shift()(m) : inbox.push(m); }
    sock.on('data', function (chunk) {
      buf = Buffer.concat([buf, chunk]);
      if (!hs) {
        const i = buf.indexOf('\r\n\r\n'); if (i < 0) return;
        hs = true; buf = buf.slice(i + 4);
        if (!settled) { settled = true; resolve(api); }
      }
      for (;;) {
        if (buf.length < 2) return;
        const op = buf[0] & 0x0f; let len = buf[1] & 0x7f, off = 2;
        if (len === 126) { if (buf.length < 4) return; len = buf.readUInt16BE(2); off = 4; }
        else if (len === 127) { if (buf.length < 10) return; len = buf.readUInt32BE(6); off = 10; }
        if (buf.length < off + len) return;
        const p = buf.slice(off, off + len); buf = buf.slice(off + len);
        if (op === 0x1) { try { deliver(JSON.parse(p.toString())); } catch (e) {} }
      }
    });
    sock.once('close', () => { isClosed = true; });
    api = {
      send: o => { const d = Buffer.from(JSON.stringify(o)); const m = crypto.randomBytes(4);
        const h = Buffer.allocUnsafe(2 + m.length + d.length); h[0] = 0x81; h[1] = 0x80 | d.length;
        m.copy(h, 2); for (let i = 0; i < d.length; i++) h[2 + 4 + i] = d[i] ^ m[i & 3]; sock.write(h); },
      next: t => inbox.length ? Promise.resolve(inbox.shift()) : new Promise((res, rej) => { const to = setTimeout(() => rej(new Error('timeout')), t || 3000); waiters.push(m => { clearTimeout(to); res(m); }); }),
      get isClosed() { return isClosed; },
      destroy: () => sock.destroy(),
      endGraceful: () => sock.end(),
      closeFrame: () => { const m = crypto.randomBytes(4); sock.write(Buffer.concat([Buffer.from([0x88, 0x80]), m])); },
      login: async function (n) { api.send({ type: 'login', name: n }); for (;;) { const m = await api.next(3000); if (m.type === 'loginOk') return m; } },
      until: async function (pred, t) { const dl = Date.now() + (t || 4000); for (;;) { if (Date.now() > dl) throw new Error('timeout'); const m = await api.next(dl - Date.now()); if (pred(m)) return m; } }
    };
  });
}

function health(port) {
  return new Promise(res => {
    http.get({ host: HOST, port: port, path: '/health', agent: false }, r => {
      let s = ''; r.on('data', c => s += c); r.on('end', () => { try { res(JSON.parse(s)); } catch (e) { res(null); } });
    }).on('error', () => res(null));
  });
}

(async function () {
  const srv = spawn(process.execPath, [path.join(__dirname, 'server.js'), String(PORT)], { stdio: ['ignore', 'pipe', 'pipe'] });
  srv.stderr.on('data', d => process.stdout.write('[server!] ' + d));
  try {
    for (let i = 0; i < 40; i++) { const up = await new Promise(r => { const s = net.connect(PORT, HOST); s.once('connect', () => { s.destroy(); r(true); }); s.once('error', () => r(false)); }); if (up) break; await sleep(150); }

    async function scenario(label, teardown) {
      const cs = [];
      for (let i = 0; i < 3; i++) { const c = await wsConnect(PORT); await c.login('回收' + i); cs.push(c); }
      for (const c of cs) { c.send({ type: 'createRoom' }); await c.until(m => m.type === 'roomCreated'); }
      const before = await health(PORT);
      teardown(cs);
      let last = null;
      for (let t = 0; t < 12; t++) { await sleep(250); last = await health(PORT); if (last && last.rooms === 0 && last.clients === 0) break; }
      const secs = '（轮询至多 3s）';
      console.log('\n【' + label + '】' + secs);
      console.log('   断开前: rooms=' + before.rooms + ' clients=' + before.clients);
      console.log('   断开后: rooms=' + last.rooms + ' clients=' + last.clients + (last.rooms === 0 && last.clients === 0 ? '  ✓ 已回收' : '  ✗ 未回收（泄漏）'));
      return last;
    }

    const r1 = await scenario('A. 客户端 sock.destroy() 硬断开', cs => cs.forEach(c => c.destroy()));
    const r2 = await scenario('B. 客户端发 WebSocket close 帧（正常关闭）', cs => cs.forEach(c => c.closeFrame()));
    const r3 = await scenario('C. 客户端 sock.end() 半关闭（只关写方向）', cs => cs.forEach(c => c.endGraceful()));

    console.log('\n--- 诊断 ---');
    console.log('A destroy  : ' + (r1.rooms === 0 ? '正常' : '泄漏 —— 硬断开未触发 close 事件'));
    console.log('B close帧  : ' + (r2.rooms === 0 ? '正常' : '泄漏'));
    console.log('C end()    : ' + (r3.rooms === 0 ? '正常' : '泄漏 —— 半关闭未处理（浏览器关闭标签页时可能走这条路径）'));
  } catch (e) {
    console.log('异常：' + e.stack);
  } finally {
    await sleep(200); srv.kill(); process.exit(0);
  }
})();
