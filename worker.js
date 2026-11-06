// ADWMX Relay — وسيط Cloudflare Worker + Durable Object.
// هاتف السيرفر بيفتح WebSocket طالع على /t/<ID>، والأجهزة التانية بتبعت HTTP على /r/<ID>/... والوسيط بيمرّره للهاتف.

const json = (o, s = 200) => new Response(JSON.stringify(o), { status: s, headers: { "content-type": "application/json; charset=utf-8" } });
const b64 = (u8) => { let s = ""; for (let i = 0; i < u8.length; i += 8192) s += String.fromCharCode.apply(null, u8.subarray(i, i + 8192)); return btoa(s); };
const unb64 = (s) => { const r = atob(s), u = new Uint8Array(r.length); for (let i = 0; i < r.length; i++) u[i] = r.charCodeAt(i); return u; };
const BLOCK = /^x-(forwarded|real|cf|amzn)/;

export class Room {
  constructor(state, env) {
    this.state = state; this.pending = new Map(); this.n = 0;
    state.setWebSocketAutoResponse(new WebSocketRequestResponsePair("ping", "pong"));
  }

  async fetch(request) {
    const url = new URL(request.url);
    if (request.headers.get("Upgrade") === "websocket") return this.accept(request);

    const sockets = this.state.getWebSockets();
    const ws = sockets[sockets.length - 1];
    if (!ws) return json({ error: "السيرفر غير متصل حالياً" }, 503);

    const method = request.method, h = {};
    for (const [k, v] of request.headers) if ((k.startsWith("x-") && !BLOCK.test(k)) || k === "content-type") h[k] = v;
    let body = "";
    if (method !== "GET" && method !== "HEAD") {
      const buf = new Uint8Array(await request.arrayBuffer());
      if (buf.length > 700000) return json({ error: "الطلب كبير" }, 413);
      body = b64(buf);
    }
    const id = ++this.n;
    const ts = new TransformStream(), writer = ts.writable.getWriter();
    let resolveHead; const head = new Promise((r) => (resolveHead = r));
    this.pending.set(id, { writer, resolveHead });
    const cancel = () => { if (this.pending.delete(id)) { try { ws.send(JSON.stringify({ t: "x", i: id })); } catch (e) {} writer.abort().catch(() => {}); } };
    request.signal.addEventListener("abort", cancel);
    const path = url.pathname.replace(/^\/r\/[^/]+/, "") + url.search;
    try { ws.send(JSON.stringify({ t: "q", i: id, m: method, p: path || "/", h, b: body })); }
    catch (e) { this.pending.delete(id); return json({ error: "السيرفر غير متصل" }, 503); }

    const timer = new Promise((r) => setTimeout(() => r(null), 30000));
    const r = await Promise.race([head, timer]);
    if (!r) { cancel(); return json({ error: "السيرفر مش بيرد" }, 504); }
    return new Response(r.s === 204 || r.s === 304 ? null : ts.readable, { status: r.s, headers: { "content-type": (r.h && r.h["content-type"]) || "application/octet-stream", "cache-control": "no-store" } });
  }

  async accept(request) {
    const key = request.headers.get("x-key") || "";
    if (key.length < 8) return new Response("bad key", { status: 400 });
    const saved = await this.state.storage.get("key");
    if (saved && saved !== key) return new Response("forbidden", { status: 403 }); // أول جهاز حجز الـ ID
    if (!saved) await this.state.storage.put("key", key);
    for (const old of this.state.getWebSockets()) { try { old.close(1000, "replaced"); } catch (e) {} }
    const pair = new WebSocketPair(), client = pair[0], server = pair[1];
    this.state.acceptWebSocket(server);
    return new Response(null, { status: 101, webSocket: client });
  }

  webSocketMessage(ws, message) {
    let m; try { m = JSON.parse(typeof message === "string" ? message : new TextDecoder().decode(message)); } catch (e) { return; }
    const p = this.pending.get(m.i);
    if (!p) { try { ws.send(JSON.stringify({ t: "x", i: m.i })); } catch (e) {} return; }
    if (m.t === "h") p.resolveHead({ s: m.s, h: m.h });
    else if (m.t === "d") p.writer.write(unb64(m.b)).catch(() => { this.pending.delete(m.i); try { ws.send(JSON.stringify({ t: "x", i: m.i })); } catch (e) {} });
    else if (m.t === "e") { this.pending.delete(m.i); p.writer.close().catch(() => {}); }
  }

  webSocketClose(ws) { this.failAll(); }
  webSocketError(ws) { this.failAll(); }
  failAll() {
    if (this.state.getWebSockets().length) return; // لسه فيه اتصال تاني (استبدال)
    for (const [, p] of this.pending) { p.resolveHead({ s: 502, h: { "content-type": "application/json" } }); p.writer.close().catch(() => {}); }
    this.pending.clear();
  }
}

export default {
  async fetch(request, env) {
    const m = new URL(request.url).pathname.match(/^\/(r|t)\/([A-Za-z0-9_-]{3,80})(\/.*)?$/);
    if (!m) return new Response("ADWMX relay OK", { headers: { "content-type": "text/plain; charset=utf-8" } });
    if (m[1] === "t" && request.headers.get("Upgrade") !== "websocket") return new Response("expected websocket", { status: 426 });
    return env.ROOM.get(env.ROOM.idFromName(m[2])).fetch(request);
  },
};
