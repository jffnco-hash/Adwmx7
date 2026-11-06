// ADWMX Relay — Cloudflare Worker + Durable Object
// Server phone connects by WebSocket: /t/<ID>
// Other clients send HTTP requests through: /r/<ID>/<path>

const json = (data, status = 200) =>
  new Response(JSON.stringify(data), {
    status,
    headers: { "content-type": "application/json; charset=utf-8", "cache-control": "no-store" },
  });

const toBase64 = (bytes) => {
  let binary = "";
  for (let i = 0; i < bytes.length; i += 8192) {
    binary += String.fromCharCode(...bytes.subarray(i, i + 8192));
  }
  return btoa(binary);
};

const fromBase64 = (value) => {
  const binary = atob(value);
  const bytes = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i++) bytes[i] = binary.charCodeAt(i);
  return bytes;
};

const BLOCKED_HEADER = /^x-(forwarded|real|cf|amzn)/i;

export class Room {
  constructor(state, env) {
    this.state = state;
    this.pending = new Map();
    this.nextRequestId = 0;
    state.setWebSocketAutoResponse(
      new WebSocketRequestResponsePair("ping", "pong")
    );
  }

  async fetch(request) {
    const url = new URL(request.url);

    if (request.headers.get("Upgrade")?.toLowerCase() === "websocket") {
      return this.accept(request);
    }

    const sockets = this.state.getWebSockets();
    const socket = sockets[sockets.length - 1];

    if (!socket || socket.readyState !== WebSocket.OPEN) {
      return json({ error: "السيرفر غير متصل حالياً" }, 503);
    }

    const method = request.method;
    const headers = {};
    for (const [name, value] of request.headers) {
      const lower = name.toLowerCase();
      if (
        (lower.startsWith("x-") && !BLOCKED_HEADER.test(lower)) ||
        lower === "content-type" ||
        lower === "authorization" ||
        lower === "accept"
      ) {
        headers[lower] = value;
      }
    }

    let body = "";
    if (method !== "GET" && method !== "HEAD") {
      const bytes = new Uint8Array(await request.arrayBuffer());
      if (bytes.length > 700000) {
        return json({ error: "الطلب أكبر من الحد المسموح" }, 413);
      }
      body = toBase64(bytes);
    }

    const id = ++this.nextRequestId;
    const stream = new TransformStream();
    const writer = stream.writable.getWriter();

    let resolveHeaders;
    const headersPromise = new Promise((resolve) => {
      resolveHeaders = resolve;
    });

    this.pending.set(id, { writer, resolveHeaders });

    const cancel = () => {
      const pending = this.pending.get(id);
      if (!pending) return;
      this.pending.delete(id);
      try {
        socket.send(JSON.stringify({ t: "x", i: id }));
      } catch {}
      pending.writer.abort().catch(() => {});
    };

    request.signal.addEventListener("abort", cancel, { once: true });

    const path = url.pathname.replace(/^\/r\/[^/]+/, "") + url.search;
    try {
      socket.send(JSON.stringify({
        t: "q",
        i: id,
        m: method,
        p: path || "/",
        h: headers,
        b: body,
      }));
    } catch {
      this.pending.delete(id);
      writer.abort().catch(() => {});
      return json({ error: "تعذر إرسال الطلب إلى هاتف السيرفر" }, 503);
    }

    const timeout = new Promise((resolve) => {
      setTimeout(() => resolve(null), 30000);
    });

    const responseHead = await Promise.race([headersPromise, timeout]);
    if (!responseHead) {
      cancel();
      return json({ error: "هاتف السيرفر لم يرد خلال المهلة" }, 504);
    }

    const status = Number(responseHead.s) || 502;
    return new Response(
      status === 204 || status === 304 ? null : stream.readable,
      {
        status,
        headers: {
          "content-type":
            (responseHead.h && responseHead.h["content-type"]) ||
            "application/octet-stream",
          "cache-control": "no-store",
        },
      }
    );
  }

  async accept(request) {
    const key = request.headers.get("x-key") || "";
    if (key.length < 8) {
      return new Response("bad key", { status: 400 });
    }

    const savedKey = await this.state.storage.get("key");
    if (savedKey && savedKey !== key) {
      return new Response("forbidden", { status: 403 });
    }
    if (!savedKey) {
      await this.state.storage.put("key", key);
    }

    for (const oldSocket of this.state.getWebSockets()) {
      try {
        oldSocket.close(1000, "replaced");
      } catch {}
    }

    const pair = new WebSocketPair();
    const client = pair[0];
    const server = pair[1];

    this.state.acceptWebSocket(server);
    return new Response(null, { status: 101, webSocket: client });
  }

  webSocketMessage(socket, message) {
    let packet;
    try {
      packet = JSON.parse(
        typeof message === "string"
          ? message
          : new TextDecoder().decode(message)
      );
    } catch {
      return;
    }

    const pending = this.pending.get(packet.i);
    if (!pending) {
      try {
        socket.send(JSON.stringify({ t: "x", i: packet.i }));
      } catch {}
      return;
    }

    if (packet.t === "h") {
      pending.resolveHeaders({
        s: packet.s,
        h: packet.h || {},
      });
    } else if (packet.t === "d") {
      try {
        pending.writer.write(fromBase64(packet.b || "")).catch(() => {
          this.pending.delete(packet.i);
          try {
            socket.send(JSON.stringify({ t: "x", i: packet.i }));
          } catch {}
        });
      } catch {
        this.pending.delete(packet.i);
      }
    } else if (packet.t === "e") {
      this.pending.delete(packet.i);
      pending.writer.close().catch(() => {});
    }
  }

  webSocketClose() {
    this.failAll();
  }

  webSocketError() {
    this.failAll();
  }

  failAll() {
    if (this.state.getWebSockets().length > 0) return;

    for (const [id, pending] of this.pending) {
      pending.resolveHeaders({
        s: 502,
        h: { "content-type": "application/json" },
      });
      pending.writer.close().catch(() => {});
      this.pending.delete(id);
    }
  }
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const match = url.pathname.match(/^\/(r|t)\/([A-Za-z0-9_-]{3,80})(\/.*)?$/);

    if (!match) {
      return new Response("ADWMX relay OK", {
        headers: { "content-type": "text/plain; charset=utf-8" },
      });
    }

    if (!env.ROOM) {
      return json({ error: "Durable Object binding ROOM غير مضبوط" }, 500);
    }

    if (
      match[1] === "t" &&
      request.headers.get("Upgrade")?.toLowerCase() !== "websocket"
    ) {
      return new Response("expected websocket", { status: 426 });
    }

    const roomId = env.ROOM.idFromName(match[2]);
    return env.ROOM.get(roomId).fetch(request);
  },
};
