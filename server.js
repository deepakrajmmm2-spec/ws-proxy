    let data = "";
    req.on("data", (c) => (data += c));
    req.on("end", () => {
      try { resolve(JSON.parse(data)); }
      catch { resolve({}); }
    });
  });
}

function checkSecret(req) {
  if (!PROXY_SECRET) return true;
  return req.headers["x-proxy-secret"] === PROXY_SECRET;
}

// ─── HTTP SERVER ──────────────────────────────────────────────
const server = http.createServer(async (req, res) => {
  const url    = new URL(req.url, `http://localhost`);
  const method = req.method.toUpperCase();

  // CORS preflight
  if (method === "OPTIONS") {
    res.writeHead(204, {
      "Access-Control-Allow-Origin" : "*",
      "Access-Control-Allow-Headers": "Content-Type, Authorization, x-proxy-secret",
      "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
    });
    return res.end();
  }

  // ── GET /ping ────────────────────────────────────────────
  if (method === "GET" && url.pathname === "/ping") {
    return sendJSON(res, 200, {
      ok: true,
      ts: Date.now(),
      wsConnected: state.wsConnected,
      tickCount  : state.tickCount,
    });
  }

  // ── GET /health ──────────────────────────────────────────
  if (method === "GET" && url.pathname === "/health") {
    return sendJSON(res, 200, {
      status     : "ok",
      wsConnected: state.wsConnected,
      loggedIn   : !!state.jwtToken,
      expiresAt  : state.expiresAt,
      expiresIn  : state.expiresAt ? Math.round((state.expiresAt - Date.now()) / 1000) : null,
      tickCount  : state.tickCount,
      clients    : state.wsClients.size,
      pingHistory: state.pingHistory,
    });
  }

  // ── GET /token ───────────────────────────────────────────
  if (method === "GET" && url.pathname === "/token") {
    if (!checkSecret(req)) return sendJSON(res, 401, { error: "Unauthorized" });
    return sendJSON(res, 200, {
      jwtToken  : state.jwtToken,
      feedToken : state.feedToken,
      expiresAt : state.expiresAt,
    });
  }

  // ── POST /login ──────────────────────────────────────────
  if (method === "POST" && url.pathname === "/login") {
    if (!checkSecret(req)) return sendJSON(res, 401, { error: "Unauthorized" });
    try {
      const data = await doLogin();
      await connectSmartStream();
      return sendJSON(res, 200, { ok: true, feedToken: data.feedToken, expiresAt: state.expiresAt });
    } catch (err) {
      return sendJSON(res, 500, { error: err.message });
    }
  }

  // ── POST /subscribe ──────────────────────────────────────
  if (method === "POST" && url.pathname === "/subscribe") {
    if (!checkSecret(req)) return sendJSON(res, 401, { error: "Unauthorized" });
    const body = await readBody(req);
    // body: { tokens: [{ exchangeType: 1, tokens: ["26000"] }], mode: 3 }
    if (!body.tokens) return sendJSON(res, 400, { error: "tokens required" });

    const sub = { tokens: body.tokens, mode: body.mode || MODE_SNAP_QUOT };
    state.subscribers.add(sub);

    if (state.wsConnected && state.wsClient) {
      state.wsClient.send(buildSubscribeMsg(body.tokens, sub.mode));
    }
    return sendJSON(res, 200, { ok: true, subscribed: body.tokens });
  }

  // ── POST /unsubscribe ────────────────────────────────────
  if (method === "POST" && url.pathname === "/unsubscribe") {
    if (!checkSecret(req)) return sendJSON(res, 401, { error: "Unauthorized" });
    const body = await readBody(req);
    if (state.wsConnected && state.wsClient) {
      state.wsClient.send(buildUnsubMsg(body.tokens, body.mode || MODE_SNAP_QUOT));
    }
    return sendJSON(res, 200, { ok: true });
  }

  // ── POST /refresh ────────────────────────────────────────
  if (method === "POST" && url.pathname === "/refresh") {
    if (!checkSecret(req)) return sendJSON(res, 401, { error: "Unauthorized" });
    try {
      await refreshToken();
      return sendJSON(res, 200, { ok: true, expiresAt: state.expiresAt });
    } catch (err) {
      return sendJSON(res, 500, { error: err.message });
    }
  }

  // ── Angel One REST proxy ─────────────────────────────────
  if (url.pathname.startsWith("/angel/")) {
    if (!checkSecret(req)) return sendJSON(res, 401, { error: "Unauthorized" });
    const angelPath = url.pathname.replace("/angel", "");
    const body      = await readBody(req);
    try {
      const result = await angelPost(angelPath, body, state.jwtToken);
      return sendJSON(res, 200, result);
    } catch (err) {
      return sendJSON(res, 500, { error: err.message });
    }
  }

  sendJSON(res, 404, { error: "Not found" });
});

// ─── BROWSER WEBSOCKET SERVER ─────────────────────────────────
const wss = new WebSocket.Server({ server });

wss.on("connection", (ws, req) => {
  // optional secret check via query param
  const url    = new URL(req.url, "http://localhost");
  const secret = url.searchParams.get("secret");
  if (PROXY_SECRET && secret !== PROXY_SECRET) {
    ws.close(4401, "Unauthorized");
    return;
  }

  state.wsClients.add(ws);
  log(`🖥️  Browser client connected (total: ${state.wsClients.size})`);

  // send current status immediately
  ws.send(JSON.stringify({
    type      : "STATUS",
    wsConnected: state.wsConnected,
    loggedIn  : !!state.jwtToken,
    expiresAt : state.expiresAt,
    feedToken : state.feedToken,
  }));

  ws.on("message", async (raw) => {
    try {
      const msg = JSON.parse(raw);

      if (msg.type === "SUBSCRIBE" && msg.tokens) {
        const sub = { tokens: msg.tokens, mode: msg.mode || MODE_SNAP_QUOT };
        state.subscribers.add(sub);
        if (state.wsConnected && state.wsClient) {
          state.wsClient.send(buildSubscribeMsg(msg.tokens, sub.mode));
        }
        ws.send(JSON.stringify({ type: "SUBSCRIBED", tokens: msg.tokens }));
      }

      if (msg.type === "LOGIN") {
        try {
          await doLogin();
          await connectSmartStream();
          ws.send(JSON.stringify({ type: "LOGGED_IN", feedToken: state.feedToken }));
        } catch (err) {
          ws.send(JSON.stringify({ type: "LOGIN_ERROR", error: err.message }));
        }
      }

      if (msg.type === "PING") {
        ws.send(JSON.stringify({ type: "PONG", ts: Date.now() }));
      }
    } catch {}
  });

  ws.on("close", () => {
    state.wsClients.delete(ws);
    log(`🔌 Browser client disconnected (total: ${state.wsClients.size})`);
  });

  ws.on("error", () => state.wsClients.delete(ws));
});

// ─── SMARTSTREAM PING/LATENCY ─────────────────────────────────
setInterval(() => {
  if (!state.wsConnected || !state.wsClient) return;
  const t = Date.now();
  try {
    state.wsClient.ping();
    state.wsClient.once("pong", () => {
      const ms = Date.now() - t;
      state.pingHistory.push(ms);
      if (state.pingHistory.length > 10) state.pingHistory.shift();
    });
  } catch {}
}, 30_000);

// ─── STARTUP ──────────────────────────────────────────────────
server.listen(PORT, async () => {
  log(`🚀 ProChain Proxy running on port ${PORT}`);

  if (CLIENT_ID && MPIN && TOTP_SECRET && API_KEY) {
    try {
      await doLogin();
      await connectSmartStream();
    } catch (err) {
      log("⚠️ Auto-login failed:", err.message, "— will retry via /login endpoint");
    }
  } else {
    log("⚠️ ENV vars not set — call POST /login to authenticate");
  }
});

// ─── GRACEFUL SHUTDOWN ────────────────────────────────────────
process.on("SIGTERM", () => {
  log("🛑 SIGTERM received, shutting down...");
  if (state.wsClient) state.wsClient.terminate();
  server.close(() => process.exit(0));
});
process.on("SIGINT", () => {
  if (state.wsClient) state.wsClient.terminate();
  server.close(() => process.exit(0));
});
