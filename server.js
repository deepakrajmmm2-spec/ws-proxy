/**
 * ============================================================
 *  ProChain — Angel One SmartStream WebSocket Proxy
 *  Railway Deploy Ready | v3.0
 * ============================================================
 *  Features:
 *   - Angel One SmartAPI login + JWT session management
 *   - TOTP auto-generation (speakeasy)
 *   - SmartStream WebSocket binary tick parser
 *   - Multi-client broadcast (browser tabs / ProChain UI)
 *   - Auto token refresh before expiry (hot-swap, no disconnect)
 *   - REST endpoints: /login /token /subscribe /health /ping
 *   - CORS open (browser safe)
 *   - Graceful reconnect on WS drop
 * ============================================================
 */

"use strict";

const http        = require("http");
const https       = require("https");
const WebSocket   = require("ws");
const speakeasy   = require("speakeasy");
const crypto      = require("crypto");

// ─── ENV CONFIG ──────────────────────────────────────────────
const PORT            = process.env.PORT            || 3000;
const CLIENT_ID       = process.env.CLIENT_ID       || "";   // Angel One client id
const MPIN            = process.env.MPIN            || "";   // Angel One MPIN
const TOTP_SECRET     = process.env.TOTP_SECRET     || "";   // Base32 TOTP secret
const API_KEY         = process.env.API_KEY         || "";   // Angel One API key
const PROXY_SECRET    = process.env.PROXY_SECRET    || "";   // optional auth header

const ANGEL_BASE      = "https://apiconnect.angelone.in";
const SMARTSTREAM_URL = "wss://smartapisocket.angelone.in/smart-stream";

// ─── STATE ───────────────────────────────────────────────────
let state = {
  jwtToken      : null,
  feedToken     : null,
  refreshToken  : null,
  loginTime     : null,
  expiresAt     : null,       // ms epoch
  wsClient      : null,       // SmartStream WS
  wsConnected   : false,
  subscribers   : new Set(),  // {tokens, mode} per client
  pingHistory   : [],         // last 10 ping ms
  tickCount     : 0,
  reconnectTimer: null,
  refreshTimer  : null,
  wsClients     : new Set(),  // browser WebSocket clients
};

// ─── LOGGER ──────────────────────────────────────────────────
const log = (...a) => console.log(`[${new Date().toISOString()}]`, ...a);

// ─── TOTP ─────────────────────────────────────────────────────
function generateTOTP() {
  if (!TOTP_SECRET) throw new Error("TOTP_SECRET not set");
  return speakeasy.totp({ secret: TOTP_SECRET, encoding: "base32" });
}

// ─── ANGEL ONE REST HELPERS ──────────────────────────────────
function angelPost(path, body, jwt) {
  return new Promise((resolve, reject) => {
    const payload = JSON.stringify(body);
    const headers = {
      "Content-Type"  : "application/json",
      "Accept"        : "application/json",
      "X-UserType"    : "USER",
      "X-SourceID"    : "WEB",
      "X-ClientLocalIP": "127.0.0.1",
      "X-ClientPublicIP": "1.1.1.1",
      "X-MACAddress"  : "00:00:00:00:00:00",
      "X-PrivateKey"  : API_KEY,
      "Content-Length": Buffer.byteLength(payload),
    };
    if (jwt) headers["Authorization"] = `Bearer ${jwt}`;

    const url = new URL(ANGEL_BASE + path);
    const req = https.request(
      { hostname: url.hostname, path: url.pathname, method: "POST", headers },
      (res) => {
        let data = "";
        res.on("data", (c) => (data += c));
        res.on("end", () => {
          try { resolve(JSON.parse(data)); }
          catch { reject(new Error("JSON parse fail: " + data)); }
        });
      }
    );
    req.on("error", reject);
    req.write(payload);
    req.end();
  });
}

// ─── LOGIN ────────────────────────────────────────────────────
async function doLogin() {
  log("🔐 Logging in to Angel One...");
  const totp = generateTOTP();
  const res = await angelPost("/rest/auth/angelbroking/user/v1/loginByPassword", {
    clientcode: CLIENT_ID,
    password  : MPIN,
    totp,
  });

  if (!res?.data?.jwtToken) {
    throw new Error("Login failed: " + JSON.stringify(res));
  }

  state.jwtToken     = res.data.jwtToken;
  state.feedToken    = res.data.feedToken;
  state.refreshToken = res.data.refreshToken;
  state.loginTime    = Date.now();
  state.expiresAt    = Date.now() + 28 * 60 * 1000; // 28 min safety margin

  log(`✅ Login OK | feedToken: ${state.feedToken?.slice(0, 20)}...`);
  scheduleTokenRefresh();
  return res.data;
}

// ─── TOKEN REFRESH ────────────────────────────────────────────
async function refreshToken() {
  log("🔄 Refreshing JWT token...");
  try {
    const res = await angelPost(
      "/rest/auth/angelbroking/jwt/v1/generateTokens",
      { refreshToken: state.refreshToken },
      state.jwtToken
    );

    if (res?.data?.jwtToken) {
      state.jwtToken     = res.data.jwtToken;
      state.feedToken    = res.data.feedToken;
      state.refreshToken = res.data.refreshToken;
      state.expiresAt    = Date.now() + 28 * 60 * 1000;
      log("✅ Token refreshed (hot-swap) | no WS disconnect");

      // hot-swap: update SmartStream auth without reconnecting
      if (state.wsConnected && state.wsClient) {
        hotSwapFeedToken();
      }
      scheduleTokenRefresh();
      broadcast({ type: "TOKEN_REFRESHED", feedToken: state.feedToken, expiresAt: state.expiresAt });
    } else {
      log("⚠️ Refresh failed, re-logging...");
      await doLogin();
      await connectSmartStream();
    }
  } catch (err) {
    log("❌ Refresh error:", err.message, "— re-logging...");
    await doLogin();
    await connectSmartStream();
  }
}

function scheduleTokenRefresh() {
  if (state.refreshTimer) clearTimeout(state.refreshTimer);
  const delay = Math.max((state.expiresAt - Date.now()) - 2 * 60 * 1000, 60_000);
  log(`⏰ Token refresh scheduled in ${Math.round(delay / 60000)}m`);
  state.refreshTimer = setTimeout(refreshToken, delay);
}

// hot-swap: re-subscribe on existing WS with new feedToken
function hotSwapFeedToken() {
  log("🔁 Hot-swapping feedToken on SmartStream...");
  // SmartStream doesn't support mid-session token swap natively
  // so reconnect WS silently
  reconnectSmartStream();
}

// ─── SMARTSTREAM WEBSOCKET ────────────────────────────────────
const MODE_LTP       = 1;
const MODE_QUOTE     = 2;
const MODE_SNAP_QUOT = 3;

function buildSubscribeMsg(tokens, mode = MODE_SNAP_QUOT) {
  // tokens: [{ exchangeType: 1, tokens: ["26000","26009"] }]
  return JSON.stringify({
    correlationID: "prochain_" + Date.now(),
    action: 1,
    params: { mode, tokenList: tokens },
  });
}

function buildUnsubMsg(tokens, mode = MODE_SNAP_QUOT) {
  return JSON.stringify({
    correlationID: "prochain_unsub_" + Date.now(),
    action: 0,
    params: { mode, tokenList: tokens },
  });
}

async function connectSmartStream() {
  if (!state.jwtToken || !state.feedToken) {
    log("⚠️ No tokens — login first");
    return;
  }

  if (state.wsClient) {
    try { state.wsClient.terminate(); } catch {}
    state.wsClient = null;
  }

  log("📡 Connecting SmartStream...");
  const ws = new WebSocket(SMARTSTREAM_URL, {
    headers: {
      Authorization: `Bearer ${state.jwtToken}`,
      "x-feed-token": state.feedToken,
      "x-client-code": CLIENT_ID,
      "x-feed-token": state.feedToken,
    },
  });

  state.wsClient = ws;

  ws.on("open", () => {
    state.wsConnected = true;
    log("✅ SmartStream connected");
    broadcast({ type: "WS_CONNECTED" });

    // re-subscribe all existing subscribers
    resubscribeAll();
  });

  ws.on("message", (data) => {
    state.tickCount++;
    try {
      const tick = parseBinaryTick(data);
      if (tick) broadcast({ type: "TICK", data: tick });
    } catch (err) {
      log("⚠️ Tick parse error:", err.message);
    }
  });

  ws.on("error", (err) => {
    log("❌ SmartStream error:", err.message);
    state.wsConnected = false;
  });

  ws.on("close", (code, reason) => {
    state.wsConnected = false;
    log(`🔌 SmartStream closed [${code}] ${reason}`);
    broadcast({ type: "WS_DISCONNECTED", code });
    scheduleReconnect();
  });

  ws.on("ping", () => ws.pong());
}

function scheduleReconnect(delay = 5000) {
  if (state.reconnectTimer) clearTimeout(state.reconnectTimer);
  state.reconnectTimer = setTimeout(() => {
    log("🔄 Reconnecting SmartStream...");
    connectSmartStream();
  }, delay);
}

function reconnectSmartStream() {
  if (state.wsClient) {
    try { state.wsClient.terminate(); } catch {}
  }
  connectSmartStream();
}

function resubscribeAll() {
  if (!state.wsConnected || !state.wsClient) return;
  const tokenMap = buildTokenMap();
  if (tokenMap.length > 0) {
    const msg = buildSubscribeMsg(tokenMap);
    state.wsClient.send(msg);
    log(`📋 Re-subscribed ${tokenMap.length} exchange groups`);
  }
}

function buildTokenMap() {
  // aggregate all subscriber tokens by exchangeType
  const map = {};
  for (const sub of state.subscribers) {
    for (const t of (sub.tokens || [])) {
      const ex = t.exchangeType || 1;
      if (!map[ex]) map[ex] = new Set();
      for (const tok of (t.tokens || [])) map[ex].add(tok);
    }
  }
  return Object.entries(map).map(([ex, toks]) => ({
    exchangeType: Number(ex),
    tokens: [...toks],
  }));
}

// ─── BINARY TICK PARSER ───────────────────────────────────────
// Angel One SmartStream binary protocol
function parseBinaryTick(buffer) {
  const buf = Buffer.isBuffer(buffer) ? buffer : Buffer.from(buffer);
  if (buf.length < 4) return null;

  const mode        = buf.readUInt8(0);
  const exchangeType= buf.readUInt8(1);
  const tokenLen    = buf.readUInt16BE(2);
  if (buf.length < 4 + tokenLen) return null;

  const token = buf.slice(4, 4 + tokenLen).toString("utf8").replace(/\0/g, "");
  let offset  = 4 + tokenLen;

  const tick = { mode, exchangeType, token, ts: Date.now() };

  const readI32 = () => { const v = buf.readInt32BE(offset); offset += 4; return v; };
  const readI64 = () => { const v = buf.readBigInt64BE(offset); offset += 8; return Number(v); };

  try {
    if (mode >= 1) {
      // LTP mode
      tick.seqNo         = readI64();
      tick.exchTs        = readI64();
      tick.ltp           = readI32() / 100;
    }
    if (mode >= 2) {
      // QUOTE mode
      tick.lastTradedQty = readI32();
      tick.avgTradedPrice= readI32() / 100;
      tick.volTradedToday= readI64();
      tick.totalBuyQty   = readI64();
      tick.totalSellQty  = readI64();
      tick.open          = readI32() / 100;
      tick.high          = readI32() / 100;
      tick.low           = readI32() / 100;
      tick.close         = readI32() / 100;
    }
    if (mode >= 3) {
      // SNAP_QUOTE — depth
      tick.depth = { buy: [], sell: [] };
      for (let i = 0; i < 5; i++) {
        tick.depth.buy.push({
          qty  : readI32(),
          price: readI32() / 100,
          orders: readI32(),
        });
      }
      for (let i = 0; i < 5; i++) {
        tick.depth.sell.push({
          qty  : readI32(),
          price: readI32() / 100,
          orders: readI32(),
        });
      }
      tick.lowerCircuit   = readI32() / 100;
      tick.upperCircuit   = readI32() / 100;
      tick.yearLow        = readI32() / 100;
      tick.yearHigh       = readI32() / 100;
    }
  } catch {
    // partial tick ok
  }

  return tick;
}

// ─── BROADCAST ────────────────────────────────────────────────
function broadcast(obj) {
  const msg = JSON.stringify(obj);
  for (const ws of state.wsClients) {
    if (ws.readyState === WebSocket.OPEN) {
      ws.send(msg);
    }
  }
}

// ─── HTTP HELPERS ─────────────────────────────────────────────
function sendJSON(res, code, obj) {
  const body = JSON.stringify(obj);
  res.writeHead(code, {
    "Content-Type" : "application/json",
    "Access-Control-Allow-Origin" : "*",
    "Access-Control-Allow-Headers": "Content-Type, Authorization, x-proxy-secret",
    "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
  });
  res.end(body);
}

function readBody(req) {
  return new Promise((resolve) => {
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
