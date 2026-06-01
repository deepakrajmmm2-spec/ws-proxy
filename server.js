require("dotenv").config();

const express    = require("express");
const cors       = require("cors");
const http       = require("http");
const WebSocket  = require("ws");
const fetch      = require("node-fetch");

const PORT         = parseInt(process.env.PORT) || 3000;
const PROXY_SECRET = process.env.PROXY_SECRET || "";
const ANGEL_WS_URL = process.env.ANGEL_WS_URL || "wss://smartstream.angelbroking.com/ws/v2";
const ANGEL_BASE   = "https://apiconnect.angelbroking.com";
const ANGEL_DATA   = "https://margincalculator.angelbroking.com";
const DHAN_BASE    = "https://api.dhan.co";
const NSE_BASE     = "https://www.nseindia.com";

const app    = express();
const server = http.createServer(app);

app.use(cors({ origin: "*" }));
app.use(express.json({ limit: "2mb" }));

const ts   = () => new Date().toISOString();
const log  = (...a) => console.log (`[${ts()}]`, ...a);
const warn = (...a) => console.warn(`[${ts()}]`, ...a);
const err  = (...a) => console.error(`[${ts()}]`, ...a);

// ── Secret check ─────────────────────────────────────────────────────────────
function checkSecret(req, res, next) {
  if (!PROXY_SECRET) return next();
  const sent = req.headers["x-proxy-secret"] || req.headers["x-proxysecret"] || "";
  if (sent !== PROXY_SECRET) return res.status(401).json({ error: "Unauthorized" });
  next();
}

// ── Health ────────────────────────────────────────────────────────────────────
app.get("/",       (_, res) => res.json({ status: "ok", service: "prochain-proxy", ts: ts() }));
app.get("/health", (_, res) => res.json({ status: "ok", ts: ts(), wsClients: wss.clients.size }));

// ── Hop-by-hop headers ────────────────────────────────────────────────────────
const HOP = ["host","connection","transfer-encoding","upgrade","keep-alive",
             "proxy-authorization","proxy-authenticate","te","trailer"];

function buildHeaders(req) {
  const h = {};
  for (const [k, v] of Object.entries(req.headers)) {
    if (!HOP.includes(k.toLowerCase()) && k.toLowerCase() !== "x-proxy-secret") h[k] = v;
  }
  return h;
}

// ── Generic proxy helper ──────────────────────────────────────────────────────
async function proxyTo(targetUrl, req, res) {
  const method = req.method;
  const fwdHeaders = buildHeaders(req);
  if (["POST","PUT","PATCH"].includes(method) && !fwdHeaders["content-type"])
    fwdHeaders["content-type"] = "application/json";
  try {
    const opts = { method, headers: fwdHeaders, signal: AbortSignal.timeout(15000) };
    if (["POST","PUT","PATCH"].includes(method)) opts.body = JSON.stringify(req.body);
    log(`→ ${method} ${req.path}  ↦  ${targetUrl}`);
    const up = await fetch(targetUrl, opts);
    const ct = up.headers.get("content-type") || "";
    res.status(up.status);
    up.headers.forEach((v, k) => { if (!HOP.includes(k.toLowerCase())) res.setHeader(k, v); });
    if (ct.includes("json")) return res.json(await up.json());
    return res.send(await up.buffer());
  } catch (e) {
    err(`Proxy error ${method} ${req.path}:`, e.message);
    return res.status(502).json({ error: "Proxy upstream error", detail: e.message });
  }
}

// ─────────────────────────────────────────────────────────────────────────────
//  ANGEL ONE ROUTES
// ─────────────────────────────────────────────────────────────────────────────
const ANGEL_MAP = {
  "POST /angel/login":            `${ANGEL_BASE}/rest/auth/angelbroking/user/v1/loginByPassword`,
  "POST /angel/logout":           `${ANGEL_BASE}/rest/secure/angelbroking/user/v1/logout`,
  "POST /angel/refresh":          `${ANGEL_BASE}/rest/auth/angelbroking/jwt/v1/generateTokens`,
  "GET /angel/profile":           `${ANGEL_BASE}/rest/secure/angelbroking/user/v1/getProfile`,
  "GET /angel/user/profile":      `${ANGEL_BASE}/rest/secure/angelbroking/user/v1/getProfile`,
  "GET /angel/user/getfunds":     `${ANGEL_BASE}/rest/secure/angelbroking/user/v1/getRMS`,
  "GET /angel/margin":            `${ANGEL_BASE}/rest/secure/angelbroking/user/v1/getRMS`,
  "POST /angel/order/place":      `${ANGEL_BASE}/rest/secure/angelbroking/order/v1/placeOrder`,
  "POST /angel/order/modify":     `${ANGEL_BASE}/rest/secure/angelbroking/order/v1/modifyOrder`,
  "POST /angel/order/cancel":     `${ANGEL_BASE}/rest/secure/angelbroking/order/v1/cancelOrder`,
  "GET /angel/order/book":        `${ANGEL_BASE}/rest/secure/angelbroking/order/v1/getOrderBook`,
  "GET /angel/trade/book":        `${ANGEL_BASE}/rest/secure/angelbroking/order/v1/getTradeBook`,
  "GET /angel/position":          `${ANGEL_BASE}/rest/secure/angelbroking/order/v1/getPosition`,
  "POST /angel/position/convert": `${ANGEL_BASE}/rest/secure/angelbroking/order/v1/convertPosition`,
  "GET /angel/holding":           `${ANGEL_BASE}/rest/secure/angelbroking/portfolio/v1/getHolding`,
  "POST /angel/quote":            `${ANGEL_BASE}/rest/secure/angelbroking/market/v1/quote/`,
  "POST /angel/historical":       `${ANGEL_BASE}/rest/secure/angelbroking/historical/v1/getCandleData`,
  "GET /angel/expiry":            `${ANGEL_BASE}/rest/secure/angelbroking/market/v1/expiry`,
  "POST /angel/optionchain":      `${ANGEL_BASE}/rest/secure/angelbroking/market/v1/optionchain`,
  "POST /angel/search/scrip":     `${ANGEL_BASE}/rest/secure/angelbroking/order/v1/searchScrip`,
  "POST /angel/gtt/create":       `${ANGEL_BASE}/gtt-service/rest/secure/angelbroking/gtt/v1/createRule`,
  "POST /angel/gtt/modify":       `${ANGEL_BASE}/gtt-service/rest/secure/angelbroking/gtt/v1/modifyRule`,
  "POST /angel/gtt/cancel":       `${ANGEL_BASE}/gtt-service/rest/secure/angelbroking/gtt/v1/cancelRule`,
  "GET /angel/gtt/list":          `${ANGEL_BASE}/gtt-service/rest/secure/angelbroking/gtt/v1/ruleList`,
};

function resolveAngel(method, path) {
  const key = `${method} ${path}`;
  if (ANGEL_MAP[key]) return ANGEL_MAP[key];
  if (method === "GET" && path.match(/^\/angel\/order\/history\//))
    return `${ANGEL_BASE}/rest/secure/angelbroking/order/v1/details/${path.split("/").pop()}`;
  if (method === "GET" && path.match(/^\/angel\/order\/status\//))
    return `${ANGEL_BASE}/rest/secure/angelbroking/order/v1/details/${path.split("/").pop()}`;
  if (method === "GET" && path.match(/^\/angel\/gtt\/details\//))
    return `${ANGEL_BASE}/gtt-service/rest/secure/angelbroking/gtt/v1/ruleDetails/${path.split("/").pop()}`;
  if (method === "GET" && path.match(/^\/angel\/scripmaster\//))
    return `${ANGEL_DATA}/OpenAPI_LTP/ltp.php?exch=${path.split("/").pop().toUpperCase()}`;
  return null;
}

app.all("/angel/*", checkSecret, (req, res) => {
  const url = resolveAngel(req.method, req.path);
  if (!url) return res.status(404).json({ error: `Unknown route: ${req.method} ${req.path}` });
  proxyTo(url, req, res);
});

app.all("/angelbroking/*", checkSecret, (req, res) => {
  const url = resolveAngel(req.method, req.path.replace("/angelbroking", ""));
  if (!url) return res.status(404).json({ error: `Unknown route: ${req.method} ${req.path}` });
  proxyTo(url, req, res);
});

// ─────────────────────────────────────────────────────────────────────────────
//  DHAN ROUTES
//  ProChain calls: /orders, /quote/:indexKey
// ─────────────────────────────────────────────────────────────────────────────
app.post("/orders", checkSecret, (req, res) => {
  proxyTo(`${DHAN_BASE}/v2/orders`, req, res);
});

app.get("/quote/:indexKey", checkSecret, (req, res) => {
  // Dhan quote API — returns LTP for index
  const symbol = req.params.indexKey;
  proxyTo(`${DHAN_BASE}/v2/marketfeed/quote?symbol=${symbol}`, req, res);
});

// ─────────────────────────────────────────────────────────────────────────────
//  NSE ROUTES
//  ProChain calls: /index/:indexKey
// ─────────────────────────────────────────────────────────────────────────────

// NSE index map — ProChain indexKey → NSE API index name
const NSE_INDEX_MAP = {
  "NIFTY":       "NIFTY 50",
  "BANKNIFTY":   "NIFTY BANK",
  "FINNIFTY":    "NIFTY FIN SERVICE",
  "MIDCPNIFTY":  "NIFTY MID SELECT",
  "SENSEX":      "SENSEX",
  "BANKEX":      "BANKEX",
};

app.get("/index/:indexKey", checkSecret, async (req, res) => {
  const key      = req.params.indexKey.toUpperCase();
  const nseIndex = NSE_INDEX_MAP[key] || key;
  try {
    // NSE needs cookies — first hit homepage to get cookies
    const cookieRes = await fetch("https://www.nseindia.com", {
      headers: {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "text/html",
      },
      signal: AbortSignal.timeout(8000),
    });
    const cookies = cookieRes.headers.get("set-cookie") || "";

    const dataRes = await fetch(
      `https://www.nseindia.com/api/allIndices`,
      {
        headers: {
          "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
          "Accept": "application/json",
          "Referer": "https://www.nseindia.com",
          "Cookie": cookies,
        },
        signal: AbortSignal.timeout(8000),
      }
    );
    const data = await dataRes.json();
    const found = data.data?.find(i => i.index === nseIndex || i.indexSymbol === key);
    if (found) {
      return res.json({ last: found.last, ltp: found.last, change: found.variation, pChange: found.percentChange });
    }
    return res.status(404).json({ error: `Index ${key} not found in NSE response` });
  } catch (e) {
    err(`NSE proxy error for ${key}:`, e.message);
    return res.status(502).json({ error: "NSE upstream error", detail: e.message });
  }
});

// ─────────────────────────────────────────────────────────────────────────────
//  WEBSOCKET BRIDGE — ProChain ↔ Angel One SmartStream
// ─────────────────────────────────────────────────────────────────────────────
const wss = new WebSocket.Server({ server, path: "/ws" });

wss.on("connection", (clientWs, req) => {
  const ip = req.socket.remoteAddress;
  log(`[WS] Client connected: ${ip}  (total: ${wss.clients.size})`);

  if (PROXY_SECRET) {
    const url = new URL(req.url, "http://localhost");
    if (url.searchParams.get("secret") !== PROXY_SECRET) {
      warn(`[WS] Unauthorized ${ip}`);
      clientWs.close(4001, "Unauthorized");
      return;
    }
  }

  let angelWs = null, destroyed = false, pingTimer = null, reconnTimer = null, reconnCount = 0;

  function connectAngel() {
    if (destroyed) return;
    if (reconnCount >= 10) { clientWs.close(1011, "Max reconnects"); return; }
    log(`[WS] Upstream connect attempt ${reconnCount + 1}`);
    angelWs = new WebSocket(ANGEL_WS_URL, {
      headers: { "Origin": "https://smartapi.angelbroking.com", "User-Agent": "ProChain/1.0" },
      rejectUnauthorized: false,
    });
    angelWs.binaryType = "nodebuffer";
    angelWs.on("open", () => { reconnCount = 0; log(`[WS] Angel open for ${ip}`); });
    angelWs.on("message", (data, isBinary) => {
      if (clientWs.readyState === WebSocket.OPEN)
        try { clientWs.send(data, { binary: isBinary }); } catch(_) {}
    });
    angelWs.on("error", (e) => err(`[WS] Angel error: ${e.message}`));
    angelWs.on("close", (code) => {
      clearInterval(pingTimer);
      log(`[WS] Angel closed ${code}`);
      if (!destroyed && clientWs.readyState === WebSocket.OPEN) {
        reconnCount++;
        reconnTimer = setTimeout(connectAngel, Math.min(1000 * 2 ** reconnCount, 30000));
      }
    });
    clearInterval(pingTimer);
    pingTimer = setInterval(() => {
      if (angelWs?.readyState === WebSocket.OPEN) try { angelWs.ping(); } catch(_) {}
    }, 25000);
  }

  clientWs.on("message", (data, isBinary) => {
    if (angelWs?.readyState === WebSocket.OPEN)
      try { angelWs.send(data, { binary: isBinary }); } catch(_) {}
  });
  clientWs.on("close", (code) => {
    log(`[WS] Client ${ip} closed: ${code}`);
    destroyed = true;
    clearInterval(pingTimer); clearTimeout(reconnTimer);
    try { angelWs?.close(); } catch(_) {}
  });
  clientWs.on("error", (e) => err(`[WS] Client error: ${e.message}`));

  connectAngel();
});

// ── Start ─────────────────────────────────────────────────────────────────────
server.listen(PORT, () => {
  log(`ProChain Proxy on port ${PORT}`);
  log(`Angel REST: /angel/*`);
  log(`Dhan REST:  /orders  /quote/:key`);
  log(`NSE REST:   /index/:key`);
  log(`WS bridge:  /ws → ${ANGEL_WS_URL}`);
  if (!PROXY_SECRET) log(`⚠ ENV vars not set — open proxy (no secret)`);
});

process.on("SIGTERM", () => { server.close(() => process.exit(0)); wss.close(); });
process.on("SIGINT",  () => process.exit(0));
