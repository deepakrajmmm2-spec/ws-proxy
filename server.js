require("dotenv").config();

const express    = require("express");
const cors       = require("cors");
const http       = require("http");
const WebSocket  = require("ws");
const fetch      = require("node-fetch");
const https      = require("https");

const PORT          = parseInt(process.env.PORT) || 3000;
const PROXY_SECRET  = process.env.PROXY_SECRET || "";
const ANGEL_WS_URL  = process.env.ANGEL_WS_URL || "wss://smartstream.angelbroking.com/ws/v2";
const ANGEL_BASE    = "https://apiconnect.angelbroking.com";
const ANGEL_DATA    = "https://margincalculator.angelbroking.com";

const app    = express();
const server = http.createServer(app);

app.use(cors({ origin: "*" }));
app.use(express.json({ limit: "2mb" }));

const ts   = ()     => new Date().toISOString();
const log  = (...a) => console.log (`[${ts()}]`, ...a);
const warn = (...a) => console.warn(`[${ts()}]`, ...a);
const err  = (...a) => console.error(`[${ts()}]`, ...a);

function checkSecret(req, res, next) {
  if (!PROXY_SECRET) return next();
  const sent = req.headers["x-proxy-secret"] || req.headers["x-proxysecret"] || "";
  if (sent !== PROXY_SECRET) return res.status(401).json({ error: "Unauthorized" });
  next();
}

app.get("/",       (_, res) => res.json({ status: "ok", service: "prochain-proxy", ts: ts() }));
app.get("/health", (_, res) => res.json({ status: "ok", ts: ts(), wsClients: wss.clients.size }));

const ANGEL_ROUTE_MAP = {
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

function resolveAngelUrl(method, path) {
  const key = `${method} ${path}`;
  if (ANGEL_ROUTE_MAP[key]) return ANGEL_ROUTE_MAP[key];
  if (method === "GET" && path.match(/^\/angel\/order\/history\//))
    return `${ANGEL_BASE}/rest/secure/angelbroking/order/v1/details/${path.split("/").pop()}`;
  if (method === "GET" && path.match(/^\/angel\/order\/status\//))
    return `${ANGEL_BASE}/rest/secure/angelbroking/order/v1/details/${path.split("/").pop()}`;
  if (method === "GET" && path.match(/^\/angel\/gtt\/details\//))
    return `${ANGEL_BASE}/gtt-service/rest/secure/angelbroking/gtt/v1/ruleDetails/${path.split("/").pop()}`;
  if (method === "GET" && path.match(/^\/angel\/scripmaster\//)) {
    const exchange = path.split("/").pop().toUpperCase();
    return `${ANGEL_DATA}/OpenAPI_LTP/ltp.php?exch=${exchange}`;
  }
  return null;
}

const HOP_BY_HOP = ["host","connection","transfer-encoding","upgrade","keep-alive","proxy-authorization","proxy-authenticate","te","trailer"];

async function proxyRest(req, res) {
  const method   = req.method;
  const path     = req.path;
  const angelUrl = resolveAngelUrl(method, path);
  if (!angelUrl) {
    warn(`Unknown route: ${method} ${path}`);
    return res.status(404).json({ error: `Unknown proxy route: ${method} ${path}` });
  }
  const fwdHeaders = {};
  for (const [k, v] of Object.entries(req.headers)) {
    if (!HOP_BY_HOP.includes(k.toLowerCase()) && k.toLowerCase() !== "x-proxy-secret") {
      fwdHeaders[k] = v;
    }
  }
  if (method === "POST" && !fwdHeaders["content-type"]) fwdHeaders["content-type"] = "application/json";
  try {
    const fetchOpts = { method, headers: fwdHeaders, signal: AbortSignal.timeout(15000) };
    if (["POST","PUT","PATCH"].includes(method)) fetchOpts.body = JSON.stringify(req.body);
    log(`→ ${method} ${path}  ↦  ${angelUrl}`);
    const upstream = await fetch(angelUrl, fetchOpts);
    const ct = upstream.headers.get("content-type") || "";
    res.status(upstream.status);
    upstream.headers.forEach((v, k) => { if (!HOP_BY_HOP.includes(k.toLowerCase())) res.setHeader(k, v); });
    if (ct.includes("json")) return res.json(await upstream.json());
    return res.send(await upstream.buffer());
  } catch (e) {
    err(`REST proxy error ${method} ${path}:`, e.message);
    return res.status(502).json({ error: "Proxy upstream error", detail: e.message });
  }
}

app.all("/angel/*", checkSecret, proxyRest);
app.all("/angelbroking/*", checkSecret, (req, res) => proxyRest(req, res));

// ── WebSocket Bridge ──────────────────────────────────────────────────────────
const wss = new WebSocket.Server({ server, path: "/ws" });

wss.on("connection", (clientWs, req) => {
  const ip = req.socket.remoteAddress;
  log(`[WS] Client connected: ${ip}  (total: ${wss.clients.size})`);

  if (PROXY_SECRET) {
    const url = new URL(req.url, `http://localhost`);
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
    log(`[WS] Connecting upstream attempt ${reconnCount + 1}`);
    angelWs = new WebSocket(ANGEL_WS_URL, {
      headers: { "Origin": "https://smartapi.angelbroking.com", "User-Agent": "ProChain/1.0" },
      rejectUnauthorized: false,
    });
    angelWs.binaryType = "nodebuffer";
    angelWs.on("open", () => { reconnCount = 0; log(`[WS] Angel upstream open for ${ip}`); });
    angelWs.on("message", (data, isBinary) => {
      if (clientWs.readyState === WebSocket.OPEN) {
        try { clientWs.send(data, { binary: isBinary }); } catch(_) {}
      }
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
    if (angelWs?.readyState === WebSocket.OPEN) {
      try { angelWs.send(data, { binary: isBinary }); } catch(_) {}
    }
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

server.listen(PORT, () => {
  log(`ProChain Proxy on port ${PORT}`);
  log(`REST /angel/* → Angel One API`);
  log(`WS   /ws      → ${ANGEL_WS_URL}`);
});

process.on("SIGTERM", () => { server.close(() => process.exit(0)); wss.close(); });
process.on("SIGINT",  () => process.exit(0));
