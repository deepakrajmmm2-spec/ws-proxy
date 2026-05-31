const express = require("express");
const http = require("http");
const WebSocket = require("ws");

const app = express();
const server = http.createServer(app);

const PORT = process.env.PORT || 3000;
const ANGEL_ONE_WS = "wss://smartapisocket.angelone.in/smart-stream";
const ANGEL_BASE = "https://apiconnect.angelone.in";
const DHAN_BASE = "https://api.dhan.co";
const NSE_BASE = "https://www.nseindia.com";

// Angel One path map
const ANGEL_PATH_MAP = {
  "/angel/login":        "/rest/auth/angelbroking/user/v1/loginByPassword",
  "/angel/refresh":      "/rest/auth/angelbroking/jwt/v1/refreshTokens",
  "/angel/profile":      "/rest/secure/angelbroking/user/v1/getProfile",
  "/angel/user/profile": "/rest/secure/angelbroking/user/v1/getProfile",
  "/angel/quote":        "/rest/secure/angelbroking/market/v1/quote/",
  "/angel/optionchain":  "/rest/secure/angelbroking/market/v1/optionchain",
  "/angel/expiry":       "/rest/secure/angelbroking/market/v1/expiry",
  "/angel/order/book":   "/rest/secure/angelbroking/order/v1/getOrderBook",
  "/angel/order/place":  "/rest/secure/angelbroking/order/v1/placeOrder",
  "/angel/order/modify": "/rest/secure/angelbroking/order/v1/modifyOrder",
  "/angel/order/cancel": "/rest/secure/angelbroking/order/v1/cancelOrder",
  "/angel/position":     "/rest/secure/angelbroking/order/v1/getPosition",
  "/angel/holding":      "/rest/secure/angelbroking/portfolio/v1/getHolding",
  "/angel/scripmaster":  "/rest/secure/angelbroking/market/v1/scripmaster",
};

// CORS
app.use((req, res, next) => {
  res.header("Access-Control-Allow-Origin", "*");
  res.header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS");
  res.header("Access-Control-Allow-Headers", "*");
  if (req.method === "OPTIONS") return res.sendStatus(200);
  next();
});

app.use(express.json());

app.get("/", (req, res) => res.json({ status: "ok", message: "✅ Proxy Running" }));
app.get("/health", (req, res) => res.json({ status: "ok" }));

async function proxyTo(targetUrl, req, res) {
  try {
    const fetch = (await import("node-fetch")).default;
    
    // Forward all Angel One required headers
    const headers = {
      "Content-Type": "application/json",
      "Accept": "application/json",
      "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
      "X-ClientLocalIP": req.headers["x-clientlocalip"] || req.headers["x-client-local-ip"] || "127.0.0.1",
      "X-ClientPublicIP": req.headers["x-clientpublicip"] || req.headers["x-client-public-ip"] || "127.0.0.1",
      "X-MACAddress": req.headers["x-macaddress"] || req.headers["x-mac-address"] || "00:00:00:00:00:00",
    };

    // Forward auth headers
    const authHeaders = ["authorization", "x-privatekey", "x-privateclientid", 
      "x-privateclientkey", "x-api-key", "x-usertype", "x-sourceid",
      "x-clientcode", "x-feedtoken"];
    for (const h of authHeaders) {
      if (req.headers[h]) headers[h] = req.headers[h];
    }

    console.log(`[PROXY] ${req.method} ${targetUrl}`);

    const response = await fetch(targetUrl, {
      method: req.method,
      headers,
      body: ["GET", "HEAD"].includes(req.method) ? undefined : JSON.stringify(req.body),
    });

    const data = await response.text();
    console.log(`[PROXY] Response ${response.status}: ${data.substring(0, 100)}`);
    
    const ct = response.headers.get("content-type") || "application/json";
    res.status(response.status).set("content-type", ct).send(data);
  } catch (err) {
    console.error(`[PROXY ERROR] ${err.message}`);
    res.status(500).json({ error: err.message });
  }
}

// Angel One routes
app.all("/angel*", (req, res) => {
  const mapped = ANGEL_PATH_MAP[req.path];
  if (mapped) return proxyTo(`${ANGEL_BASE}${mapped}`, req, res);
  // scripmaster with exchange param
  if (req.path.startsWith("/angel/scripmaster/")) {
    const exchange = req.path.split("/angel/scripmaster/")[1];
    return proxyTo(`${ANGEL_BASE}/rest/secure/angelbroking/market/v1/scripmaster?exchange=${exchange}`, req, res);
  }
  const path = req.path.replace(/^\/angel/, "") || "/";
  proxyTo(`${ANGEL_BASE}${path}`, req, res);
});

// Dhan routes
app.all("/dhan*", (req, res) => {
  const path = req.path.replace(/^\/dhan/, "") || "/";
  proxyTo(`${DHAN_BASE}${path}`, req, res);
});

// NSE routes
app.all("/nse*", (req, res) => {
  const path = req.path.replace(/^\/nse/, "") || "/";
  proxyTo(`${NSE_BASE}${path}`, req, res);
});

// WebSocket proxy
const wss = new WebSocket.Server({ server });
wss.on("connection", (clientWs) => {
  console.log("🔌 WS Client connected");
  const angelWs = new WebSocket(ANGEL_ONE_WS, {
    headers: { Origin: "https://smartapi.angelone.in" },
  });
  clientWs.on("message", (data) => { if (angelWs.readyState === WebSocket.OPEN) angelWs.send(data); });
  angelWs.on("message", (data) => { if (clientWs.readyState === WebSocket.OPEN) clientWs.send(data); });
  angelWs.on("open", () => console.log("✅ Angel One WS connected"));
  angelWs.on("error", (err) => { console.error("❌ WS:", err.message); try { clientWs.close(); } catch(_){} });
  angelWs.on("close", () => { try { clientWs.close(); } catch(_){} });
  clientWs.on("close", () => { try { angelWs.close(); } catch(_){} });
  clientWs.on("error", () => { try { angelWs.close(); } catch(_){} });
});

server.listen(PORT, () => console.log(`🚀 Proxy on port ${PORT}`));
