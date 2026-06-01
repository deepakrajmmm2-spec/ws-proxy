// server.js — ProChain Railway Backend v3.0
// HTTP REST proxy + WebSocket proxy (SmartAPI Angel One)
// npm install express http-proxy-middleware ws http-proxy-agent https-proxy-agent

const express = require("express");
const { createProxyMiddleware } = require("http-proxy-middleware");
const http = require("http");
const WebSocket = require("ws");

const app = express();
const server = http.createServer(app);

// ── CORS ─────────────────────────────────────────────────────────────
app.use((req, res, next) => {
  res.header("Access-Control-Allow-Origin", "*");
  res.header("Access-Control-Allow-Headers", "*");
  res.header("Access-Control-Allow-Methods", "GET,POST,PUT,DELETE,OPTIONS");
  if (req.method === "OPTIONS") return res.sendStatus(200);
  next();
});

// ── Health check ─────────────────────────────────────────────────────
app.get("/health", (req, res) => res.json({ status: "ok", ts: Date.now() }));

// ── Angel One REST proxy ──────────────────────────────────────────────
app.use("/angel", createProxyMiddleware({
  target: "https://apiconnect.angelone.in",
  changeOrigin: true,
  pathRewrite: { "^/angel": "" },
  on: {
    error: (err, req, res) => {
      console.error("[ANGEL REST]", err.message);
      res.status(502).json({ error: "Angel REST proxy error", detail: err.message });
    }
  }
}));

// ── Dhan REST proxy ───────────────────────────────────────────────────
app.use("/dhan", createProxyMiddleware({
  target: "https://api.dhan.co",
  changeOrigin: true,
  pathRewrite: { "^/dhan": "" },
  on: {
    error: (err, req, res) => {
      console.error("[DHAN REST]", err.message);
      res.status(502).json({ error: "Dhan REST proxy error", detail: err.message });
    }
  }
}));

// ── NSE REST proxy ────────────────────────────────────────────────────
app.use("/nse", createProxyMiddleware({
  target: "https://www.nseindia.com",
  changeOrigin: true,
  pathRewrite: { "^/nse": "" },
  headers: {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json"
  },
  on: {
    error: (err, req, res) => {
      console.error("[NSE REST]", err.message);
      res.status(502).json({ error: "NSE REST proxy error", detail: err.message });
    }
  }
}));

// ── WebSocket Proxy — Angel One SmartAPI ─────────────────────────────
// Frontend connects: wss://your-railway-app.up.railway.app/ws
// Backend forwards to: wss://smartapiws.angelone.in/
const ANGEL_WS_URL = "wss://smartapiws.angelone.in/";

const wss = new WebSocket.Server({ noServer: true });

wss.on("connection", (clientWs) => {
  console.log("[WS] Client connected — opening upstream to Angel SmartAPI");

  const upstream = new WebSocket(ANGEL_WS_URL);
  upstream.binaryType = "arraybuffer";

  // CLIENT → ANGEL
  clientWs.on("message", (data) => {
    if (upstream.readyState === WebSocket.OPEN) {
      upstream.send(data);
    }
  });

  // ANGEL → CLIENT
  upstream.on("message", (data) => {
    if (clientWs.readyState === WebSocket.OPEN) {
      clientWs.send(data);
    }
  });

  // Upstream errors
  upstream.on("error", (err) => {
    console.error("[WS upstream error]", err.message);
    clientWs.close(1011, "upstream_error");
  });

  upstream.on("close", (code, reason) => {
    console.log(`[WS upstream closed] ${code} ${reason}`);
    if (clientWs.readyState === WebSocket.OPEN) {
      clientWs.close(code, reason);
    }
  });

  // Client disconnect
  clientWs.on("close", (code, reason) => {
    console.log(`[WS client disconnected] ${code} ${reason}`);
    if (upstream.readyState !== WebSocket.CLOSED) {
      upstream.close();
    }
  });

  clientWs.on("error", (err) => {
    console.error("[WS client error]", err.message);
    if (upstream.readyState !== WebSocket.CLOSED) upstream.close();
  });
});

// ── HTTP Upgrade → WebSocket server ──────────────────────────────────
server.on("upgrade", (req, socket, head) => {
  if (req.url === "/ws") {
    wss.handleUpgrade(req, socket, head, (ws) => {
      wss.emit("connection", ws, req);
    });
  } else {
    socket.destroy();
  }
});

// ── Start ─────────────────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`[ProChain Backend] Running on port ${PORT}`);
  console.log(`[REST] /angel /dhan /nse proxy active`);
  console.log(`[WS] /ws → ${ANGEL_WS_URL}`);
});
