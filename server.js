const express = require("express");
const http = require("http");
const WebSocket = require("ws");

const app = express();
const server = http.createServer(app);

const PORT = process.env.PORT || 3000;
const ANGEL_ONE_WS = "wss://smartapisocket.angelone.in/smart-stream";

// CORS headers for all requests
app.use((req, res, next) => {
  res.header("Access-Control-Allow-Origin", "*");
  res.header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS");
  res.header("Access-Control-Allow-Headers", "*");
  if (req.method === "OPTIONS") return res.sendStatus(200);
  next();
});

app.use(express.json());

// Health check
app.get("/", (req, res) => res.json({ status: "ok", message: "✅ WebSocket Proxy Running" }));
app.get("/health", (req, res) => res.json({ status: "ok" }));

// Angel One REST proxy
app.all("/angel/*", async (req, res) => {
  const path = req.path.replace("/angel", "");
  const url = `https://apiconnect.angelone.in${path}`;
  try {
    const fetch = (await import("node-fetch")).default;
    const response = await fetch(url, {
      method: req.method,
      headers: { ...req.headers, host: "apiconnect.angelone.in" },
      body: ["GET", "HEAD"].includes(req.method) ? undefined : JSON.stringify(req.body),
    });
    const data = await response.text();
    res.status(response.status).send(data);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Dhan REST proxy
app.all("/dhan/*", async (req, res) => {
  const path = req.path.replace("/dhan", "");
  const url = `https://api.dhan.co${path}`;
  try {
    const fetch = (await import("node-fetch")).default;
    const response = await fetch(url, {
      method: req.method,
      headers: { ...req.headers, host: "api.dhan.co" },
      body: ["GET", "HEAD"].includes(req.method) ? undefined : JSON.stringify(req.body),
    });
    const data = await response.text();
    res.status(response.status).send(data);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// NSE Feed proxy
app.all("/nse/*", async (req, res) => {
  const path = req.path.replace("/nse", "");
  const url = `https://www.nseindia.com${path}`;
  try {
    const fetch = (await import("node-fetch")).default;
    const response = await fetch(url, {
      method: req.method,
      headers: { ...req.headers, host: "www.nseindia.com" },
      body: ["GET", "HEAD"].includes(req.method) ? undefined : JSON.stringify(req.body),
    });
    const data = await response.text();
    res.status(response.status).send(data);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// WebSocket proxy
const wss = new WebSocket.Server({ server });

wss.on("connection", (clientWs) => {
  console.log("🔌 Client connected — opening Angel One WS...");

  const angelWs = new WebSocket(ANGEL_ONE_WS, {
    headers: { Origin: "https://smartapi.angelone.in" },
  });

  clientWs.on("message", (data) => {
    if (angelWs.readyState === WebSocket.OPEN) angelWs.send(data);
  });

  angelWs.on("message", (data) => {
    if (clientWs.readyState === WebSocket.OPEN) clientWs.send(data);
  });

  angelWs.on("open", () => console.log("✅ Angel One WS connected"));
  angelWs.on("error", (err) => { console.error("❌ Angel One WS error:", err.message); clientWs.close(); });
  angelWs.on("close", () => clientWs.close());
  clientWs.on("close", () => angelWs.close());
  clientWs.on("error", () => angelWs.close());
});

server.listen(PORT, () => console.log(`🚀 Proxy running on port ${PORT}`));
