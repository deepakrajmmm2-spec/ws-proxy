const express = require("express");
const http = require("http");
const WebSocket = require("ws");

const app = express();
const server = http.createServer(app);

const PORT = process.env.PORT || 3000;
const ANGEL_ONE_WS = "wss://smartapisocket.angelone.in/smart-stream";

// Health check
app.get("/", (req, res) => {
  res.send("✅ WebSocket Proxy Running");
});

// WebSocket proxy server
const wss = new WebSocket.Server({ server });

wss.on("connection", (clientWs, req) => {
  console.log("🔌 Client connected — opening Angel One WS...");

  const angelWs = new WebSocket(ANGEL_ONE_WS, {
    headers: {
      Origin: "https://smartapi.angelone.in",
    },
  });

  // Client → Angel One
  clientWs.on("message", (data) => {
    if (angelWs.readyState === WebSocket.OPEN) {
      angelWs.send(data);
    }
  });

  // Angel One → Client
  angelWs.on("message", (data) => {
    if (clientWs.readyState === WebSocket.OPEN) {
      clientWs.send(data);
    }
  });

  angelWs.on("open", () => {
    console.log("✅ Angel One WS connected");
  });

  angelWs.on("error", (err) => {
    console.error("❌ Angel One WS error:", err.message);
    clientWs.close();
  });

  angelWs.on("close", (code, reason) => {
    console.log("🔴 Angel One WS closed:", code, reason.toString());
    clientWs.close();
  });

  clientWs.on("close", () => {
    console.log("🔴 Client disconnected");
    angelWs.close();
  });

  clientWs.on("error", (err) => {
    console.error("❌ Client WS error:", err.message);
    angelWs.close();
  });
});

server.listen(PORT, () => {
  console.log(`🚀 Proxy server running on port ${PORT}`);
});
