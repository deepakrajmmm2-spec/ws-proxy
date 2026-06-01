/**
 * ws-proxy — Angel One SmartStream WebSocket Relay
 * Railway pe deploy karo — ProChain ka /ws endpoint yahi handle karta hai
 *
 * Routes:
 *   GET  /health   → {"status":"ok"}
 *   GET  /ping     → {"pong":true}
 *   WS   /ws       → Angel One SmartStream relay (binary + text frames)
 *   GET  /api/*    → Angel One REST API proxy (CORS fix)
 *   POST /api/*    → Angel One REST API proxy (CORS fix)
 */

const http       = require("http");
const https      = require("https");
const WebSocket  = require("ws");
const url        = require("url");

// ── Config
const PORT         = process.env.PORT || 3000;
const PROXY_SECRET = process.env.PROXY_SECRET || "";
const ANGEL_WS_URL = "wss://smartapisocket.angelone.in/smart-stream";

// ── HTTP server
const server = http.createServer((req, res) => {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization, X-UserType, X-SourceID, X-ClientLocalIP, X-ClientPublicIP, X-MACAddress, X-PrivateKey, X-Proxy-Secret");

  if (req.method === "OPTIONS") { res.writeHead(204); res.end(); return; }

  const parsedUrl = url.parse(req.url, true);
  const pathname  = parsedUrl.pathname;

  if (pathname === "/health" || pathname === "/") {
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ status: "ok", ts: Date.now() }));
    return;
  }

  if (pathname === "/ping") {
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ pong: true, ts: Date.now() }));
    return;
  }

  if (pathname.startsWith("/api/")) {
    if (PROXY_SECRET) {
      const clientSecret = req.headers["x-proxy-secret"] || "";
      if (clientSecret !== PROXY_SECRET) {
        res.writeHead(403, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ error: "Forbidden" }));
        return;
      }
    }
    const angelPath = pathname.replace(/^\/api/, "");
    const options = {
      hostname: "apiconnect.angelone.in",
      path: angelPath + (parsedUrl.search || ""),
      method: req.method,
      headers: { ...req.headers, host: "apiconnect.angelone.in" },
    };
    delete options.headers["x-proxy-secret"];
    const proxyReq = https.request(options, (proxyRes) => {
      res.writeHead(proxyRes.statusCode, { ...proxyRes.headers, "access-control-allow-origin": "*" });
      proxyRes.pipe(res);
    });
    proxyReq.on("error", (e) => {
      res.writeHead(502, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ error: "Proxy error", detail: e.message }));
    });
    req.pipe(proxyReq);
    return;
  }

  res.writeHead(404, { "Content-Type": "application/json" });
  res.end(JSON.stringify({ error: "Not found", path: pathname }));
});

// ── WebSocket server
const wss = new WebSocket.Server({ noServer: true });

server.on("upgrade", (req, socket, head) => {
  const parsedUrl = url.parse(req.url, true);
  if (parsedUrl.pathname !== "/ws") {
    socket.write("HTTP/1.1 404 Not Found\r\n\r\n");
    socket.destroy();
    return;
  }
  if (PROXY_SECRET) {
    const qSecret = parsedUrl.query["secret"] || "";
    const hSecret = req.headers["x-proxy-secret"] || "";
    if (qSecret !== PROXY_SECRET && hSecret !== PROXY_SECRET) {
      socket.write("HTTP/1.1 403 Forbidden\r\n\r\n");
      socket.destroy();
      return;
    }
  }
  wss.handleUpgrade(req, socket, head, (clientWs) => {
    wss.emit("connection", clientWs, req);
  });
});

wss.on("connection", (clientWs, req) => {
  const clientIp = req.socket.remoteAddress || "unknown";
  console.log(`[WS] Client connected: ${clientIp}`);

  let angelWs   = null;
  let pingTimer = null;
  let destroyed = false;

  function connectToAngel() {
    angelWs = new WebSocket(ANGEL_WS_URL, {
      headers: { "Origin": "https://smartapi.angelone.in" }
    });

    angelWs.on("open", () => {
      console.log(`[WS] Angel One connected for: ${clientIp}`);
      pingTimer = setInterval(() => {
        if (angelWs.readyState === WebSocket.OPEN) angelWs.ping();
      }, 25000);
    });

    angelWs.on("message", (data, isBinary) => {
      if (clientWs.readyState === WebSocket.OPEN) clientWs.send(data, { binary: isBinary });
    });

    angelWs.on("ping", () => { try { angelWs.pong(); } catch(_) {} });

    angelWs.on("error", (err) => {
      console.error(`[WS] Angel error: ${err.message}`);
      if (clientWs.readyState === WebSocket.OPEN)
        clientWs.send(JSON.stringify({ type: "error", message: err.message }));
    });

    angelWs.on("close", (code, reason) => {
      console.log(`[WS] Angel disconnected: ${code}`);
      clearInterval(pingTimer);
      if (!destroyed && clientWs.readyState === WebSocket.OPEN) clientWs.close(code, reason);
    });
  }

  connectToAngel();

  clientWs.on("message", (data, isBinary) => {
    if (angelWs && angelWs.readyState === WebSocket.OPEN) angelWs.send(data, { binary: isBinary });
  });

  clientWs.on("ping", () => { try { clientWs.pong(); } catch(_) {} });

  clientWs.on("close", (code) => {
    console.log(`[WS] Client disconnected: ${clientIp} (${code})`);
    destroyed = true;
    clearInterval(pingTimer);
    if (angelWs) { try { angelWs.close(1000, "client_disconnected"); } catch(_) {} }
  });

  clientWs.on("error", (err) => { console.error(`[WS] Client error: ${err.message}`); });
});

// ── Start
server.listen(PORT, () => {
  console.log(`ws-proxy running on port ${PORT}`);
  console.log(`  WS: ws://localhost:${PORT}/ws`);
  console.log(`  Secret: ${PROXY_SECRET ? "SET ✓" : "NOT SET"}`);
});
