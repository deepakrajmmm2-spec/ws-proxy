const express = require('express');
const axios = require('axios');
const cors = require('cors');
const WebSocket = require('ws');
const http = require('http');

const app = express();
const server = http.createServer(app);

// ─── CORS ───────────────────────────────────────────────────────────────────
app.use(cors({
  origin: '*',
  methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
  allowedHeaders: ['Content-Type', 'Authorization', 'X-UserType', 'X-SourceID',
                   'X-ClientLocalIP', 'X-ClientPublicIP', 'X-MACAddress',
                   'X-PrivateKey', 'apikey', 'jwttoken']
}));
app.use(express.json());

// ─── ANGEL ONE BASE URLs ─────────────────────────────────────────────────────
const ANGEL_BASE     = 'https://apiconnect.angelone.in';
const ANGEL_WS_URL   = 'wss://smartapisocket.angelone.in/smart-stream';

// ─── HEALTH CHECK ─────────────────────────────────────────────────────────────
app.get('/', (req, res) => {
  res.json({ status: 'ok', service: 'ProChain Proxy', time: new Date().toISOString() });
});

// ─── REST PROXY ───────────────────────────────────────────────────────────────
// Usage: POST /proxy  body: { url, method, headers, data }
app.post('/proxy', async (req, res) => {
  const { url, method = 'GET', headers = {}, data } = req.body;

  if (!url) return res.status(400).json({ error: 'url required' });

  // Only allow Angel One endpoints
  if (!url.startsWith(ANGEL_BASE)) {
    return res.status(403).json({ error: 'Only Angel One API calls allowed' });
  }

  try {
    const response = await axios({
      url,
      method,
      headers: {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        ...headers
      },
      data,
      timeout: 15000
    });
    res.json(response.data);
  } catch (err) {
    const status = err.response?.status || 500;
    const body   = err.response?.data  || { error: err.message };
    res.status(status).json(body);
  }
});

// ─── WEBSOCKET BRIDGE ─────────────────────────────────────────────────────────
// Client connects to ws://YOUR_RAILWAY_URL/ws?token=JWT&feedToken=FEED&clientCode=CODE
const wss = new WebSocket.Server({ server, path: '/ws' });

wss.on('connection', (clientWs, req) => {
  const params     = new URLSearchParams(req.url.replace('/ws?', ''));
  const token      = params.get('token');
  const feedToken  = params.get('feedToken');
  const clientCode = params.get('clientCode');

  if (!token || !feedToken || !clientCode) {
    clientWs.close(4001, 'Missing token/feedToken/clientCode');
    return;
  }

  console.log(`[WS] Client connected: ${clientCode}`);

  // Connect to Angel One SmartStream
  const angelWs = new WebSocket(ANGEL_WS_URL, {
    headers: {
      Authorization: `Bearer ${token}`,
      'x-feed-token': feedToken,
      'x-client-code': clientCode
    }
  });

  angelWs.on('open', () => {
    console.log(`[WS] Angel One connected for ${clientCode}`);
    clientWs.send(JSON.stringify({ type: 'connected', message: 'Angel One WS bridge ready' }));
  });

  // Forward Angel → Client
  angelWs.on('message', (data) => {
    if (clientWs.readyState === WebSocket.OPEN) clientWs.send(data);
  });

  // Forward Client → Angel
  clientWs.on('message', (data) => {
    if (angelWs.readyState === WebSocket.OPEN) angelWs.send(data);
  });

  angelWs.on('error', (err) => {
    console.error('[WS] Angel error:', err.message);
    clientWs.send(JSON.stringify({ type: 'error', message: err.message }));
  });

  angelWs.on('close', (code, reason) => {
    console.log(`[WS] Angel closed: ${code}`);
    if (clientWs.readyState === WebSocket.OPEN) clientWs.close(code, reason);
  });

  clientWs.on('close', () => {
    console.log(`[WS] Client disconnected: ${clientCode}`);
    if (angelWs.readyState === WebSocket.OPEN) angelWs.close();
  });

  clientWs.on('error', (err) => {
    console.error('[WS] Client error:', err.message);
    if (angelWs.readyState === WebSocket.OPEN) angelWs.close();
  });
});

// ─── START ────────────────────────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`ProChain Proxy running on port ${PORT}`);
});
