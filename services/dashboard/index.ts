import express, { Request, Response } from 'express';
import http from 'http';
import { WebSocketServer, WebSocket } from 'ws';
import { createClient } from 'redis';
import { log, retry } from '../shared-utils';

const PORT = process.env.PORT || 3005;
const REDIS_URL = process.env.REDIS_URL || 'redis://redis:6379';

const app = express();
app.use(express.json());

const server = http.createServer(app);
const wss = new WebSocketServer({ noServer: true });

const redisClient = createClient({ url: REDIS_URL });
const subscriberClient = createClient({ url: REDIS_URL });

// Serve dynamic premium HTML UI directly at GET /
app.get('/', (req: Request, res: Response) => {
  res.send(`
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>SagaMesh - Distributed Order Saga Dashboard</title>
  <link href="https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;600;800&display=swap" rel="stylesheet">
  <style>
    :root {
      --bg-dark: #0f172a;
      --panel-bg: rgba(30, 41, 59, 0.7);
      --border-color: rgba(255, 255, 255, 0.08);
      --text-main: #f8fafc;
      --text-muted: #94a3b8;
      --accent-primary: #6366f1;
      --accent-hover: #4f46e5;
      
      --status-completed: #10b981;
      --status-pending: #f59e0b;
      --status-failed: #ef4444;
      --status-compensating: #ec4899;
    }

    * {
      box-sizing: border-box;
      margin: 0;
      padding: 0;
    }

    body {
      background-color: var(--bg-dark);
      background-image: 
        radial-gradient(at 0% 0%, rgba(99, 102, 241, 0.1) 0px, transparent 50%),
        radial-gradient(at 100% 100%, rgba(236, 72, 153, 0.1) 0px, transparent 50%);
      color: var(--text-main);
      font-family: 'Outfit', sans-serif;
      min-height: 100vh;
      display: flex;
      flex-direction: column;
    }

    header {
      padding: 24px 40px;
      border-bottom: 1px solid var(--border-color);
      display: flex;
      justify-content: space-between;
      align-items: center;
      backdrop-filter: blur(12px);
    }

    header h1 {
      font-size: 24px;
      font-weight: 800;
      background: linear-gradient(135deg, #a5b4fc, #818cf8, #ec4899);
      -webkit-background-clip: text;
      -webkit-text-fill-color: transparent;
      letter-spacing: -0.5px;
    }

    .ws-badge {
      font-size: 13px;
      font-weight: 600;
      padding: 6px 14px;
      border-radius: 9999px;
      background: rgba(255, 255, 255, 0.05);
      border: 1px solid var(--border-color);
      display: flex;
      align-items: center;
      gap: 8px;
    }

    .ws-indicator {
      width: 8px;
      height: 8px;
      border-radius: 50%;
      background: #ef4444;
      box-shadow: 0 0 8px #ef4444;
    }

    .ws-indicator.connected {
      background: #10b981;
      box-shadow: 0 0 8px #10b981;
    }

    main {
      flex: 1;
      padding: 40px;
      max-width: 1400px;
      width: 100%;
      margin: 0 auto;
      display: grid;
      grid-template-columns: 320px 1fr;
      gap: 32px;
    }

    .controls-panel {
      background: var(--panel-bg);
      border: 1px solid var(--border-color);
      border-radius: 20px;
      padding: 28px;
      height: fit-content;
      backdrop-filter: blur(16px);
      box-shadow: 0 20px 40px -15px rgba(0, 0, 0, 0.3);
    }

    .controls-panel h2 {
      font-size: 18px;
      font-weight: 600;
      margin-bottom: 24px;
      color: var(--text-main);
    }

    .control-group {
      margin-bottom: 24px;
    }

    .control-label {
      font-size: 13px;
      color: var(--text-muted);
      text-transform: uppercase;
      letter-spacing: 0.5px;
      display: block;
      margin-bottom: 8px;
    }

    .failure-rate-display {
      font-size: 32px;
      font-weight: 800;
      color: var(--accent-primary);
      margin-bottom: 16px;
    }

    .btn {
      width: 100%;
      padding: 12px 20px;
      border-radius: 12px;
      font-size: 14px;
      font-weight: 600;
      cursor: pointer;
      border: none;
      transition: all 0.2s ease;
      margin-bottom: 12px;
    }

    .btn-danger {
      background: #ef4444;
      color: white;
    }

    .btn-danger:hover {
      background: #dc2626;
      box-shadow: 0 0 16px rgba(239, 68, 68, 0.4);
    }

    .btn-secondary {
      background: rgba(255, 255, 255, 0.05);
      border: 1px solid var(--border-color);
      color: var(--text-main);
    }

    .btn-secondary:hover {
      background: rgba(255, 255, 255, 0.1);
    }

    .dashboard-panel {
      display: flex;
      flex-direction: column;
      gap: 24px;
    }

    .dashboard-header {
      display: flex;
      justify-content: space-between;
      align-items: center;
    }

    .dashboard-header h2 {
      font-size: 22px;
      font-weight: 600;
    }

    .table-container {
      background: var(--panel-bg);
      border: 1px solid var(--border-color);
      border-radius: 20px;
      overflow: hidden;
      backdrop-filter: blur(16px);
      box-shadow: 0 20px 40px -15px rgba(0, 0, 0, 0.3);
    }

    table {
      width: 100%;
      border-collapse: collapse;
      text-align: left;
    }

    th {
      background: rgba(255, 255, 255, 0.02);
      border-bottom: 1px solid var(--border-color);
      padding: 18px 24px;
      font-size: 13px;
      font-weight: 600;
      color: var(--text-muted);
      text-transform: uppercase;
      letter-spacing: 0.5px;
    }

    td {
      padding: 20px 24px;
      border-bottom: 1px solid var(--border-color);
      font-size: 14px;
      color: var(--text-main);
    }

    tr:last-child td {
      border-bottom: none;
    }

    .badge {
      display: inline-block;
      padding: 6px 12px;
      border-radius: 8px;
      font-size: 12px;
      font-weight: 600;
      text-transform: uppercase;
    }

    .badge-PENDING {
      background: rgba(245, 158, 11, 0.15);
      color: var(--status-pending);
    }

    .badge-COMPLETED {
      background: rgba(16, 185, 129, 0.15);
      color: var(--status-completed);
    }

    .badge-COMPENSATING {
      background: rgba(236, 72, 153, 0.15);
      color: var(--status-compensating);
    }

    .badge-FAILED {
      background: rgba(239, 68, 68, 0.15);
      color: var(--status-failed);
    }

    .history-list {
      display: flex;
      flex-direction: column;
      gap: 6px;
      max-height: 120px;
      overflow-y: auto;
    }

    .history-item {
      font-size: 11px;
      background: rgba(255, 255, 255, 0.03);
      padding: 4px 8px;
      border-radius: 4px;
      display: flex;
      justify-content: space-between;
      align-items: center;
      border-left: 2px solid var(--accent-primary);
    }

    .history-item.status-SUCCESS {
      border-left-color: var(--status-completed);
    }

    .history-item.status-FAILURE {
      border-left-color: var(--status-failed);
    }

    .history-item.status-COMPENSATING {
      border-left-color: var(--status-compensating);
    }

    .empty-state {
      padding: 60px;
      text-align: center;
      color: var(--text-muted);
      font-size: 16px;
    }
  </style>
</head>
<body>

  <header>
    <h1>SagaMesh</h1>
    <div class="ws-badge">
      <div id="wsIndicator" class="ws-indicator"></div>
      <span id="wsStatus">Connecting WebSocket...</span>
    </div>
  </header>

  <main>
    <div class="controls-panel">
      <h2>Saga Simulation</h2>
      
      <div class="control-group">
        <span class="control-label">Current Failure Rate</span>
        <div id="failureRateDisplay" class="failure-rate-display">0.0</div>
      </div>

      <button id="btnFail" class="btn btn-danger">Trigger 100% Payment Failure</button>
      <button id="btnReset" class="btn btn-secondary">Reset Failure Rate</button>
    </div>

    <div class="dashboard-panel">
      <div class="dashboard-header">
        <h2>Active Order Sagas</h2>
      </div>

      <div class="table-container">
        <table>
          <thead>
            <tr>
              <th style="width: 25%">Order ID</th>
              <th style="width: 15%">Status</th>
              <th style="width: 60%">Event Execution History</th>
            </tr>
          </thead>
          <tbody id="sagasTableBody">
            <tr id="emptyRow">
              <td colspan="3" class="empty-state">No active order sagas tracked. Create an order to see it flow live here.</td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>
  </main>

  <script>
    const sagas = {};

    function updateWebSocketStatus(connected) {
      const indicator = document.getElementById('wsIndicator');
      const statusText = document.getElementById('wsStatus');
      if (connected) {
        indicator.classList.add('connected');
        statusText.textContent = 'WS Connected';
      } else {
        indicator.classList.remove('connected');
        statusText.textContent = 'WS Disconnected. Reconnecting...';
      }
    }

    function renderSagas() {
      const tbody = document.getElementById('sagasTableBody');
      const keys = Object.keys(sagas).sort((a, b) => {
        // Sort by latest event timestamp
        const tA = sagas[a].history[sagas[a].history.length - 1]?.timestamp || '';
        const tB = sagas[b].history[sagas[b].history.length - 1]?.timestamp || '';
        return tB.localeCompare(tA);
      });

      if (keys.length === 0) {
        tbody.innerHTML = \`<tr id="emptyRow"><td colspan="3" class="empty-state">No active order sagas tracked.</td></tr>\`;
        return;
      }

      tbody.innerHTML = '';
      keys.forEach(id => {
        const saga = sagas[id];
        const row = document.createElement('tr');
        
        const historyHtml = saga.history.map(h => \`
          <div class="history-item status-\${h.status}">
            <span><strong>\${h.service.toUpperCase()}</strong>: \${h.event}</span>
            <span style="color: var(--text-muted); margin-left: 10px;">\${new Date(h.timestamp).toLocaleTimeString()}</span>
          </div>
        \`).join('');

        row.innerHTML = \`
          <td style="font-family: monospace; font-size: 13px;">\${id}</td>
          <td><span class="badge badge-\${saga.status}">\${saga.status}</span></td>
          <td>
            <div class="history-list">
              \${historyHtml}
            </div>
          </td>
        \`;
        tbody.appendChild(row);
      });
    }

    function connectWS() {
      const ws = new WebSocket('ws://' + window.location.host + '/ws/sagas');
      
      ws.onopen = () => {
        updateWebSocketStatus(true);
      };

      ws.onmessage = (event) => {
        try {
          const sagaUpdate = JSON.parse(event.data);
          if (sagaUpdate && sagaUpdate.orderId) {
            sagas[sagaUpdate.orderId] = sagaUpdate;
            renderSagas();
          }
        } catch (e) {
          console.error("WS parse error:", e);
        }
      };

      ws.onclose = () => {
        updateWebSocketStatus(false);
        setTimeout(connectWS, 2000);
      };

      ws.onerror = () => {
        ws.close();
      };
    }

    async function setFailureRate(rate) {
      try {
        const res = await fetch('/api/simulate/failure', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ service: 'payment', failureRate: rate })
        });
        const data = await res.json();
        if (res.ok) {
          document.getElementById('failureRateDisplay').textContent = data.failureRate.toFixed(1);
        } else {
          alert("Error: " + data.error);
        }
      } catch (e) {
        alert("Failed to connect to backend: " + e.message);
      }
    }

    async function loadInitialSagas() {
      try {
        const res = await fetch('/api/sagas');
        if (res.ok) {
          const data = await res.json();
          data.forEach(saga => {
            sagas[saga.orderId] = saga;
          });
          renderSagas();
        }
      } catch (e) {
        console.error("Failed to load initial sagas:", e);
      }
    }

    document.getElementById('btnFail').onclick = () => setFailureRate(1.0);
    document.getElementById('btnReset').onclick = () => setFailureRate(0.0);

    // Initial load
    connectWS();
    loadInitialSagas();
  </script>
</body>
</html>
  `);
});

// GET /api/sagas - fetch all active sagas
app.get('/api/sagas', async (req: Request, res: Response) => {
  try {
    const keys = await redisClient.keys('saga:*');
    const list = [];
    for (const key of keys) {
      const data = await redisClient.hGetAll(key);
      if (data && data.status) {
        list.push({
          orderId: key.replace('saga:', ''),
          status: data.status,
          history: JSON.parse(data.history || '[]'),
        });
      }
    }
    return res.status(200).json(list);
  } catch (err: any) {
    log({
      level: 'error',
      service: 'dashboard',
      message: `Failed to fetch all sagas: ${err.message}`,
      timestamp: new Date().toISOString(),
    });
    return res.status(500).json({ error: 'Internal server error' });
  }
});

// POST /api/simulate/failure (Req 9)
app.post('/api/simulate/failure', async (req: Request, res: Response) => {
  const { service, failureRate } = req.body;

  if (service !== 'payment') {
    return res.status(400).json({ error: "Only 'payment' service is supported for now." });
  }

  const rate = parseFloat(failureRate);
  if (isNaN(rate) || rate < 0.0 || rate > 1.0) {
    return res.status(400).json({ error: 'failureRate must be a number between 0.0 and 1.0.' });
  }

  try {
    await redisClient.set('payment:failureRate', rate.toString());
    log({
      level: 'info',
      service: 'dashboard',
      message: `Set payment failure rate simulation to ${rate}`,
      timestamp: new Date().toISOString(),
    });
    return res.status(200).json({
      service: 'payment',
      failureRate: rate,
      updated: true,
    });
  } catch (err: any) {
    log({
      level: 'error',
      service: 'dashboard',
      message: `Failed to update simulation in Redis: ${err.message}`,
      timestamp: new Date().toISOString(),
    });
    return res.status(500).json({ error: 'Failed to write setting to cache.' });
  }
});

// GET /api/sagas/:orderId (Req 10)
app.get('/api/sagas/:orderId', async (req: Request, res: Response) => {
  const { orderId } = req.params;
  const sagaKey = `saga:${orderId}`;

  try {
    const data = await redisClient.hGetAll(sagaKey);
    if (!data || Object.keys(data).length === 0) {
      return res.status(404).json({ error: 'Saga not found' });
    }

    return res.status(200).json({
      orderId,
      status: data.status,
      history: JSON.parse(data.history || '[]'),
    });
  } catch (err: any) {
    log({
      level: 'error',
      service: 'dashboard',
      message: `Failed to fetch saga ${orderId}: ${err.message}`,
      timestamp: new Date().toISOString(),
      orderId,
    });
    return res.status(500).json({ error: 'Internal server error' });
  }
});

// WebSocket upgrading (Req 11)
server.on('upgrade', (request, socket, head) => {
  const pathname = new URL(request.url || '', `http://${request.headers.host}`).pathname;

  if (pathname === '/ws/sagas') {
    wss.handleUpgrade(request, socket, head, (ws) => {
      wss.emit('connection', ws, request);
    });
  } else {
    socket.destroy();
  }
});

wss.on('connection', (ws: WebSocket) => {
  log({
    level: 'info',
    service: 'dashboard',
    message: 'WebSocket client connected to /ws/sagas',
    timestamp: new Date().toISOString(),
  });

  ws.on('error', (err) => {
    log({
      level: 'warn',
      service: 'dashboard',
      message: `WebSocket connection error: ${err.message}`,
      timestamp: new Date().toISOString(),
    });
  });
});

async function start() {
  // 1. Redis Connection Retry
  await retry(async () => {
    await redisClient.connect();
    await subscriberClient.connect();
  }, 'dashboard', 'Redis connection');

  // Subscribe to Redis pubsub channel 'saga-updates' (Req 11)
  await subscriberClient.subscribe('saga-updates', (message) => {
    // Broadcast message to all WebSocket clients
    wss.clients.forEach((client) => {
      if (client.readyState === WebSocket.OPEN) {
        client.send(message);
      }
    });
  });

  server.listen(PORT, () => {
    log({
      level: 'info',
      service: 'dashboard',
      message: `Dashboard service running on port ${PORT}`,
      timestamp: new Date().toISOString(),
    });
  });
}

start().catch((err) => {
  log({
    level: 'error',
    service: 'dashboard',
    message: `Bootstrap failed: ${err.message}`,
    timestamp: new Date().toISOString(),
  });
  process.exit(1);
});
