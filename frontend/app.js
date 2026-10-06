// Network Telemetry & Automation Frontend Controller

let selectedRouter = 'Router1';
let routersData = [];
let activeTaskId = null;
let ws = null;

// Initialize when DOM ready
document.addEventListener('DOMContentLoaded', () => {
  initRouterSelector();
  initWebSocket();
  fetchRouters();
  fetchTasks();
  checkHealth();
  setInterval(checkHealth, 10000);
});

// Router selection buttons
function initRouterSelector() {
  const buttons = document.querySelectorAll('#router-selector .router-btn');
  buttons.forEach(btn => {
    btn.addEventListener('click', () => {
      buttons.forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      selectedRouter = btn.getAttribute('data-router');
      updateIpLabel();
    });
  });
}

function updateIpLabel() {
  const label = document.getElementById('selected-ip-label');
  if (selectedRouter === 'Router1') {
    label.textContent = '192.168.122.101 (Core)';
  } else if (selectedRouter === 'Router2') {
    label.textContent = '192.168.122.102 (Distribution)';
  } else {
    label.textContent = 'All Devices (101 & 102)';
  }
}

// Preset command setter
function setCommand(cmd) {
  document.getElementById('command-input').value = cmd;
}

// Health check for Kafka
async function checkHealth() {
  try {
    const res = await fetch('/api/health');
    const data = await res.json();
    const statusText = document.getElementById('kafka-status-text');
    const dot = document.getElementById('kafka-dot');
    const ping = document.getElementById('kafka-ping');

    if (data.kafka_connected) {
      statusText.textContent = 'Kafka Broker Active';
      dot.className = 'relative inline-flex rounded-full h-2 w-2 bg-emerald-500';
      ping.className = 'animate-ping absolute inline-flex h-full w-full rounded-full bg-emerald-400 opacity-75';
    } else {
      statusText.textContent = 'Kafka Reconnecting...';
      dot.className = 'relative inline-flex rounded-full h-2 w-2 bg-amber-500';
      ping.className = 'animate-ping absolute inline-flex h-full w-full rounded-full bg-amber-400 opacity-75';
    }
  } catch (err) {
    document.getElementById('kafka-status-text').textContent = 'Backend Offline';
  }
}

// Fetch Routers
async function fetchRouters() {
  try {
    const res = await fetch('/api/routers');
    routersData = await res.json();
  } catch (err) {
    console.error('Failed to load routers:', err);
  }
}

// Animate architecture cards
function triggerFlowAnimation() {
  const steps = ['flow-frontend', 'flow-backend', 'flow-kafka', 'flow-worker', 'flow-gns3'];
  steps.forEach((id, index) => {
    setTimeout(() => {
      steps.forEach(s => document.getElementById(s)?.classList.remove('pulsing'));
      document.getElementById(id)?.classList.add('pulsing', 'active');
    }, index * 400);
  });

  setTimeout(() => {
    steps.forEach(s => document.getElementById(s)?.classList.remove('pulsing'));
  }, steps.length * 400 + 800);
}

// Submit Task to Kafka
async function submitTask() {
  const cmdInput = document.getElementById('command-input').value.trim();
  if (!cmdInput) {
    alert('Please enter at least one command.');
    return;
  }

  const commands = cmdInput.split('\n').map(c => c.trim()).filter(Boolean);
  const submitBtn = document.getElementById('submit-btn');
  const loader = document.getElementById('terminal-loader');
  const termContent = document.getElementById('terminal-content');

  submitBtn.disabled = true;
  loader.classList.remove('hidden');
  termContent.innerHTML = `<span class="text-amber-400">// Dispatching task to Kafka topic 'router-tasks'...</span>\n<span class="text-slate-400">Target: ${selectedRouter} | Commands: [${commands.join(', ')}]</span>`;

  triggerFlowAnimation();

  try {
    const res = await fetch('/api/tasks', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        router_id: selectedRouter,
        commands: commands
      })
    });

    if (!res.ok) {
      throw new Error(`HTTP error ${res.status}`);
    }

    const data = await res.json();
    activeTaskId = data.task_id;
    document.getElementById('terminal-title').textContent = `Task [${data.task_id}] - Queued in Kafka`;
    
    // Poll fallback in case websocket is delayed
    pollTaskResult(data.task_id);
    fetchTasks();
  } catch (err) {
    termContent.innerHTML = `<span class="text-rose-400">Error submitting task: ${err.message}</span>`;
    loader.classList.add('hidden');
    submitBtn.disabled = false;
  }
}

// Poll fallback for task completion
async function pollTaskResult(taskId, attempts = 0) {
  if (attempts > 30) {
    document.getElementById('terminal-loader').classList.add('hidden');
    document.getElementById('submit-btn').disabled = false;
    return;
  }

  try {
    const res = await fetch(`/api/tasks/${taskId}`);
    if (res.ok) {
      const task = await res.json();
      if (task.status === 'SUCCESS' || task.status === 'FAILED') {
        renderTaskOutput(task);
        fetchTasks();
        return;
      }
    }
  } catch (e) {}

  setTimeout(() => pollTaskResult(taskId, attempts + 1), 1000);
}

// Render output in Terminal
function renderTaskOutput(task) {
  document.getElementById('terminal-loader').classList.add('hidden');
  document.getElementById('submit-btn').disabled = false;

  const termContent = document.getElementById('terminal-content');
  const title = document.getElementById('terminal-title');
  const timeBadge = document.getElementById('output-meta-time');
  const modeBadge = document.getElementById('output-meta-badge');

  title.textContent = `Console Output - Task [${task.task_id}] (${task.router_id})`;

  timeBadge.textContent = `${task.execution_time_ms || 0}ms`;
  timeBadge.classList.remove('hidden');

  modeBadge.classList.remove('hidden');
  if (task.is_mock) {
    modeBadge.textContent = 'MOCK SIMULATION';
    modeBadge.className = 'px-2 py-0.5 rounded text-[10px] font-semibold bg-purple-500/20 text-purple-300 border border-purple-500/30';
  } else {
    modeBadge.textContent = 'REAL GNS3 SSH';
    modeBadge.className = 'px-2 py-0.5 rounded text-[10px] font-semibold bg-emerald-500/20 text-emerald-300 border border-emerald-500/30';
  }

  termContent.textContent = task.output || '[No output returned]';
}

// Fetch list of recent tasks
async function fetchTasks() {
  try {
    const res = await fetch('/api/tasks');
    const tasks = await res.json();
    const container = document.getElementById('tasks-list');

    if (!tasks || tasks.length === 0) {
      container.innerHTML = `<div class="text-xs text-slate-500 text-center py-6">No commands dispatched yet.</div>`;
      return;
    }

    container.innerHTML = tasks.map(t => {
      const isSuccess = t.status === 'SUCCESS';
      const isQueued = t.status === 'QUEUED';
      const statusBadge = isQueued
        ? `<span class="px-1.5 py-0.5 rounded text-[9px] bg-amber-500/10 text-amber-400 border border-amber-500/20 font-mono">QUEUED</span>`
        : isSuccess
          ? `<span class="px-1.5 py-0.5 rounded text-[9px] bg-emerald-500/10 text-emerald-400 border border-emerald-500/20 font-mono">${t.is_mock ? 'MOCK' : 'SUCCESS'}</span>`
          : `<span class="px-1.5 py-0.5 rounded text-[9px] bg-rose-500/10 text-rose-400 border border-rose-500/20 font-mono">FAILED</span>`;

      return `
        <div onclick="selectTask('${t.task_id}')" class="p-2.5 bg-dark-950/60 hover:bg-slate-800/50 border border-slate-800 rounded-xl cursor-pointer transition flex items-center justify-between text-xs">
          <div class="space-y-0.5">
            <div class="font-mono text-slate-300 flex items-center gap-2">
              <span class="font-bold text-white">${t.router_id}</span>
              <span class="text-[11px] text-slate-500">#${t.task_id.slice(-6)}</span>
            </div>
            <div class="text-[11px] text-slate-400 truncate max-w-[200px]">
              ${(t.commands || []).join('; ')}
            </div>
          </div>
          <div class="text-right space-y-1">
            ${statusBadge}
            <div class="text-[10px] text-slate-500 font-mono">${t.execution_time_ms ? t.execution_time_ms + 'ms' : ''}</div>
          </div>
        </div>
      `;
    }).join('');
  } catch (err) {
    console.error('Failed to load tasks:', err);
  }
}

// Select task from list to view
async function selectTask(taskId) {
  try {
    const res = await fetch(`/api/tasks/${taskId}`);
    if (res.ok) {
      const task = await res.json();
      renderTaskOutput(task);
    }
  } catch (e) {
    console.error(e);
  }
}

// Clear terminal
function clearTerminal() {
  document.getElementById('terminal-content').textContent = '// Console cleared.';
  document.getElementById('terminal-title').textContent = 'Console Output (Ready)';
  document.getElementById('output-meta-time').classList.add('hidden');
  document.getElementById('output-meta-badge').classList.add('hidden');
}

// Copy output to clipboard
function copyOutput() {
  const text = document.getElementById('terminal-content').textContent;
  navigator.clipboard.writeText(text).then(() => {
    alert('Output copied to clipboard!');
  });
}

// Setup WebSocket
function initWebSocket() {
  const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
  const wsUrl = `${protocol}//${window.location.host}/ws`;

  try {
    ws = new WebSocket(wsUrl);

    ws.onmessage = (event) => {
      try {
        const msg = JSON.parse(event.data);
        if (msg.type === 'TASK_COMPLETED' && msg.data) {
          if (activeTaskId === msg.data.task_id) {
            renderTaskOutput(msg.data);
          }
          fetchTasks();
        } else if (msg.type === 'TASK_CREATED') {
          fetchTasks();
        }
      } catch (e) {
        console.error(e);
      }
    };

    ws.onclose = () => {
      setTimeout(initWebSocket, 4000);
    };
  } catch (err) {
    console.warn('WebSocket connection not available, falling back to polling.');
  }
}
