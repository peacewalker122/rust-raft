use std::{collections::HashMap, net::SocketAddr, sync::Arc};

use rust_raft::{
    config::RaftConfig,
    node::{
        node::RaftNode,
        rpc::{NodeRpcService, proto::raft_rpc_server::RaftRpcServer},
        scheduler::NodeScheduler,
    },
    storage::storage::PersistentStore,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::{RwLock, mpsc},
};
use tonic::transport::Server;

#[derive(Debug, Clone, PartialEq, Eq)]
enum KvCommand {
    Set { key: String, value: Vec<u8> },
    Delete { key: String },
}

#[derive(Debug)]
enum KvCommandError {
    InvalidFormat,
}

#[derive(Debug, Default)]
struct KvStore {
    data: HashMap<String, Vec<u8>>,
}

impl KvStore {
    fn apply(&mut self, command: KvCommand) {
        match command {
            KvCommand::Set { key, value } => {
                self.data.insert(key, value);
            }
            KvCommand::Delete { key } => {
                self.data.remove(&key);
            }
        }
    }

    fn snapshot(&self) -> Vec<(String, String)> {
        self.data
            .iter()
            .map(|(key, value)| (key.clone(), String::from_utf8_lossy(value).to_string()))
            .collect()
    }
}

fn encode_command(command: &KvCommand) -> Vec<u8> {
    match command {
        KvCommand::Set { key, value } => {
            let mut data = Vec::with_capacity(1 + 4 + key.len() + 4 + value.len());
            data.push(b'S');
            data.extend_from_slice(&(key.len() as u32).to_be_bytes());
            data.extend_from_slice(key.as_bytes());
            data.extend_from_slice(&(value.len() as u32).to_be_bytes());
            data.extend_from_slice(value);
            data
        }
        KvCommand::Delete { key } => {
            let mut data = Vec::with_capacity(1 + 4 + key.len());
            data.push(b'D');
            data.extend_from_slice(&(key.len() as u32).to_be_bytes());
            data.extend_from_slice(key.as_bytes());
            data
        }
    }
}

fn decode_command(data: &[u8]) -> Result<KvCommand, KvCommandError> {
    if data.is_empty() {
        return Err(KvCommandError::InvalidFormat);
    }

    match data[0] {
        b'S' => decode_set_command(data),
        b'D' => decode_delete_command(data),
        _ => Err(KvCommandError::InvalidFormat),
    }
}

fn decode_set_command(data: &[u8]) -> Result<KvCommand, KvCommandError> {
    let mut cursor = 1;
    let key_len = read_u32(data, &mut cursor)? as usize;
    let key = read_string(data, &mut cursor, key_len)?;
    let value_len = read_u32(data, &mut cursor)? as usize;
    let value = read_bytes(data, &mut cursor, value_len)?;

    Ok(KvCommand::Set { key, value })
}

fn decode_delete_command(data: &[u8]) -> Result<KvCommand, KvCommandError> {
    let mut cursor = 1;
    let key_len = read_u32(data, &mut cursor)? as usize;
    let key = read_string(data, &mut cursor, key_len)?;

    Ok(KvCommand::Delete { key })
}

fn read_u32(data: &[u8], cursor: &mut usize) -> Result<u32, KvCommandError> {
    if data.len() < *cursor + 4 {
        return Err(KvCommandError::InvalidFormat);
    }

    let value = u32::from_be_bytes(
        data[*cursor..*cursor + 4]
            .try_into()
            .map_err(|_| KvCommandError::InvalidFormat)?,
    );
    *cursor += 4;
    Ok(value)
}

fn read_string(data: &[u8], cursor: &mut usize, length: usize) -> Result<String, KvCommandError> {
    let bytes = read_bytes(data, cursor, length)?;
    String::from_utf8(bytes).map_err(|_| KvCommandError::InvalidFormat)
}

fn read_bytes(data: &[u8], cursor: &mut usize, length: usize) -> Result<Vec<u8>, KvCommandError> {
    if data.len() < *cursor + length {
        return Err(KvCommandError::InvalidFormat);
    }

    let bytes = data[*cursor..*cursor + length].to_vec();
    *cursor += length;
    Ok(bytes)
}

async fn apply_events_to_store(mut receiver: mpsc::Receiver<Vec<u8>>, store: Arc<RwLock<KvStore>>) {
    while let Some(payload) = receiver.recv().await {
        if let Ok(command) = decode_command(&payload) {
            let mut store = store.write().await;
            store.apply(command);
        }
    }
}

async fn run_http_server(
    addr: SocketAddr,
    node: Arc<RwLock<RaftNode>>,
    store: Arc<RwLock<KvStore>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let listener = TcpListener::bind(addr).await?;

    loop {
        let (mut socket, _) = listener.accept().await?;
        let node = node.clone();
        let store = store.clone();

        tokio::spawn(async move {
            if let Err(err) = handle_connection(&mut socket, node, store).await {
                let _ = socket.shutdown().await;
                eprintln!("kv http connection error: {err}");
            }
        });
    }
}

async fn handle_connection(
    socket: &mut tokio::net::TcpStream,
    node: Arc<RwLock<RaftNode>>,
    store: Arc<RwLock<KvStore>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let request = read_http_request(socket).await?;
    let response = route_request(request, node, store).await;
    socket.write_all(response.as_bytes()).await?;
    Ok(())
}

async fn read_http_request(
    socket: &mut tokio::net::TcpStream,
) -> Result<HttpRequest, Box<dyn std::error::Error + Send + Sync>> {
    let mut buffer = Vec::with_capacity(1024);
    let mut header_end = None;

    loop {
        let mut temp = [0u8; 1024];
        let read = socket.read(&mut temp).await?;
        if read == 0 {
            break;
        }
        buffer.extend_from_slice(&temp[..read]);
        if let Some(pos) = find_header_end(&buffer) {
            header_end = Some(pos);
            break;
        }
        if buffer.len() > 16 * 1024 {
            break;
        }
    }

    let header_end = header_end.ok_or("invalid http request")?;
    let (header_bytes, body_bytes) = buffer.split_at(header_end);
    let headers_text = String::from_utf8_lossy(header_bytes);
    let request_line = headers_text.lines().next().ok_or("invalid request")?;
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or("");
    let path = parts.next().unwrap_or("");
    let content_length = parse_content_length(&headers_text);

    let mut body = body_bytes.to_vec();
    let remaining = content_length.saturating_sub(body.len());
    if remaining > 0 {
        let mut extra = vec![0u8; remaining];
        socket.read_exact(&mut extra).await?;
        body.extend_from_slice(&extra);
    }

    Ok(HttpRequest {
        method: method.to_string(),
        path: path.to_string(),
        body,
    })
}

fn find_header_end(buffer: &[u8]) -> Option<usize> {
    buffer
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|pos| pos + 4)
}

fn parse_content_length(headers: &str) -> usize {
    headers
        .lines()
        .find_map(|line| {
            let lower = line.to_ascii_lowercase();
            if lower.starts_with("content-length") {
                line.split(':').nth(1).map(|value| value.trim().to_string())
            } else {
                None
            }
        })
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(0)
}

async fn route_request(
    request: HttpRequest,
    node: Arc<RwLock<RaftNode>>,
    store: Arc<RwLock<KvStore>>,
) -> String {
    match (request.method.as_str(), request.path.as_str()) {
        ("GET", "/") => html_response(HTML_UI),
        ("GET", "/kv") => kv_snapshot_response(&store).await,
        ("GET", "/status") => status_response(&node).await,
        ("POST", "/kv/set") => apply_set(&request.body, &node).await,
        ("POST", "/kv/delete") => apply_delete(&request.body, &node).await,
        _ => not_found_response(),
    }
}

async fn kv_snapshot_response(store: &Arc<RwLock<KvStore>>) -> String {
    let snapshot = store.read().await.snapshot();
    let mut lines = Vec::new();
    for (key, value) in snapshot {
        let line = format!("{}={}", percent_encode(&key), percent_encode(&value));
        lines.push(line);
    }
    text_response(lines.join("\n"))
}

async fn status_response(node: &Arc<RwLock<RaftNode>>) -> String {
    let node = node.read().await;
    let leader_id = node.get_leader_id().unwrap_or("-");
    let payload = format!(
        "id={};term={};commit={};leader={}",
        node.get_id(),
        node.get_term(),
        node.get_commit_index(),
        leader_id
    );
    text_response(payload)
}

async fn apply_set(body: &[u8], node: &Arc<RwLock<RaftNode>>) -> String {
    let params = parse_form_body(body);
    let key = match params.get("key") {
        Some(value) if !value.is_empty() => value.clone(),
        _ => return bad_request("missing key"),
    };
    let value = match params.get("value") {
        Some(value) => value.clone(),
        None => return bad_request("missing value"),
    };

    let command = KvCommand::Set {
        key,
        value: value.into_bytes(),
    };

    apply_command(command, node).await
}

async fn apply_delete(body: &[u8], node: &Arc<RwLock<RaftNode>>) -> String {
    let params = parse_form_body(body);
    let key = match params.get("key") {
        Some(value) if !value.is_empty() => value.clone(),
        _ => return bad_request("missing key"),
    };

    let command = KvCommand::Delete { key };
    apply_command(command, node).await
}

async fn apply_command(command: KvCommand, node: &Arc<RwLock<RaftNode>>) -> String {
    let mut node = node.write().await;
    if !node.is_leader() {
        return conflict_response("not leader");
    }

    let payload = encode_command(&command);
    match node.push_log(payload, None).await {
        Ok(()) => text_response("ok".to_string()),
        Err(err) => server_error(&format!("raft error: {err}")),
    }
}

fn parse_form_body(body: &[u8]) -> HashMap<String, String> {
    let raw = String::from_utf8_lossy(body);
    raw.split('&')
        .filter_map(|pair| pair.split_once('='))
        .map(|(key, value)| (percent_decode(key), percent_decode(value)))
        .collect()
}

fn percent_encode(value: &str) -> String {
    value
        .bytes()
        .flat_map(|byte| match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                vec![byte as char]
            }
            _ => format!("%{:02X}", byte).chars().collect(),
        })
        .collect()
}

fn percent_decode(value: &str) -> String {
    let mut bytes = Vec::new();
    let mut chars = value.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '%' {
            let high = chars.next();
            let low = chars.next();
            if let (Some(high), Some(low)) = (high, low) {
                if let Ok(byte) = u8::from_str_radix(&format!("{high}{low}"), 16) {
                    bytes.push(byte);
                    continue;
                }
            }
        }
        bytes.extend_from_slice(ch.to_string().as_bytes());
    }

    String::from_utf8_lossy(&bytes).to_string()
}

fn html_response(body: &str) -> String {
    http_response(200, "OK", "text/html; charset=utf-8", body)
}

fn text_response(body: String) -> String {
    http_response(200, "OK", "text/plain; charset=utf-8", &body)
}

fn bad_request(message: &str) -> String {
    http_response(400, "Bad Request", "text/plain; charset=utf-8", message)
}

fn conflict_response(message: &str) -> String {
    http_response(409, "Conflict", "text/plain; charset=utf-8", message)
}

fn not_found_response() -> String {
    http_response(404, "Not Found", "text/plain; charset=utf-8", "not found")
}

fn server_error(message: &str) -> String {
    http_response(
        500,
        "Internal Server Error",
        "text/plain; charset=utf-8",
        message,
    )
}

fn http_response(status: u16, status_text: &str, content_type: &str, body: &str) -> String {
    format!(
        "HTTP/1.1 {status} {status_text}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )
}

struct HttpRequest {
    method: String,
    path: String,
    body: Vec<u8>,
}

const HTML_UI: &str = r#"<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Raft KV Forge</title>
  <link rel="preconnect" href="https://fonts.googleapis.com" />
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin />
  <link href="https://fonts.googleapis.com/css2?family=Syne:wght@600;700;800&family=Spectral:wght@300;400;500&display=swap" rel="stylesheet" />
  <style>
    :root {
      --ink: #0b0b0b;
      --fog: #f5f0e9;
      --acid: #c4ff00;
      --ember: #ff4d00;
      --steel: #1f1f1f;
      --grid: rgba(11, 11, 11, 0.06);
    }

    * {
      box-sizing: border-box;
    }

    body {
      margin: 0;
      min-height: 100vh;
      font-family: "Spectral", serif;
      color: var(--ink);
      background:
        linear-gradient(120deg, rgba(196, 255, 0, 0.08), transparent 50%),
        repeating-linear-gradient(0deg, transparent 0 36px, var(--grid) 36px 37px),
        repeating-linear-gradient(90deg, transparent 0 36px, var(--grid) 36px 37px),
        var(--fog);
    }

    header {
      padding: 40px 6vw 24px;
      border-bottom: 2px solid var(--ink);
      display: flex;
      justify-content: space-between;
      align-items: flex-end;
      gap: 32px;
    }

    h1 {
      font-family: "Syne", sans-serif;
      font-size: clamp(2.8rem, 4vw, 4.6rem);
      letter-spacing: -0.04em;
      margin: 0;
      text-transform: uppercase;
    }

    .meta {
      font-size: 0.95rem;
      text-transform: uppercase;
      letter-spacing: 0.08em;
      border-left: 3px solid var(--ember);
      padding-left: 16px;
      max-width: 280px;
    }

    main {
      display: grid;
      grid-template-columns: minmax(280px, 1fr) minmax(320px, 1.2fr);
      gap: 32px;
      padding: 36px 6vw 60px;
    }

    .panel {
      background: white;
      border: 2px solid var(--ink);
      box-shadow: 8px 8px 0 var(--ink);
      padding: 24px;
      position: relative;
    }

    .panel::after {
      content: "";
      position: absolute;
      inset: 8px;
      border: 1px dashed rgba(11, 11, 11, 0.3);
      pointer-events: none;
    }

    .panel h2 {
      font-family: "Syne", sans-serif;
      font-size: 1.6rem;
      margin-top: 0;
      margin-bottom: 16px;
      text-transform: uppercase;
    }

    .status {
      display: flex;
      flex-direction: column;
      gap: 12px;
      font-size: 0.95rem;
    }

    .status span {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      font-family: "Syne", sans-serif;
      text-transform: uppercase;
      letter-spacing: 0.08em;
      font-size: 0.75rem;
    }

    .status strong {
      display: block;
      font-size: 1.2rem;
      font-family: "Spectral", serif;
    }

    .status .leader-row {
      position: relative;
    }

    .status .leader-row::before {
      content: "👑";
      position: absolute;
      left: -28px;
      font-size: 1.1rem;
      animation: crown-pulse 2s ease-in-out infinite;
    }

    .status .leader-indicator {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      background: var(--acid);
      color: var(--ink);
      padding: 4px 12px;
      border: 2px solid var(--ink);
      font-weight: bold;
      position: relative;
      overflow: hidden;
    }

    .status .leader-indicator::before {
      content: "";
      position: absolute;
      top: 0;
      left: -100%;
      width: 100%;
      height: 100%;
      background: linear-gradient(90deg, transparent, rgba(255,255,255,0.4), transparent);
      animation: leader-shine 2s ease-in-out infinite;
    }

    @keyframes crown-pulse {
      0%, 100% { transform: translateY(0) scale(1); }
      50% { transform: translateY(-3px) scale(1.1); }
    }

    @keyframes leader-shine {
      0% { left: -100%; }
      50%, 100% { left: 100%; }
    }

    .status .follower-indicator {
      color: var(--steel);
      font-style: italic;
      opacity: 0.6;
    }

    form {
      display: grid;
      gap: 12px;
    }

    label {
      font-size: 0.8rem;
      text-transform: uppercase;
      letter-spacing: 0.16em;
      font-family: "Syne", sans-serif;
    }

    input {
      padding: 12px 14px;
      font-size: 1rem;
      border: 2px solid var(--ink);
      background: #fff;
      font-family: "Spectral", serif;
    }

    button {
      border: 2px solid var(--ink);
      background: var(--acid);
      padding: 12px 18px;
      font-family: "Syne", sans-serif;
      text-transform: uppercase;
      letter-spacing: 0.08em;
      cursor: pointer;
      transition: transform 0.2s ease;
    }

    button:hover {
      transform: translate(-2px, -2px);
    }

    .danger {
      background: var(--ember);
      color: white;
    }

    .ledger {
      max-height: 380px;
      overflow: auto;
      border: 2px solid var(--ink);
      padding: 16px;
      background: #fdfbf7;
    }

    .entry {
      display: flex;
      justify-content: space-between;
      padding: 10px 0;
      border-bottom: 1px dashed rgba(11, 11, 11, 0.2);
      font-family: "Syne", sans-serif;
      text-transform: uppercase;
      letter-spacing: 0.08em;
      font-size: 0.8rem;
    }

    .entry span {
      font-family: "Spectral", serif;
      text-transform: none;
      letter-spacing: 0;
      font-size: 1rem;
    }

    .log {
      background: var(--steel);
      color: #fdfbf7;
      padding: 16px;
      font-family: "Syne", sans-serif;
      text-transform: uppercase;
      letter-spacing: 0.08em;
      min-height: 120px;
    }

    .log p {
      margin: 0 0 8px;
      font-size: 0.75rem;
      color: #c4ff00;
    }

    .log span {
      font-family: "Spectral", serif;
      text-transform: none;
      letter-spacing: 0;
      color: #fff;
    }

    @media (max-width: 900px) {
      main {
        grid-template-columns: 1fr;
      }
    }
  </style>
</head>
<body>
  <header>
    <div>
      <h1>Raft KV Forge</h1>
      <p>Black-box KV + black-box Raft. Pure log energy, zero coupling.</p>
    </div>
    <div class="meta">
      Brutalist console for demoing a replicated key-value ledger.<br />
      Everything here is committed, nothing is assumed.
    </div>
  </header>

  <main>
    <section class="panel">
      <h2>Node Status</h2>
      <div class="status" id="status">
        <div><span>ID</span><strong>loading</strong></div>
        <div><span>TERM</span><strong>loading</strong></div>
        <div><span>COMMIT</span><strong>loading</strong></div>
        <div><span>LEADER</span><strong>loading</strong></div>
      </div>
    </section>

    <section class="panel">
      <h2>Write</h2>
      <form id="set-form">
        <label for="set-key">Key</label>
        <input id="set-key" name="key" placeholder="signal" required />
        <label for="set-value">Value</label>
        <input id="set-value" name="value" placeholder="alpha" required />
        <button type="submit">Commit Set</button>
      </form>
      <div style="height:16px"></div>
      <form id="delete-form">
        <label for="delete-key">Key</label>
        <input id="delete-key" name="key" placeholder="signal" required />
        <button class="danger" type="submit">Commit Delete</button>
      </form>
    </section>

    <section class="panel">
      <h2>Ledger</h2>
      <div class="ledger" id="ledger"></div>
    </section>

    <section class="panel">
      <h2>Signal Log</h2>
      <div class="log" id="log"></div>
    </section>
  </main>

  <script>
    const statusEl = document.getElementById('status');
    const ledgerEl = document.getElementById('ledger');
    const logEl = document.getElementById('log');

    const appendLog = (label, message) => {
      const entry = document.createElement('p');
      const time = new Date().toLocaleTimeString();
      entry.innerHTML = `${label} <span>${time} · ${message}</span>`;
      logEl.prepend(entry);
    };

    const encodeForm = (data) =>
      Object.entries(data)
        .map(([key, value]) => `${encodeURIComponent(key)}=${encodeURIComponent(value)}`)
        .join('&');

    const refreshStatus = async () => {
      const res = await fetch('/status');
      const text = await res.text();
      const parts = Object.fromEntries(text.split(';').map(part => part.split('=')));
      const isLeader = parts.id === parts.leader;
      const leaderClass = isLeader ? 'leader-row' : '';
      const leaderValue = parts.leader && parts.leader !== '-'
        ? `<span class="leader-indicator">${parts.leader}</span>`
        : '<span class="follower-indicator">unknown</span>';
      statusEl.innerHTML = `
        <div><span>ID</span><strong>${parts.id || 'unknown'}</strong></div>
        <div><span>TERM</span><strong>${parts.term || '-'}</strong></div>
        <div><span>COMMIT</span><strong>${parts.commit || '-'}</strong></div>
        <div class="${leaderClass}"><span>LEADER</span><strong>${leaderValue}</strong></div>
      `;
    };

    const refreshLedger = async () => {
      const res = await fetch('/kv');
      const text = await res.text();
      if (!text.trim()) {
        ledgerEl.innerHTML = '<em>empty ledger</em>';
        return;
      }
      const rows = text.split('\n').filter(Boolean).map(line => line.split('='));
      ledgerEl.innerHTML = rows.map(([key, value]) => `
        <div class="entry">
          <div>${decodeURIComponent(key)}</div>
          <span>${decodeURIComponent(value || '')}</span>
        </div>
      `).join('');
    };

    document.getElementById('set-form').addEventListener('submit', async (event) => {
      event.preventDefault();
      const key = document.getElementById('set-key').value;
      const value = document.getElementById('set-value').value;
      const res = await fetch('/kv/set', {
        method: 'POST',
        headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
        body: encodeForm({ key, value }),
      });
      appendLog('SET', res.ok ? `${key} → ${value}` : 'rejected');
      await refreshLedger();
      await refreshStatus();
    });

    document.getElementById('delete-form').addEventListener('submit', async (event) => {
      event.preventDefault();
      const key = document.getElementById('delete-key').value;
      const res = await fetch('/kv/delete', {
        method: 'POST',
        headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
        body: encodeForm({ key }),
      });
      appendLog('DEL', res.ok ? key : 'rejected');
      await refreshLedger();
      await refreshStatus();
    });

    refreshStatus();
    refreshLedger();
    appendLog('READY', 'KV forge online');
  </script>
</body>
</html>"#;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize structured logging
    rust_raft::logging::init();

    let config = RaftConfig::from_env()?;

    let term_file = tokio::fs::File::options()
        .read(true)
        .write(true)
        .create(true)
        .open(&config.term_file_path)
        .await?;
    let log_file = tokio::fs::File::options()
        .read(true)
        .write(true)
        .create(true)
        .open(&config.log_file_path)
        .await?;

    let storage = Box::new(PersistentStore::new(log_file, term_file));
    let (event_sender, event_receiver) = tokio::sync::mpsc::channel(100);
    let shared_node = Arc::new(RwLock::new(RaftNode::new(
        config.node_id,
        config.peers,
        storage,
        event_sender.clone(),
    )));
    let (scheduler_tx, scheduler_rx) = tokio::sync::mpsc::channel(100);
    let mut scheduler = NodeScheduler::new(shared_node.clone(), scheduler_rx);

    let store = Arc::new(RwLock::new(KvStore::default()));
    tokio::spawn(apply_events_to_store(event_receiver, store.clone()));

    tokio::spawn(async move {
        scheduler.start().await;
    });

    let http_addr: SocketAddr = std::env::var("KV_HTTP_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:8080".to_string())
        .parse()?;
    tokio::spawn(run_http_server(
        http_addr,
        shared_node.clone(),
        store.clone(),
    ));

    println!(
        "Starting RAFT KV node with ID '{}' on '{}'",
        shared_node.read().await.get_id(),
        config.grpc_bind
    );
    println!("KV demo UI available at http://{http_addr}");

    Server::builder()
        .add_service(RaftRpcServer::new(NodeRpcService::new(
            shared_node,
            scheduler_tx,
            event_sender,
        )))
        .serve(config.grpc_bind)
        .await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_decode_set_round_trip() {
        let command = KvCommand::Set {
            key: "alpha".to_string(),
            value: b"beta".to_vec(),
        };

        let encoded = encode_command(&command);
        let decoded = decode_command(&encoded).expect("decode set command");

        assert_eq!(decoded, command);
    }

    #[test]
    fn encode_decode_delete_round_trip() {
        let command = KvCommand::Delete {
            key: "alpha".to_string(),
        };

        let encoded = encode_command(&command);
        let decoded = decode_command(&encoded).expect("decode delete command");

        assert_eq!(decoded, command);
    }
}
