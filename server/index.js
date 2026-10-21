// Lethal Chat server: reads YouTube and TikTok live chat and passes it to the website.
// The website connects with: wss://<this server>/chat?youtube=@handle&tiktok=username
import http from 'node:http';
import { WebSocketServer } from 'ws';
import { TikTokLiveConnection, WebcastEvent, UserOfflineError } from 'tiktok-live-connector';
import { Innertube } from 'youtubei.js';

const PORT = process.env.PORT || 10000;
const RETRY_OFFLINE_MS = 60_000;
const MAX_SOURCES = 30;

// ---------- shared sources: one reader per channel, shared by every open page ----------
const sources = new Map(); // key -> { subs:Set<ws>, stop(), status }

function broadcast(src, msg) {
  const text = JSON.stringify(msg);
  if (msg.type === 'status') src.status = msg;
  for (const ws of src.subs) if (ws.readyState === 1) ws.send(text);
}

function subscribe(key, ws, create) {
  let src = sources.get(key);
  if (!src) {
    if (sources.size >= MAX_SOURCES) {
      ws.send(JSON.stringify({ type: 'status', platform: key.split(':')[0], state: 'err', text: 'Server is busy, try again soon' }));
      return;
    }
    src = { subs: new Set([ws]), status: null, stop: () => {} };
    sources.set(key, src);
    src.stop = create(msg => broadcast(src, msg));
  } else {
    if (src.status && ws.readyState === 1) ws.send(JSON.stringify(src.status));
    src.subs.add(ws);
  }
  ws.on('close', () => {
    src.subs.delete(ws);
    if (src.subs.size === 0) {
      // keep it briefly in case the page is just reloading
      setTimeout(() => { if (src.subs.size === 0 && sources.get(key) === src) { src.stop(); sources.delete(key); } }, 30_000);
    }
  });
}

// ---------- TikTok ----------
function tiktokSource(username) {
  return emit => {
    let stopped = false, conn = null, timer = null;
    const status = (state, text) => emit({ type: 'status', platform: 'tiktok', state, text });
    const start = async () => {
      if (stopped) return;
      status('wait', 'Connecting to @' + username + '…');
      conn = new TikTokLiveConnection(username, { processInitialData: false });
      conn.on(WebcastEvent.CHAT, d => {
        const u = d.user || {};
        if (d.comment) emit({ type: 'chat', platform: 'tiktok', name: u.nickname || u.uniqueId || 'viewer', text: String(d.comment) });
      });
      conn.on(WebcastEvent.STREAM_END, () => { status('wait', 'Live ended, waiting for next live'); retry(); });
      conn.on('disconnected', () => { if (!stopped) { status('wait', 'Reconnecting…'); retry(10_000); } });
      try {
        await conn.connect();
        status('on', 'Live: @' + username);
      } catch (e) {
        if (e instanceof UserOfflineError || /offline|not live|isn't live/i.test(String(e && e.message))) status('wait', '@' + username + ' isn\'t live yet. Checking every minute');
        else status('err', 'Couldn\'t connect: ' + String((e && e.message) || e).slice(0, 80));
        retry();
      }
    };
    const retry = (ms = RETRY_OFFLINE_MS) => {
      clearTimeout(timer);
      try { conn && conn.disconnect(); } catch (e) {}
      if (!stopped) timer = setTimeout(start, ms);
    };
    start();
    return () => { stopped = true; clearTimeout(timer); try { conn && conn.disconnect(); } catch (e) {} };
  };
}

// ---------- YouTube ----------
let innertube = null;
const getInnertube = async () => innertube || (innertube = await Innertube.create({ retrieve_player: false }));

async function findLiveVideoId(input) {
  input = input.trim();
  const direct = input.match(/(?:v=|youtu\.be\/|\/live\/|\/shorts\/|\/embed\/)([\w-]{11})/) || input.match(/^([\w-]{11})$/);
  if (direct) return direct[1];
  let path;
  const chan = input.match(/(UC[\w-]{22})/);
  if (chan) path = '/channel/' + chan[1];
  else path = '/@' + input.replace(/^.*youtube\.com\//, '').replace(/^@/, '').split(/[/?]/)[0];
  const res = await fetch('https://www.youtube.com' + path + '/live', {
    headers: { 'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0 Safari/537.36', 'accept-language': 'en-US,en;q=0.9', cookie: 'CONSENT=YES+1' }
  });
  if (res.status === 404) throw new Error('Couldn\'t find that YouTube channel');
  const html = await res.text();
  const canon = html.match(/<link rel="canonical" href="https:\/\/www\.youtube\.com\/watch\?v=([\w-]{11})"/);
  if (!canon) return null;
  if (!/"isLive":true|"isLiveNow":true|"liveBroadcastDetails"/.test(html)) return null;
  return canon[1];
}

function youtubeSource(handle) {
  return emit => {
    let stopped = false, chat = null, timer = null;
    const status = (state, text) => emit({ type: 'status', platform: 'youtube', state, text });
    const retry = (ms = RETRY_OFFLINE_MS) => {
      clearTimeout(timer);
      try { chat && chat.stop(); } catch (e) {}
      chat = null;
      if (!stopped) timer = setTimeout(start, ms);
    };
    const start = async () => {
      if (stopped) return;
      status('wait', 'Looking for your live stream…');
      try {
        const id = await findLiveVideoId(handle);
        if (!id) { status('wait', 'Not live yet. Checking every minute'); return retry(); }
        const yt = await getInnertube();
        const info = await yt.getInfo(id);
        chat = info.getLiveChat();
        chat.on('start', () => status('on', 'Live'));
        chat.on('chat-update', action => {
          if (action.type !== 'AddChatItemAction' || !action.item) return;
          const item = action.item;
          if (item.type !== 'LiveChatTextMessage' && item.type !== 'LiveChatPaidMessage') return;
          const text = item.message ? item.message.toString() : '';
          const name = item.author && item.author.name ? String(item.author.name) : 'viewer';
          const badges = [];
          if (item.author && item.author.is_moderator) badges.push('mod');
          if (item.author && item.author.badges && item.author.badges.some(b => /member/i.test(String(b.tooltip || '')))) badges.push('sub');
          if (text) emit({ type: 'chat', platform: 'youtube', name: name.replace(/^@/, ''), text, badges });
        });
        chat.on('end', () => { status('wait', 'Live ended, waiting for next live'); retry(); });
        chat.on('error', e => { status('wait', 'Reconnecting…'); retry(15_000); });
        chat.start();
      } catch (e) {
        status('err', String((e && e.message) || e).slice(0, 80) + '. Trying again soon');
        retry();
      }
    };
    start();
    return () => { stopped = true; clearTimeout(timer); try { chat && chat.stop(); } catch (e) {} };
  };
}

// ---------- server ----------
const server = http.createServer((req, res) => {
  res.writeHead(200, { 'content-type': 'text/plain', 'access-control-allow-origin': '*' });
  res.end('Lethal Chat server is running.');
});
const wss = new WebSocketServer({ server, path: '/chat' });
const clean = v => String(v || '').trim().slice(0, 120);

wss.on('connection', (ws, req) => {
  const q = new URL(req.url, 'http://x').searchParams;
  const tiktok = clean(q.get('tiktok')).replace(/^.*tiktok\.com\//, '').replace(/^@/, '').split(/[/?]/)[0].toLowerCase();
  const youtube = clean(q.get('youtube'));
  if (tiktok && /^[\w.]{2,30}$/.test(tiktok)) subscribe('tiktok:' + tiktok, ws, tiktokSource(tiktok));
  if (youtube) subscribe('youtube:' + youtube.toLowerCase(), ws, youtubeSource(youtube));
  ws.send(JSON.stringify({ type: 'hello' }));
  const ping = setInterval(() => { if (ws.readyState === 1) ws.ping(); }, 25_000);
  ws.on('close', () => clearInterval(ping));
});

server.listen(PORT, () => console.log('Lethal Chat server listening on ' + PORT));
