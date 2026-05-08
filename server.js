/**
 * 들락날락 키오스크 서버 v3.0
 * 광양시청소년문화센터
 * Node.js + Express + Supabase + SSE
 */

const express = require('express');
const { createClient } = require('@supabase/supabase-js');
const app = express();
const PORT = process.env.PORT || 3000;

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_KEY
);

app.use(express.json());
app.use(express.static(__dirname));
app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Headers', 'Content-Type');
  res.header('Access-Control-Allow-Methods', 'GET,POST,DELETE,OPTIONS');
  if (req.method === 'OPTIONS') return res.sendStatus(200);
  next();
});

let pcTimers = {};
let sseClients = [];

// ===================== SSE =====================
function broadcast(eventName, data) {
  const msg = `event: ${eventName}\ndata: ${JSON.stringify(data)}\n\n`;
  sseClients = sseClients.filter(c => { try { c.write(msg); return true; } catch(e) { return false; } });
}

app.get('/events', async (req, res) => {
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.flushHeaders();
  try {
    const state = await getState();
    res.write(`event: init\ndata: ${JSON.stringify(state)}\n\n`);
  } catch(e) { console.error('SSE init error:', e); }
  sseClients.push(res);
  const hb = setInterval(() => { try { res.write(': heartbeat\n\n'); } catch(e) {} }, 20000);
  req.on('close', () => { clearInterval(hb); sseClients = sseClients.filter(c => c !== res); });
});

// ===================== 상태 조회 =====================
async function getState() {
  const [recRes, pcRes, pcQRes, kqRes, notRes, fsRes] = await Promise.all([
    supabase.from('records').select('*').order('id', { ascending: true }),
    supabase.from('pc_status').select('*').order('pc_key'),
    supabase.from('pc_queue').select('*').order('id', { ascending: true }),
    supabase.from('karaoke_queue').select('*').order('id', { ascending: true }),
    supabase.from('notices').select('*').eq('active', true).order('id', { ascending: false }),
    supabase.from('facility_status').select('*'),
  ]);

  const records = (recRes.data || []).map(r => ({
    id: r.id, date: r.date, time: r.time,
    name: r.name, phone: r.phone, facility: r.facility,
    booth: r.booth, game: r.game || '-', active: r.active, endTime: r.end_time,
    grade: r.grade || '-',
  }));

  const pcStatus = {};
  (pcRes.data || []).forEach(p => {
    pcStatus[p.pc_key] = { free: p.free, user: p.user_name, phone: p.phone, time: p.start_time, endTime: p.end_time, endMs: p.end_ms };
  });

  const pcQueue = (pcQRes.data || []).map(q => ({ id: q.id, name: q.name, phone: q.phone, time: q.time, grade: q.grade || '-' }));
  const karaokeQueue = (kqRes.data || []).map(q => ({ id: q.id, name: q.name, phone: q.phone, time: q.time, grade: q.grade || '-' }));
  const notices = (notRes.data || []).map(n => ({ id: n.id, title: n.title, content: n.content, createdAt: n.created_at }));

  const facilityStatus = {};
  (fsRes.data || []).forEach(f => { facilityStatus[f.id] = { inspection: f.inspection, msg: f.inspection_msg }; });

  return { records, pcStatus, pcQueue, karaokeQueue, notices, facilityStatus };
}

function nowStr() { return new Date().toLocaleTimeString('ko-KR', { hour: '2-digit', minute: '2-digit', second: '2-digit' }); }
function todayStr() { return new Date().toLocaleDateString('ko-KR'); }

// ===================== PC 타이머 =====================
async function restoreTimers() {
  const { data } = await supabase.from('pc_status').select('*').eq('free', false);
  if (!data) return;
  data.forEach(p => {
    if (p.end_ms) {
      const rem = p.end_ms - Date.now();
      if (rem > 0) pcTimers[p.pc_key] = setTimeout(() => endPC(p.pc_key, true), rem);
      else endPC(p.pc_key, true);
    }
  });
}

async function endPC(pcKey, auto = false) {
  if (pcTimers[pcKey]) { clearTimeout(pcTimers[pcKey]); delete pcTimers[pcKey]; }
  const { data: pcData } = await supabase.from('pc_status').select('user_name').eq('pc_key', pcKey).single();
  const prevUser = pcData?.user_name || '';
  await Promise.all([
    supabase.from('pc_status').update({ free: true, user_name: null, phone: null, start_time: null, end_time: null, end_ms: null }).eq('pc_key', pcKey),
    supabase.from('records').update({ active: false, end_time: nowStr() }).eq('facility', 'PC').eq('booth', pcKey).eq('active', true),
  ]);
  const state = await getState();
  broadcast('update', state);
  broadcast('notify', { type: 'pc_end', message: `${pcKey} 이용 종료${auto ? ' (1시간 만료)' : ''} — ${prevUser}`, pcKey, auto });
  if (state.pcQueue.length > 0) broadcast('notify', { type: 'queue_ready', message: `⏳ PC 다음 대기자: ${state.pcQueue[0].name}`, next: state.pcQueue[0] });
}

// ===================== API: 상태 =====================
app.get('/api/state', async (req, res) => {
  try { res.json(await getState()); } catch(e) { res.status(500).json({ error: e.message }); }
});

// ===================== API: 일반 시설 등록 =====================
app.post('/api/register', async (req, res) => {
  const { name, phone, facility, booth, game, grade } = req.body;
  if (!name || !phone || !facility) return res.status(400).json({ error: '필수 항목 누락' });
  const { data, error } = await supabase.from('records').insert({
    date: todayStr(), time: nowStr(), name, phone, facility, booth: booth || '-', game: game || '-', active: true, grade: grade || '-',
  }).select().single();
  if (error) return res.status(500).json({ error: error.message });
  const state = await getState();
  broadcast('update', state);
  broadcast('notify', { type: 'register', message: `${facility} 등록 — ${name} ${grade||''} (${booth || '자유이용'})` });
  res.json({ ok: true, record: data });
});

// ===================== API: PC 시작 =====================
app.post('/api/pc/start', async (req, res) => {
  const { name, phone, pcKey, grade } = req.body;
  if (!name || !phone || !pcKey) return res.status(400).json({ error: '필수 항목 누락' });
  const { data: pc } = await supabase.from('pc_status').select('free').eq('pc_key', pcKey).single();
  if (!pc?.free) return res.status(400).json({ error: '이미 사용 중' });
  const t = nowStr(); const endMs = Date.now() + 60 * 60 * 1000;
  const endTimeStr = new Date(endMs).toLocaleTimeString('ko-KR', { hour: '2-digit', minute: '2-digit' });
  await Promise.all([
    supabase.from('pc_status').update({ free: false, user_name: name, phone, start_time: t, end_time: endTimeStr, end_ms: endMs }).eq('pc_key', pcKey),
    supabase.from('records').insert({ date: todayStr(), time: t, name, phone, facility: 'PC', booth: pcKey, game: '-', active: true, grade: grade || '-' }),
  ]);
  if (pcTimers[pcKey]) clearTimeout(pcTimers[pcKey]);
  pcTimers[pcKey] = setTimeout(() => endPC(pcKey, true), 60 * 60 * 1000);
  const state = await getState();
  broadcast('update', state);
  broadcast('notify', { type: 'pc_start', message: `${pcKey} 이용 시작 — ${name} ${grade||''} (종료: ${endTimeStr})`, pcKey });
  res.json({ ok: true, endTime: endTimeStr });
});

// ===================== API: PC 종료 =====================
app.post('/api/pc/end', async (req, res) => {
  const { pcKey } = req.body;
  if (!pcKey) return res.status(400).json({ error: 'pcKey 필요' });
  await endPC(pcKey, false);
  res.json({ ok: true });
});

// ===================== API: PC 대기 =====================
app.post('/api/pc/queue', async (req, res) => {
  const { name, phone, grade } = req.body;
  if (!name || !phone) return res.status(400).json({ error: '필수 항목 누락' });
  await supabase.from('pc_queue').insert({ name, phone, time: nowStr(), grade: grade || '-' });
  const state = await getState();
  broadcast('update', state);
  broadcast('notify', { type: 'queue', message: `PC 대기 등록 — ${name} ${grade||''} (${state.pcQueue.length}번째)` });
  res.json({ ok: true, position: state.pcQueue.length });
});

// ===================== API: PC 대기 배정 =====================
app.post('/api/pc/queue-assign', async (req, res) => {
  const { pcKey } = req.body;
  const { data: queue } = await supabase.from('pc_queue').select('*').order('id').limit(1);
  if (!queue?.length) return res.status(400).json({ error: '대기자 없음' });
  const next = queue[0]; const t = nowStr(); const endMs = Date.now() + 60 * 60 * 1000;
  const endTimeStr = new Date(endMs).toLocaleTimeString('ko-KR', { hour: '2-digit', minute: '2-digit' });
  await Promise.all([
    supabase.from('pc_queue').delete().eq('id', next.id),
    supabase.from('pc_status').update({ free: false, user_name: next.name, phone: next.phone, start_time: t, end_time: endTimeStr, end_ms: endMs }).eq('pc_key', pcKey),
    supabase.from('records').insert({ date: todayStr(), time: t, name: next.name, phone: next.phone, facility: 'PC', booth: pcKey, game: '-', active: true }),
  ]);
  if (pcTimers[pcKey]) clearTimeout(pcTimers[pcKey]);
  pcTimers[pcKey] = setTimeout(() => endPC(pcKey, true), 60 * 60 * 1000);
  const state = await getState();
  broadcast('update', state);
  broadcast('notify', { type: 'queue_assign', message: `대기자 ${next.name} → ${pcKey} 배정 완료` });
  res.json({ ok: true });
});

// ===================== API: 코인노래방 대기 =====================
app.post('/api/karaoke/queue', async (req, res) => {
  const { name, phone, grade } = req.body;
  if (!name || !phone) return res.status(400).json({ error: '필수 항목 누락' });
  await supabase.from('karaoke_queue').insert({ name, phone, time: nowStr(), grade: grade || '-' });
  const state = await getState();
  broadcast('update', state);
  broadcast('notify', { type: 'karaoke_queue', message: `코인노래방 대기 — ${name} ${grade||''} (${state.karaokeQueue.length}번째)` });
  res.json({ ok: true, position: state.karaokeQueue.length });
});

app.post('/api/karaoke/queue-remove', async (req, res) => {
  const { id } = req.body;
  await supabase.from('karaoke_queue').delete().eq('id', id);
  const state = await getState();
  broadcast('update', state);
  res.json({ ok: true });
});

// ===================== API: 일반 종료 =====================
app.post('/api/end', async (req, res) => {
  const { facility, booth } = req.body;
  await supabase.from('records').update({ active: false, end_time: nowStr() }).eq('facility', facility).eq('booth', booth).eq('active', true);
  const state = await getState();
  broadcast('update', state);
  res.json({ ok: true });
});

// ===================== API: 기록 삭제 =====================
app.delete('/api/record/:id', async (req, res) => {
  const { id } = req.params;
  if (!id) return res.status(400).json({ error: 'id 필요' });
  const { error } = await supabase.from('records').delete().eq('id', id);
  if (error) return res.status(500).json({ error: error.message });
  const state = await getState();
  broadcast('update', state);
  res.json({ ok: true });
});

// ===================== API: 공지사항 =====================
app.get('/api/notices', async (req, res) => {
  const { data } = await supabase.from('notices').select('*').eq('active', true).order('id', { ascending: false });
  res.json(data || []);
});

app.post('/api/notices', async (req, res) => {
  const { title, content } = req.body;
  if (!title || !content) return res.status(400).json({ error: '제목과 내용을 입력해주세요' });
  const { data, error } = await supabase.from('notices').insert({ title, content, active: true }).select().single();
  if (error) return res.status(500).json({ error: error.message });
  const state = await getState();
  broadcast('update', state);
  broadcast('notify', { type: 'notice', message: `📢 새 공지사항: ${title}` });
  res.json({ ok: true, notice: data });
});

app.delete('/api/notices/:id', async (req, res) => {
  await supabase.from('notices').update({ active: false }).eq('id', req.params.id);
  const state = await getState();
  broadcast('update', state);
  res.json({ ok: true });
});

// ===================== API: 시설 점검 =====================
app.post('/api/facility/inspection', async (req, res) => {
  const { id, inspection, msg } = req.body;
  await supabase.from('facility_status').update({ inspection, inspection_msg: msg || '점검 중입니다' }).eq('id', id);
  const state = await getState();
  broadcast('update', state);
  broadcast('notify', { type: 'inspection', message: `${inspection ? '🔧 점검 시작' : '✅ 점검 완료'}: ${id}` });
  res.json({ ok: true });
});

// ===================== 서버 시작 =====================
app.listen(PORT, '0.0.0.0', async () => {
  console.log('========================================');
  console.log('  🎮 들락날락 키오스크 서버 v3.0');
  console.log('  광양시청소년문화센터');
  console.log(`  포트: ${PORT}`);
  console.log('========================================');
  await restoreTimers();
  console.log('  [DB] Supabase 연결 완료');
});
