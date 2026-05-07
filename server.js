/**
 * 들락날락 키오스크 서버
 * Node.js + Express + Supabase + SSE 실시간 동기화
 * Railway 배포용
 */

const express = require('express');
const { createClient } = require('@supabase/supabase-js');
const path = require('path');
const app = express();
const PORT = process.env.PORT || 3000;

// ===================== Supabase 연결 =====================
const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_KEY
);

app.use(express.json());
app.use(express.static(__dirname));

// CORS 허용
app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Headers', 'Content-Type');
  res.header('Access-Control-Allow-Methods', 'GET,POST,OPTIONS');
  if (req.method === 'OPTIONS') return res.sendStatus(200);
  next();
});

// ===================== 메모리 상태 (PC타이머용) =====================
let pcTimers = {};
let sseClients = [];

// ===================== SSE 브로드캐스트 =====================
function broadcast(eventName, data) {
  const msg = `event: ${eventName}\ndata: ${JSON.stringify(data)}\n\n`;
  sseClients = sseClients.filter(client => {
    try { client.write(msg); return true; }
    catch (e) { return false; }
  });
}

// ===================== SSE 연결 =====================
app.get('/events', async (req, res) => {
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.flushHeaders();

  try {
    const state = await getState();
    res.write(`event: init\ndata: ${JSON.stringify(state)}\n\n`);
  } catch (e) {
    console.error('SSE init error:', e);
  }

  sseClients.push(res);
  console.log(`[SSE] 연결 (총 ${sseClients.length}개)`);

  const hb = setInterval(() => {
    try { res.write(': heartbeat\n\n'); } catch (e) {}
  }, 20000);

  req.on('close', () => {
    clearInterval(hb);
    sseClients = sseClients.filter(c => c !== res);
    console.log(`[SSE] 해제 (총 ${sseClients.length}개)`);
  });
});

// ===================== 상태 조회 (Supabase) =====================
async function getState() {
  const [recRes, pcRes, qRes] = await Promise.all([
    supabase.from('records').select('*').order('id', { ascending: true }),
    supabase.from('pc_status').select('*').order('pc_key'),
    supabase.from('pc_queue').select('*').order('id', { ascending: true }),
  ]);

  const records = (recRes.data || []).map(r => ({
    id: r.id,
    date: r.date,
    time: r.time,
    name: r.name,
    phone: r.phone,
    facility: r.facility,
    booth: r.booth,
    game: r.game || '-',
    active: r.active,
    endTime: r.end_time,
  }));

  const pcStatus = {};
  (pcRes.data || []).forEach(p => {
    pcStatus[p.pc_key] = {
      free: p.free,
      user: p.user_name,
      phone: p.phone,
      time: p.start_time,
      endTime: p.end_time,
      endMs: p.end_ms,
    };
  });

  const pcQueue = (qRes.data || []).map(q => ({
    id: q.id,
    name: q.name,
    phone: q.phone,
    time: q.time,
  }));

  return { records, pcStatus, pcQueue };
}

// ===================== 헬퍼 =====================
function nowStr() {
  return new Date().toLocaleTimeString('ko-KR', { hour: '2-digit', minute: '2-digit', second: '2-digit' });
}
function todayStr() {
  return new Date().toLocaleDateString('ko-KR');
}

// ===================== PC 자동종료 타이머 복원 =====================
async function restoreTimers() {
  const { data } = await supabase.from('pc_status').select('*').eq('free', false);
  if (!data) return;
  data.forEach(p => {
    if (p.end_ms) {
      const remaining = p.end_ms - Date.now();
      if (remaining > 0) {
        console.log(`[타이머복원] ${p.pc_key} 남은시간: ${Math.round(remaining/60000)}분`);
        pcTimers[p.pc_key] = setTimeout(() => endPC(p.pc_key, true), remaining);
      } else {
        endPC(p.pc_key, true);
      }
    }
  });
}

// ===================== PC 종료 처리 =====================
async function endPC(pcKey, auto = false) {
  if (pcTimers[pcKey]) { clearTimeout(pcTimers[pcKey]); delete pcTimers[pcKey]; }

  const { data: pcData } = await supabase.from('pc_status').select('user_name').eq('pc_key', pcKey).single();
  const prevUser = pcData?.user_name || '';

  await Promise.all([
    supabase.from('pc_status').update({
      free: true, user_name: null, phone: null,
      start_time: null, end_time: null, end_ms: null
    }).eq('pc_key', pcKey),
    supabase.from('records').update({ active: false, end_time: nowStr() })
      .eq('facility', 'PC').eq('booth', pcKey).eq('active', true),
  ]);

  const state = await getState();
  broadcast('update', state);
  broadcast('notify', {
    type: 'pc_end',
    message: `${pcKey} 이용 종료${auto ? ' (1시간 만료)' : ''} — ${prevUser}`,
    pcKey, auto
  });

  if (state.pcQueue.length > 0) {
    broadcast('notify', {
      type: 'queue_ready',
      message: `⏳ 다음 대기자: ${state.pcQueue[0].name} (${state.pcQueue[0].phone})`,
      next: state.pcQueue[0]
    });
  }
}

// ===================== API: 상태 조회 =====================
app.get('/api/state', async (req, res) => {
  try {
    res.json(await getState());
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ===================== API: 일반 시설 등록 =====================
app.post('/api/register', async (req, res) => {
  const { name, phone, facility, booth, game } = req.body;
  if (!name || !phone || !facility) return res.status(400).json({ error: '필수 항목 누락' });

  const { data, error } = await supabase.from('records').insert({
    date: todayStr(), time: nowStr(),
    name, phone, facility,
    booth: booth || '-',
    game: game || '-',
    active: true,
  }).select().single();

  if (error) return res.status(500).json({ error: error.message });

  const state = await getState();
  broadcast('update', state);
  broadcast('notify', { type: 'register', message: `${facility} 등록 — ${name} (${booth || '자유이용'})` });
  res.json({ ok: true, record: data });
});

// ===================== API: PC 이용 시작 =====================
app.post('/api/pc/start', async (req, res) => {
  const { name, phone, pcKey } = req.body;
  if (!name || !phone || !pcKey) return res.status(400).json({ error: '필수 항목 누락' });

  const { data: pc } = await supabase.from('pc_status').select('free').eq('pc_key', pcKey).single();
  if (!pc?.free) return res.status(400).json({ error: '이미 사용 중' });

  const t = nowStr();
  const endMs = Date.now() + 60 * 60 * 1000;
  const endTimeStr = new Date(endMs).toLocaleTimeString('ko-KR', { hour: '2-digit', minute: '2-digit' });

  await Promise.all([
    supabase.from('pc_status').update({
      free: false, user_name: name, phone,
      start_time: t, end_time: endTimeStr, end_ms: endMs
    }).eq('pc_key', pcKey),
    supabase.from('records').insert({
      date: todayStr(), time: t,
      name, phone, facility: 'PC', booth: pcKey, game: '-', active: true
    }),
  ]);

  if (pcTimers[pcKey]) clearTimeout(pcTimers[pcKey]);
  pcTimers[pcKey] = setTimeout(() => endPC(pcKey, true), 60 * 60 * 1000);

  const state = await getState();
  broadcast('update', state);
  broadcast('notify', { type: 'pc_start', message: `${pcKey} 이용 시작 — ${name} (종료: ${endTimeStr})`, pcKey });
  res.json({ ok: true, endTime: endTimeStr });
});

// ===================== API: PC 이용 종료 =====================
app.post('/api/pc/end', async (req, res) => {
  const { pcKey } = req.body;
  if (!pcKey) return res.status(400).json({ error: 'pcKey 필요' });
  await endPC(pcKey, false);
  res.json({ ok: true });
});

// ===================== API: PC 대기 등록 =====================
app.post('/api/pc/queue', async (req, res) => {
  const { name, phone } = req.body;
  if (!name || !phone) return res.status(400).json({ error: '필수 항목 누락' });

  await supabase.from('pc_queue').insert({ name, phone, time: nowStr() });

  const state = await getState();
  broadcast('update', state);
  broadcast('notify', { type: 'queue', message: `PC 대기 등록 — ${name} (${state.pcQueue.length}번째)` });
  res.json({ ok: true, position: state.pcQueue.length });
});

// ===================== API: 대기자 → PC 배정 =====================
app.post('/api/pc/queue-assign', async (req, res) => {
  const { pcKey } = req.body;

  const { data: queue } = await supabase.from('pc_queue').select('*').order('id').limit(1);
  if (!queue?.length) return res.status(400).json({ error: '대기자 없음' });

  const next = queue[0];
  const t = nowStr();
  const endMs = Date.now() + 60 * 60 * 1000;
  const endTimeStr = new Date(endMs).toLocaleTimeString('ko-KR', { hour: '2-digit', minute: '2-digit' });

  await Promise.all([
    supabase.from('pc_queue').delete().eq('id', next.id),
    supabase.from('pc_status').update({
      free: false, user_name: next.name, phone: next.phone,
      start_time: t, end_time: endTimeStr, end_ms: endMs
    }).eq('pc_key', pcKey),
    supabase.from('records').insert({
      date: todayStr(), time: t,
      name: next.name, phone: next.phone,
      facility: 'PC', booth: pcKey, game: '-', active: true
    }),
  ]);

  if (pcTimers[pcKey]) clearTimeout(pcTimers[pcKey]);
  pcTimers[pcKey] = setTimeout(() => endPC(pcKey, true), 60 * 60 * 1000);

  const state = await getState();
  broadcast('update', state);
  broadcast('notify', { type: 'queue_assign', message: `대기자 ${next.name} → ${pcKey} 배정 완료` });
  res.json({ ok: true });
});

// ===================== API: 일반 시설 이용 종료 =====================
app.post('/api/end', async (req, res) => {
  const { facility, booth } = req.body;
  await supabase.from('records').update({ active: false, end_time: nowStr() })
    .eq('facility', facility).eq('booth', booth).eq('active', true);

  const state = await getState();
  broadcast('update', state);
  res.json({ ok: true });
});

// ===================== 서버 시작 =====================
app.listen(PORT, '0.0.0.0', async () => {
  console.log('');
  console.log('========================================');
  console.log('  🎮 들락날락 키오스크 서버 가동 중');
  console.log(`  포트: ${PORT}`);
  console.log('========================================');
  await restoreTimers();
  console.log('  [DB] Supabase 연결 완료');
  console.log('========================================');
});
