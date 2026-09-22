/**
 * 들락날락 키오스크 서버 v3.2
 * 광양시청소년문화센터
 * Node.js + Express + Supabase + SSE
 *
 * v3.2 변경사항 (이용기록부 전체 날짜 조회)
 * - /api/records 엔드포인트 추가: ?date=전체 또는 ?date=2026. 5. 13.
 * - getState()는 SSE/실시간용으로 오늘 날짜만 유지 (트래픽 절감 유지)
 * - admin.html의 이용기록부 탭에서 날짜 선택 시 /api/records 호출
 */

const express = require('express');
const { createClient } = require('@supabase/supabase-js');
const app = express();
const PORT = process.env.PORT || 3000;

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_KEY
);

app.set('trust proxy', 1);
app.use(express.json());
app.use(express.static(__dirname));
app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Headers', 'Content-Type, x-admin-pw');
  res.header('Access-Control-Allow-Methods', 'GET,POST,DELETE,OPTIONS');
  if (req.method === 'OPTIONS') return res.sendStatus(200);
  next();
});

let pcTimers = {};
let sseClients = [];

// ===================== 관리자 인증 =====================
// ⚠️ 배포 환경(Railway 등)에 ADMIN_PW 환경변수를 반드시 설정하세요.
// 설정하지 않으면 기존 기본값('2230')으로 동작하되 콘솔에 경고를 출력합니다.
const ADMIN_PW = process.env.ADMIN_PW || '2230';
if (!process.env.ADMIN_PW) {
  console.warn('[경고] ADMIN_PW 환경변수가 설정되지 않아 기본 비밀번호를 사용합니다. 배포 환경에서는 반드시 설정하세요.');
}
function requireAdmin(req, res, next) {
  const pw = req.headers['x-admin-pw'];
  if (pw !== ADMIN_PW) return res.status(401).json({ error: '관리자 인증 필요' });
  next();
}
app.post('/api/admin/verify', (req, res) => {
  const { pw } = req.body || {};
  res.json({ ok: pw === ADMIN_PW });
});

// ===================== 입력 정제 (XSS/데이터 오염 방지) =====================
function sanitizeText(v, maxLen = 60) {
  if (typeof v !== 'string') return v;
  return v.replace(/[<>&"']/g, '').trim().slice(0, maxLen);
}

// ===================== 간단한 요청 빈도 제한 =====================
function getClientIp(req) {
  const xff = req.headers['x-forwarded-for'];
  if (xff) return xff.split(',')[0].trim();
  return req.socket.remoteAddress;
}
const rateBuckets = new Map();
function rateLimit({ windowMs = 60000, max = 20 } = {}) {
  return (req, res, next) => {
    const key = getClientIp(req);
    const now = Date.now();
    let bucket = rateBuckets.get(key);
    if (!bucket || now - bucket.start > windowMs) {
      bucket = { start: now, count: 0 };
      rateBuckets.set(key, bucket);
    }
    bucket.count++;
    if (bucket.count > max) {
      return res.status(429).json({ error: '요청이 너무 많습니다. 잠시 후 다시 시도해주세요.' });
    }
    next();
  };
}
setInterval(() => {
  const now = Date.now();
  for (const [key, bucket] of rateBuckets) {
    if (now - bucket.start > 5 * 60000) rateBuckets.delete(key);
  }
}, 5 * 60000);

// ===================== getState 캐시 =====================
let stateCache = null;
let stateCacheTime = 0;
const STATE_CACHE_TTL = 5000; // 5초

async function getState(forceRefresh = false) {
  const now = Date.now();
  if (!forceRefresh && stateCache && (now - stateCacheTime) < STATE_CACHE_TTL) {
    return stateCache;
  }

  // SSE/실시간용: 오늘 날짜만 조회 (트래픽 절감 유지)
  const today = todayStr();

  const [recRes, pcRes, pcQRes, kqRes, pqRes, notRes, fsRes] = await Promise.all([
    supabase.from('records').select('*').eq('date', today).order('id', { ascending: true }),
    supabase.from('pc_status').select('*').order('pc_key'),
    supabase.from('pc_queue').select('*').order('id', { ascending: true }),
    supabase.from('karaoke_queue').select('*').order('id', { ascending: true }),
    supabase.from('pool_queue').select('*').order('id', { ascending: true }),
    supabase.from('notices').select('*').eq('active', true).order('id', { ascending: false }),
    supabase.from('facility_status').select('*'),
  ]);

  const records = (recRes.data || []).map(r => ({
    id: r.id, date: r.date, time: r.time,
    name: r.name, phone: r.phone, facility: r.facility,
    booth: r.booth, game: r.game || '-', active: r.active, endTime: r.end_time,
    grade: r.grade || '-', gender: r.gender || '-', headcount: r.headcount || 1,
  }));

  const pcStatus = {};
  (pcRes.data || []).forEach(p => {
    pcStatus[p.pc_key] = { free: p.free, user: p.user_name, phone: p.phone, time: p.start_time, endTime: p.end_time, endMs: p.end_ms };
  });

  const pcQueue = (pcQRes.data || []).map(q => ({ id: q.id, name: q.name, phone: q.phone, time: q.time, grade: q.grade || '-' }));
  const karaokeQueue = (kqRes.data || []).map(q => ({ id: q.id, name: q.name, phone: q.phone, time: q.time, grade: q.grade || '-' }));
  const poolQueue = (pqRes.data || []).map(q => ({ id: q.id, name: q.name, phone: q.phone, time: q.time, grade: q.grade || '-' }));
  const notices = (notRes.data || []).map(n => ({ id: n.id, title: n.title, content: n.content, createdAt: n.created_at }));

  const facilityStatus = {};
  (fsRes.data || []).forEach(f => { facilityStatus[f.id] = { inspection: f.inspection, msg: f.inspection_msg }; });

  const result = { records, pcStatus, pcQueue, karaokeQueue, poolQueue, notices, facilityStatus, pcPowerStatus };

  stateCache = result;
  stateCacheTime = now;

  return result;
}

function invalidateCache() {
  stateCache = null;
  stateCacheTime = 0;
}

function formatPhone(phone) {
  const v = (phone || '').replace(/\D/g, '');
  if (v.length > 10) return `${v.slice(0,3)}-${v.slice(3,7)}-${v.slice(7,11)}`;
  if (v.length > 7) return `${v.slice(0,3)}-${v.slice(3,6)}-${v.slice(6,10)}`;
  if (v.length > 3) return `${v.slice(0,3)}-${v.slice(3)}`;
  return phone;
}

const KR_TZ = 'Asia/Seoul';
function nowStr() {
  return new Date().toLocaleTimeString('ko-KR', { hour: '2-digit', minute: '2-digit', second: '2-digit', timeZone: KR_TZ, hour12: false });
}
function todayStr() {
  return new Date().toLocaleDateString('ko-KR', { timeZone: KR_TZ });
}

// ===================== SSE =====================
function broadcast(eventName, data) {
  const msg = `event: ${eventName}\ndata: ${JSON.stringify(data)}\n\n`;
  sseClients = sseClients.filter(c => { try { c.write(msg); return true; } catch(e) { return false; } });
}

app.get('/events', async (req, res) => {
  const clientIp = getClientIp(req);

  const sameIpCount = sseClients.filter(c => c._clientIp === clientIp).length;
  if (sameIpCount >= 3) {
    console.warn(`[SSE 제한] ${clientIp} 연결 초과 (${sameIpCount}개) — 거부`);
    return res.status(429).end();
  }

  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache, no-transform');
  res.setHeader('Connection', 'keep-alive');
  res.setHeader('X-Accel-Buffering', 'no');
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.flushHeaders();

  res._clientIp = clientIp;

  try {
    const state = await getState();
    res.write(`event: init\ndata: ${JSON.stringify(state)}\n\n`);
  } catch(e) { console.error('SSE init error:', e); }

  sseClients.push(res);
  console.log(`[SSE] 연결 (+1) 현재 ${sseClients.length}개 — ${clientIp}`);

  const hb = setInterval(() => { try { res.write(': heartbeat\n\n'); } catch(e) {} }, 20000);
  req.on('close', () => {
    clearInterval(hb);
    sseClients = sseClients.filter(c => c !== res);
    console.log(`[SSE] 연결 해제 (-1) 현재 ${sseClients.length}개 — ${clientIp}`);
  });
});

// ===================== 상태 조회 헬퍼 =====================
async function refreshAndBroadcast(eventName, extraData) {
  invalidateCache();
  const state = await getState(true);
  broadcast('update', state);
  if (eventName && extraData) {
    broadcast(eventName, extraData);
  }
  return state;
}

// ===================== API: 상태 (실시간/오늘) =====================
app.get('/api/state', async (req, res) => {
  try { res.json(await getState()); } catch(e) { res.status(500).json({ error: e.message }); }
});

// ===================== API: 이용기록 날짜 목록 (관리자 전용: 개인정보 포함) =====================
app.get('/api/records/dates', requireAdmin, async (req, res) => {
  try {
    const { data, error } = await supabase
      .from('records')
      .select('date')
      .order('date', { ascending: false });
    if (error) return res.status(500).json({ error: error.message });
    const dates = [...new Set((data || []).map(r => r.date))];
    res.json({ dates });
  } catch(e) {
    res.status(500).json({ error: e.message });
  }
});

// ===================== API: 이용기록 조회 (날짜 필터 지원) =====================
// ?date=전체  → 전체 기간
// ?date=2026. 5. 13.  → 특정 날짜
// (파라미터 없음)  → 오늘
// 관리자 전용: 이름/전화번호 등 개인정보가 포함되므로 인증 필요
app.get('/api/records', requireAdmin, async (req, res) => {
  try {
    const { date } = req.query;
    let query = supabase.from('records').select('*').order('id', { ascending: false }).limit(10000);

    if (!date || date === '오늘') {
      // 오늘만
      query = query.eq('date', todayStr());
    } else if (date.startsWith('월:')) {
      // 월별 필터: "월:2026년 6월" → "2026. 6." 포함하는 날짜
      const m = date.match(/(\d+)년\s*(\d+)월/);
      if (m) {
        const prefix = `${m[1]}. ${parseInt(m[2])}.`; // "2026. 6."
        query = query.like('date', `${prefix}%`);
      }
    } else if (date !== '전체') {
      // 특정 날짜
      query = query.eq('date', date);
    }
    // date === '전체' 이면 필터 없이 전체 조회

    const { data, error } = await query;
    if (error) return res.status(500).json({ error: error.message });

    const records = (data || []).map(r => ({
      id: r.id, date: r.date, time: r.time,
      name: r.name, phone: r.phone, facility: r.facility,
      booth: r.booth, game: r.game || '-', active: r.active, endTime: r.end_time,
      grade: r.grade || '-', gender: r.gender || '-', headcount: r.headcount || 1,
    }));

    res.json({ records });
  } catch(e) {
    res.status(500).json({ error: e.message });
  }
});

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
  setPCCommand(pcKey, 'shutdown');
  console.log(`[자동 종료] ${pcKey} 끄기 명령 자동 전송`);

  const state = await refreshAndBroadcast('notify', {
    type: 'pc_end',
    message: `${pcKey} 이용 종료${auto ? ' (1시간 만료)' : ''} — ${prevUser} 🔴 자동 끄기 명령 전송`,
    pcKey, auto
  });

  if (state.pcQueue.length > 0) {
    broadcast('notify', { type: 'queue_ready', message: `⏳ ${pcKey} 빈자리! 대기자 ${state.pcQueue[0].name}님 배정 가능`, next: state.pcQueue[0], pcKey });
  }
}

// ===================== API: 일반 시설 등록 =====================
app.post('/api/register', rateLimit(), async (req, res) => {
  let { name, phone, facility, booth, game, grade, gender, headcount } = req.body;
  if (!name || !phone || !facility) return res.status(400).json({ error: '필수 항목 누락' });
  name = sanitizeText(name, 30);
  phone = formatPhone(sanitizeText(phone, 20));
  facility = sanitizeText(facility, 20);
  booth = sanitizeText(booth, 20);
  game = sanitizeText(game, 30);
  grade = sanitizeText(grade, 20);
  gender = sanitizeText(gender, 20);
  if (!name || !phone) return res.status(400).json({ error: '필수 항목 누락' });
  const { data, error } = await supabase.from('records').insert({
    date: todayStr(), time: nowStr(), name, phone, facility, booth: booth || '-',
    game: game || '-', active: true, grade: grade || '-', gender: gender || '-',
    headcount: headcount || 1,
  }).select().single();
  if (error) return res.status(500).json({ error: error.message });
  await refreshAndBroadcast('notify', { type: 'register', message: `${facility} 등록 — ${name} ${gender||''} ${grade||''} ${headcount>1?headcount+'명':''} (${booth || '자유이용'})` });
  res.json({ ok: true, record: data });
});

// ===================== API: PC 시작 =====================
app.post('/api/pc/start', rateLimit(), async (req, res) => {
  let { name, phone, pcKey, grade, gender } = req.body;
  if (!name || !phone || !pcKey) return res.status(400).json({ error: '필수 항목 누락' });
  name = sanitizeText(name, 30);
  phone = formatPhone(sanitizeText(phone, 20));
  grade = sanitizeText(grade, 20);
  gender = sanitizeText(gender, 20);
  if (!name || !phone) return res.status(400).json({ error: '필수 항목 누락' });
  const t = nowStr(); const endMs = Date.now() + 60 * 60 * 1000;
  const endTimeStr = new Date(endMs).toLocaleTimeString('ko-KR', { hour: '2-digit', minute: '2-digit', timeZone: KR_TZ, hour12: false });
  // free===true 조건을 걸어 원자적으로 선점 (동시 요청 시 중복 배정 방지)
  const { data: claimed, error: claimErr } = await supabase.from('pc_status')
    .update({ free: false, user_name: name, phone, start_time: t, end_time: endTimeStr, end_ms: endMs })
    .eq('pc_key', pcKey).eq('free', true).select();
  if (claimErr) return res.status(500).json({ error: claimErr.message });
  if (!claimed || claimed.length === 0) return res.status(409).json({ error: '이미 사용 중' });
  await supabase.from('records').insert({ date: todayStr(), time: t, name, phone, facility: 'PC', booth: pcKey, game: '-', active: true, grade: grade || '-', gender: gender || '-' });
  if (pcTimers[pcKey]) clearTimeout(pcTimers[pcKey]);
  pcTimers[pcKey] = setTimeout(() => endPC(pcKey, true), 60 * 60 * 1000);
  setPCCommand(pcKey, 'wakeup');
  console.log(`[자동 WOL] ${pcKey} 켜기 명령 전송`);
  await refreshAndBroadcast('notify', { type: 'pc_start', message: `${pcKey} 이용 시작 — ${name} ${gender||''} ${grade||''} (종료: ${endTimeStr}) 💡 자동 켜기 명령 전송`, pcKey });
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
app.post('/api/pc/queue', rateLimit(), async (req, res) => {
  let { name, phone, grade } = req.body;
  if (!name || !phone) return res.status(400).json({ error: '필수 항목 누락' });
  name = sanitizeText(name, 30); phone = formatPhone(sanitizeText(phone, 20)); grade = sanitizeText(grade, 20);
  if (!name || !phone) return res.status(400).json({ error: '필수 항목 누락' });
  await supabase.from('pc_queue').insert({ name, phone, time: nowStr(), grade: grade || '-' });
  const state = await refreshAndBroadcast('notify', { type: 'queue', message: `PC 대기 등록 — ${name} ${grade||''}` });
  res.json({ ok: true, position: state.pcQueue.length });
});

// ===================== API: PC 대기 취소 =====================
app.post('/api/pc/queue-cancel', async (req, res) => {
  const { id } = req.body;
  if (!id) return res.status(400).json({ error: 'id 필요' });
  await supabase.from('pc_queue').delete().eq('id', id);
  await refreshAndBroadcast();
  res.json({ ok: true });
});

// ===================== API: PC 대기 → 빈자리 자동배정 =====================
app.post('/api/pc/queue-assign', async (req, res) => {
  const { pcKey } = req.body;
  if (!pcKey) return res.status(400).json({ error: 'pcKey 필요' });
  const { data: queue } = await supabase.from('pc_queue').select('*').order('id').limit(1);
  if (!queue?.length) return res.status(400).json({ error: '대기자 없음' });
  const next = queue[0];
  // 대기열에서 원자적으로 제거(선점)되는지 확인 — 동시 배정 요청 시 중복 배정 방지
  const { data: removed } = await supabase.from('pc_queue').delete().eq('id', next.id).select();
  if (!removed || removed.length === 0) return res.status(409).json({ error: '이미 배정 처리된 대기자입니다' });
  const t = nowStr(); const endMs = Date.now() + 60 * 60 * 1000;
  const endTimeStr = new Date(endMs).toLocaleTimeString('ko-KR', { hour: '2-digit', minute: '2-digit', timeZone: KR_TZ, hour12: false });
  await Promise.all([
    supabase.from('pc_status').update({ free: false, user_name: next.name, phone: next.phone, start_time: t, end_time: endTimeStr, end_ms: endMs }).eq('pc_key', pcKey),
    supabase.from('records').insert({ date: todayStr(), time: t, name: next.name, phone: next.phone, facility: 'PC', booth: pcKey, game: '-', active: true, grade: next.grade||'-', gender: next.gender||'-' }),
  ]);
  if (pcTimers[pcKey]) clearTimeout(pcTimers[pcKey]);
  pcTimers[pcKey] = setTimeout(() => endPC(pcKey, true), 60 * 60 * 1000);
  setPCCommand(pcKey, 'wakeup');
  await refreshAndBroadcast('notify', { type: 'queue_assign', message: `대기자 ${next.name} → ${pcKey} 자동 배정 완료 💡 켜기 명령 전송` });
  res.json({ ok: true });
});

// ===================== API: 코인노래방 대기 =====================
app.post('/api/karaoke/queue', rateLimit(), async (req, res) => {
  let { name, phone, grade } = req.body;
  if (!name || !phone) return res.status(400).json({ error: '필수 항목 누락' });
  name = sanitizeText(name, 30); phone = formatPhone(sanitizeText(phone, 20)); grade = sanitizeText(grade, 20);
  if (!name || !phone) return res.status(400).json({ error: '필수 항목 누락' });
  await supabase.from('karaoke_queue').insert({ name, phone, time: nowStr(), grade: grade || '-' });
  const state = await refreshAndBroadcast('notify', { type: 'karaoke_queue', message: `코인노래방 대기 — ${name} ${grade||''}` });
  res.json({ ok: true, position: state.karaokeQueue.length });
});

app.post('/api/karaoke/queue-assign', async (req, res) => {
  const { booth } = req.body;
  if (!booth) return res.status(400).json({ error: 'booth 필요' });
  const { data: queue } = await supabase.from('karaoke_queue').select('*').order('id').limit(1);
  if (!queue?.length) return res.status(400).json({ error: '대기자 없음' });
  const next = queue[0];
  const { data: removed } = await supabase.from('karaoke_queue').delete().eq('id', next.id).select();
  if (!removed || removed.length === 0) return res.status(409).json({ error: '이미 배정 처리된 대기자입니다' });
  const t = nowStr();
  await supabase.from('records').insert({
    date: todayStr(), time: t, name: next.name, phone: next.phone,
    facility: '코인노래방', booth, game: '-', active: true,
    grade: next.grade || '-', gender: next.gender || '-', headcount: next.headcount || 1,
  });
  await refreshAndBroadcast('notify', { type: 'queue_assign', message: `🎤 대기자 ${next.name} → 코인노래방 ${booth} 배정 완료` });
  res.json({ ok: true });
});

app.post('/api/karaoke/queue-remove', async (req, res) => {
  const { id } = req.body;
  await supabase.from('karaoke_queue').delete().eq('id', id);
  await refreshAndBroadcast();
  res.json({ ok: true });
});

// ===================== API: 포켓볼 대기 =====================
app.post('/api/pool/queue', rateLimit(), async (req, res) => {
  let { name, phone, grade } = req.body;
  if (!name || !phone) return res.status(400).json({ error: '필수 항목 누락' });
  name = sanitizeText(name, 30); phone = formatPhone(sanitizeText(phone, 20)); grade = sanitizeText(grade, 20);
  if (!name || !phone) return res.status(400).json({ error: '필수 항목 누락' });
  await supabase.from('pool_queue').insert({ name, phone, time: nowStr(), grade: grade || '-' });
  const state = await refreshAndBroadcast('notify', { type: 'pool_queue', message: `포켓볼 대기 등록 — ${name} ${grade||''}` });
  res.json({ ok: true, position: state.poolQueue.length });
});

app.post('/api/pool/queue-assign', async (req, res) => {
  const { booth } = req.body;
  if (!booth) return res.status(400).json({ error: 'booth 필요' });
  const { data: queue } = await supabase.from('pool_queue').select('*').order('id').limit(1);
  if (!queue?.length) return res.status(400).json({ error: '대기자 없음' });
  const next = queue[0];
  const { data: removed } = await supabase.from('pool_queue').delete().eq('id', next.id).select();
  if (!removed || removed.length === 0) return res.status(409).json({ error: '이미 배정 처리된 대기자입니다' });
  const t = nowStr();
  await supabase.from('records').insert({
    date: todayStr(), time: t, name: next.name, phone: formatPhone(next.phone),
    facility: '포켓볼', booth, game: '-', active: true,
    grade: next.grade || '-', gender: next.gender || '-', headcount: next.headcount || 1,
  });
  await refreshAndBroadcast('notify', { type: 'queue_assign', message: `🎱 대기자 ${next.name} → 포켓볼 ${booth} 배정 완료` });
  res.json({ ok: true });
});

app.post('/api/pool/queue-remove', async (req, res) => {
  const { id } = req.body;
  await supabase.from('pool_queue').delete().eq('id', id);
  await refreshAndBroadcast();
  res.json({ ok: true });
});

// ===================== API: 일반 종료 =====================
app.post('/api/end', async (req, res) => {
  const { facility, booth } = req.body;
  await supabase.from('records').update({ active: false, end_time: nowStr() }).eq('facility', facility).eq('booth', booth).eq('active', true);
  await refreshAndBroadcast();
  res.json({ ok: true });
});

// ===================== PC 전원 상태 =====================
let pcPowerStatus = {
  PC1:{on:false,lastSeen:null},PC2:{on:false,lastSeen:null},
  PC3:{on:false,lastSeen:null},PC4:{on:false,lastSeen:null},
  PC5:{on:false,lastSeen:null},PC6:{on:false,lastSeen:null},
};

app.post('/api/pc/heartbeat', (req, res) => {
  const { pcKey } = req.body;
  if (!pcKey || !pcPowerStatus[pcKey]) return res.status(400).json({ error: '잘못된 PC' });
  pcPowerStatus[pcKey] = { on: true, lastSeen: Date.now() };
  res.json({ ok: true });
});

setInterval(() => {
  let changed = false;
  Object.keys(pcPowerStatus).forEach(k => {
    const s = pcPowerStatus[k];
    if (s.on && s.lastSeen && Date.now() - s.lastSeen > 10000) {
      pcPowerStatus[k].on = false;
      changed = true;
      console.log(`[전원감지] ${k} 꺼짐 감지`);
    }
  });
  if (changed) broadcast('power', pcPowerStatus);
}, 5000);

app.get('/api/pc/power', (req, res) => {
  res.json(pcPowerStatus);
});

// ===================== PC 원격제어 =====================
const PC_CONFIG = {
  PC1: { mac: '10-FF-E0-9A-E3-D9', ip: '192.168.0.121' },
  PC2: { mac: '74-56-3C-EA-52-85', ip: '192.168.0.122' },
  PC3: { mac: '74-56-3C-EF-A0-8D', ip: '192.168.0.123' },
  PC4: { mac: '10-FF-E0-9A-E3-DA', ip: '192.168.0.124' },
  PC5: { mac: '10-FF-E0-4C-19-20', ip: '192.168.0.125' },
  PC6: { mac: '74-56-3C-EA-50-67', ip: '192.168.0.126' },
};

let pcCommands = {};

function setPCCommand(pcKey, cmd) {
  pcCommands[pcKey] = {
    command: cmd,
    retryLeft: 5,
    expireAt: Date.now() + 30000
  };
  console.log(`[명령 등록] ${pcKey}: ${cmd} (5회 재시도, 30초 유효)`);
}

app.get('/api/pc/command/:pcKey', (req, res) => {
  const { pcKey } = req.params;
  const entry = pcCommands[pcKey];
  if (!entry) return res.json({ pcKey, command: null });
  if (Date.now() > entry.expireAt) {
    delete pcCommands[pcKey];
    console.log(`[명령 만료] ${pcKey}`);
    return res.json({ pcKey, command: null });
  }
  entry.retryLeft--;
  console.log(`[명령 전달] ${pcKey}: ${entry.command} (남은 재시도: ${entry.retryLeft})`);
  if (entry.retryLeft <= 0) delete pcCommands[pcKey];
  res.json({ pcKey, command: entry.command });
});

app.get('/api/pc/commands', (req, res) => {
  const commands = { ...pcCommands };
  Object.keys(pcCommands).forEach(k => { pcCommands[k] = null; });
  res.json(commands);
});

// 아래 두 엔드포인트는 관리자 화면의 수동 PC 켜기/끄기 버튼 전용입니다.
// (세션 시작/종료 시 자동 켜기/끄기는 /api/pc/start, /api/pc/end 내부에서 별도로 처리되며 인증이 필요 없습니다)
app.post('/api/pc/wakeup', requireAdmin, async (req, res) => {
  const { pcKey } = req.body;
  if (!PC_CONFIG[pcKey]) return res.status(400).json({ error: '잘못된 PC' });
  setPCCommand(pcKey, 'wakeup');
  broadcast('notify', { type: 'pc_wakeup', message: `💡 ${pcKey} 켜기 명령 전송` });
  res.json({ ok: true, mac: PC_CONFIG[pcKey].mac });
});

app.post('/api/pc/shutdown', requireAdmin, async (req, res) => {
  const { pcKey } = req.body;
  if (!PC_CONFIG[pcKey]) return res.status(400).json({ error: '잘못된 PC' });
  setPCCommand(pcKey, 'shutdown');
  broadcast('notify', { type: 'pc_shutdown', message: `🔴 ${pcKey} 끄기 명령 전송` });
  res.json({ ok: true });
});

// ===================== API: 기록 삭제 (관리자 전용) =====================
app.delete('/api/record/:id', requireAdmin, async (req, res) => {
  const { id } = req.params;
  if (!id) return res.status(400).json({ error: 'id 필요' });
  const { error } = await supabase.from('records').delete().eq('id', id);
  if (error) return res.status(500).json({ error: error.message });
  await refreshAndBroadcast();
  res.json({ ok: true });
});

// ===================== API: 공지사항 =====================
app.get('/api/notices', async (req, res) => {
  const { data } = await supabase.from('notices').select('*').eq('active', true).order('id', { ascending: false });
  res.json(data || []);
});

app.post('/api/notices', requireAdmin, async (req, res) => {
  let { title, content } = req.body;
  if (!title || !content) return res.status(400).json({ error: '제목과 내용을 입력해주세요' });
  title = sanitizeText(title, 60); content = sanitizeText(content, 500);
  if (!title || !content) return res.status(400).json({ error: '제목과 내용을 입력해주세요' });
  const { data, error } = await supabase.from('notices').insert({ title, content, active: true }).select().single();
  if (error) return res.status(500).json({ error: error.message });
  await refreshAndBroadcast('notify', { type: 'notice', message: `📢 새 공지사항: ${title}` });
  res.json({ ok: true, notice: data });
});

app.delete('/api/notices/:id', requireAdmin, async (req, res) => {
  await supabase.from('notices').update({ active: false }).eq('id', req.params.id);
  await refreshAndBroadcast();
  res.json({ ok: true });
});

// ===================== API: 시설 점검 (관리자 전용) =====================
app.post('/api/facility/inspection', requireAdmin, async (req, res) => {
  const { id, inspection, msg } = req.body;
  await supabase.from('facility_status').update({ inspection, inspection_msg: sanitizeText(msg, 100) || '점검 중입니다' }).eq('id', id);
  await refreshAndBroadcast('notify', { type: 'inspection', message: `${inspection ? '🔧 점검 시작' : '✅ 점검 완료'}: ${id}` });
  res.json({ ok: true });
});

// ===================== 서버 시작 =====================
app.listen(PORT, '0.0.0.0', async () => {
  console.log('========================================');
  console.log('  🎮 들락날락 키오스크 서버 v3.2');
  console.log('  광양시청소년문화센터');
  console.log(`  포트: ${PORT}`);
  console.log('  [v3.2] /api/records?date= 엔드포인트 추가 (전체 날짜 조회)');
  console.log('========================================');
  await restoreTimers();
  console.log('  [DB] Supabase 연결 완료');
});
