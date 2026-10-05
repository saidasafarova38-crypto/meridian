// Shared seating-plan storage in Redis. Works with either Vercel storage integration:
//   - "Redis" (Redis Cloud)   -> REDIS_URL, TCP connection
//   - "Upstash for Redis"     -> KV_REST_API_URL/TOKEN or UPSTASH_REDIS_REST_URL/TOKEN, HTTP
// The whole plan is one Redis hash:
//   meta:<tableId>        -> table JSON {num, shape, cap, x, y}
//   name:<tableId>:<seat> -> guest name
// One field per seat means two people filling different seats never overwrite each other.
const KEY = 'meridian:plan';
const FIELD = /^(meta:\d{1,4}|name:\d{1,4}:\d{1,2})$/;
const MAX_FIELDS = 3000;
const MAX_VALUE = 500;

// Vercel lets you pick a custom prefix when connecting a store (e.g. STORAGE_REDIS_URL),
// so match on the suffix rather than the exact name.
function env(...suffixes) {
  for (const s of suffixes) {
    if (process.env[s]) return process.env[s];
    const k = Object.keys(process.env).find((n) => n.endsWith('_' + s) && process.env[n]);
    if (k) return process.env[k];
  }
  return undefined;
}
const REST_URL = env('KV_REST_API_URL', 'UPSTASH_REDIS_REST_URL');
const REST_TOKEN = env('KV_REST_API_TOKEN', 'UPSTASH_REDIS_REST_TOKEN');
const REDIS_URL = env('REDIS_URL', 'KV_URL');

// ---------- Upstash REST ----------
async function rest(path, body) {
  const r = await fetch(REST_URL + path, {
    method: 'POST',
    headers: { Authorization: 'Bearer ' + REST_TOKEN, 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  const data = await r.json();
  if (!r.ok || data.error) throw new Error(data.error || 'redis ' + r.status);
  return data;
}
const restStore = {
  async read() {
    const { result } = await rest('', ['HGETALL', KEY]);
    const fields = {};
    for (let i = 0; i < result.length; i += 2) fields[result[i]] = result[i + 1];
    return fields;
  },
  async write({ replace, del, set }) {
    const cmds = [];
    if (replace) cmds.push(['DEL', KEY]);
    if (del.length) cmds.push(['HDEL', KEY, ...del]);
    const keys = Object.keys(set);
    if (keys.length) cmds.push(['HSET', KEY, ...keys.flatMap((k) => [k, set[k]])]);
    if (!cmds.length) return;
    const out = await rest('/multi-exec', cmds);
    const failed = Array.isArray(out) && out.find((o) => o && o.error);
    if (failed) throw new Error(failed.error);
  },
};

// ---------- TCP (node-redis), connection reused across warm invocations ----------
let clientPromise = null;
function tcpClient() {
  if (!clientPromise) {
    const { createClient } = require('redis');
    const client = createClient({ url: REDIS_URL, socket: { connectTimeout: 5000 } });
    client.on('error', () => { clientPromise = null; });
    clientPromise = client.connect().then(() => client, (e) => { clientPromise = null; throw e; });
  }
  return clientPromise;
}
const tcpStore = {
  async read() {
    return (await tcpClient()).hGetAll(KEY);
  },
  async write({ replace, del, set }) {
    const tx = (await tcpClient()).multi();
    if (replace) tx.del(KEY);
    if (del.length) tx.hDel(KEY, del);
    if (Object.keys(set).length) tx.hSet(KEY, set);
    await tx.exec();
  },
};

const store = REST_URL && REST_TOKEN ? restStore : REDIS_URL ? tcpStore : null;

module.exports = async (req, res) => {
  res.setHeader('Cache-Control', 'no-store');
  if (!store) {
    // Names only (never values), to tell "store not connected" from "connected under another name".
    const seen = Object.keys(process.env).filter((n) => /REDIS|UPSTASH|KV_/.test(n));
    return res.status(503).json({ error: 'storage not configured', env: seen, vercelEnv: process.env.VERCEL_ENV || null });
  }

  try {
    if (req.method === 'GET') {
      return res.status(200).json({ fields: await store.read() });
    }

    if (req.method === 'POST') {
      const body = typeof req.body === 'string' ? JSON.parse(req.body) : req.body || {};
      const set = body.set && typeof body.set === 'object' ? body.set : {};
      const del = Array.isArray(body.del) ? body.del : [];
      const keys = Object.keys(set);
      if (keys.length + del.length > MAX_FIELDS) return res.status(413).json({ error: 'too many fields' });
      for (const f of keys.concat(del)) {
        if (typeof f !== 'string' || !FIELD.test(f)) return res.status(400).json({ error: 'bad field' });
      }
      for (const f of keys) {
        if (typeof set[f] !== 'string' || set[f].length > MAX_VALUE) return res.status(400).json({ error: 'bad value' });
      }
      await store.write({ replace: body.replace === true, del, set });
      return res.status(200).json({ ok: true });
    }

    res.setHeader('Allow', 'GET, POST');
    return res.status(405).json({ error: 'method not allowed' });
  } catch (e) {
    return res.status(502).json({ error: String((e && e.message) || e) });
  }
};
