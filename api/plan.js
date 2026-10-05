// Shared seating-plan storage on Upstash Redis (Vercel Marketplace → Storage → Upstash for Redis).
// The whole plan is one Redis hash:
//   meta:<tableId>        -> table JSON {num, shape, cap, x, y}
//   name:<tableId>:<seat> -> guest name
// One field per seat means two people filling different seats never overwrite each other.
const KEY = 'meridian:plan';
const FIELD = /^(meta:\d{1,4}|name:\d{1,4}:\d{1,2})$/;
const MAX_FIELDS = 3000;
const MAX_VALUE = 500;

const BASE = process.env.KV_REST_API_URL || process.env.UPSTASH_REDIS_REST_URL;
const TOKEN = process.env.KV_REST_API_TOKEN || process.env.UPSTASH_REDIS_REST_TOKEN;

async function redis(path, body) {
  const r = await fetch(BASE + path, {
    method: 'POST',
    headers: { Authorization: 'Bearer ' + TOKEN, 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  const data = await r.json();
  if (!r.ok || data.error) throw new Error(data.error || 'redis ' + r.status);
  return data;
}

module.exports = async (req, res) => {
  res.setHeader('Cache-Control', 'no-store');
  if (!BASE || !TOKEN) return res.status(503).json({ error: 'storage not configured' });

  try {
    if (req.method === 'GET') {
      const { result } = await redis('', ['HGETALL', KEY]);
      const fields = {};
      for (let i = 0; i < result.length; i += 2) fields[result[i]] = result[i + 1];
      return res.status(200).json({ fields });
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

      const cmds = [];
      if (body.replace === true) cmds.push(['DEL', KEY]);
      if (del.length) cmds.push(['HDEL', KEY, ...del]);
      if (keys.length) cmds.push(['HSET', KEY, ...keys.flatMap((k) => [k, set[k]])]);
      if (cmds.length) {
        const out = await redis('/multi-exec', cmds);
        const failed = Array.isArray(out) && out.find((o) => o && o.error);
        if (failed) throw new Error(failed.error);
      }
      return res.status(200).json({ ok: true });
    }

    res.setHeader('Allow', 'GET, POST');
    return res.status(405).json({ error: 'method not allowed' });
  } catch (e) {
    return res.status(502).json({ error: String((e && e.message) || e) });
  }
};
