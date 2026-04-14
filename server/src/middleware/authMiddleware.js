import { verifyAccessToken } from '../lib/auth.js';
import { db } from '../lib/db.js';

function findUserById(id) {
  return db.prepare(`
    SELECT id, email, full_name, provider, created_at, updated_at, last_login_at, avatar_url
    FROM users
    WHERE id = ?
  `).get(Number(id));
}

function readBearerToken(req) {
  const header = req.headers.authorization || '';
  if (!header.startsWith('Bearer ')) return '';
  return header.slice('Bearer '.length).trim();
}

export function requireAuth(req, res, next) {
  const token = readBearerToken(req);
  if (!token) return res.status(401).json({ error: 'Authentication required' });

  try {
    const payload = verifyAccessToken(token);
    const user = findUserById(payload.sub);
    if (!user) return res.status(401).json({ error: 'User no longer exists' });
    req.user = user;
    req.auth = payload;
    return next();
  } catch {
    return res.status(401).json({ error: 'Invalid or expired token' });
  }
}

export function optionalAuth(req, _res, next) {
  const token = readBearerToken(req);
  if (!token) return next();

  try {
    const payload = verifyAccessToken(token);
    const user = findUserById(payload.sub);
    if (user) {
      req.user = user;
      req.auth = payload;
    }
  } catch {
    // ignore and continue as guest
  }

  return next();
}
