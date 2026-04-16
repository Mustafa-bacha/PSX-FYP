import { Router } from 'express';
import { db } from '../lib/db.js';
import {
  hashPassword,
  normalizeEmail,
  signAccessToken,
  validatePasswordStrength,
  verifyPassword
} from '../lib/auth.js';
import { requireAuth } from '../middleware/authMiddleware.js';

export const authRouter = Router();

const ADMIN_EMAIL = normalizeEmail('hassan33@gmail.com');
const ADMIN_PASSWORD = 'stockfull';
const ADMIN_DEFAULT_NAME = 'Admin User';

function sanitizeUser(user) {
  if (!user) return null;
  return {
    id: user.id,
    email: user.email,
    full_name: user.full_name || '',
    date_of_birth: user.date_of_birth || null,
    provider: user.provider,
    avatar_url: user.avatar_url || null,
    created_at: user.created_at,
    updated_at: user.updated_at,
    last_login_at: user.last_login_at || null
  };
}

function getUserByEmail(email) {
  return db.prepare('SELECT * FROM users WHERE email = ?').get(email);
}

function getUserById(id) {
  return db.prepare('SELECT * FROM users WHERE id = ?').get(Number(id));
}

function parseDateOnly(value) {
  const raw = String(value || '').trim();
  if (!raw) return '';
  if (!/^\d{4}-\d{2}-\d{2}$/.test(raw)) return '';
  const dt = new Date(`${raw}T00:00:00Z`);
  if (Number.isNaN(dt.getTime())) return '';
  const today = new Date();
  const todayDateOnly = new Date(Date.UTC(today.getUTCFullYear(), today.getUTCMonth(), today.getUTCDate()));
  if (dt > todayDateOnly) return '';
  return raw;
}

async function ensureHardcodedAdminUser() {
  let user = getUserByEmail(ADMIN_EMAIL);
  if (!user) {
    const passwordHash = await hashPassword(ADMIN_PASSWORD);
    const insert = db.prepare(`
      INSERT INTO users (
        email,
        password_hash,
        full_name,
        date_of_birth,
        provider,
        created_at,
        updated_at,
        last_login_at
      )
      VALUES (?, ?, ?, NULL, 'admin', datetime('now'), datetime('now'), datetime('now'))
    `).run(ADMIN_EMAIL, passwordHash, ADMIN_DEFAULT_NAME);
    user = getUserById(insert.lastInsertRowid);
  }

  if (!user.password_hash || user.provider !== 'admin') {
    const passwordHash = await hashPassword(ADMIN_PASSWORD);
    db.prepare(`
      UPDATE users
      SET password_hash = ?,
          provider = 'admin',
          full_name = COALESCE(NULLIF(full_name, ''), ?),
          updated_at = datetime('now')
      WHERE id = ?
    `).run(passwordHash, ADMIN_DEFAULT_NAME, user.id);
    user = getUserById(user.id);
  }

  return user;
}

authRouter.post('/signup', async (req, res) => {
  try {
    const email = normalizeEmail(req.body?.email);
    const password = String(req.body?.password || '');
    const confirmPassword = String(req.body?.confirm_password || '');
    const fullName = String(req.body?.full_name || '').trim();
    const dateOfBirth = parseDateOnly(req.body?.date_of_birth);

    if (!email) return res.status(400).json({ error: 'Email is required' });
    if (!fullName) return res.status(400).json({ error: 'Full name is required' });
    if (!dateOfBirth) return res.status(400).json({ error: 'A valid date of birth is required' });
    if (!password) return res.status(400).json({ error: 'Password is required' });
    if (!confirmPassword) return res.status(400).json({ error: 'Confirm password is required' });
    if (password !== confirmPassword) return res.status(400).json({ error: 'Passwords do not match' });

    const strengthError = validatePasswordStrength(password);
    if (strengthError) return res.status(400).json({ error: strengthError });

    const existing = getUserByEmail(email);
    if (existing) return res.status(409).json({ error: 'Email already registered' });

    const passwordHash = await hashPassword(password);
    const insert = db.prepare(`
      INSERT INTO users (
        email,
        password_hash,
        full_name,
        date_of_birth,
        provider,
        created_at,
        updated_at,
        last_login_at
      )
      VALUES (?, ?, ?, ?, 'local', datetime('now'), datetime('now'), datetime('now'))
    `).run(email, passwordHash, fullName, dateOfBirth);

    const user = getUserById(insert.lastInsertRowid);
    const token = signAccessToken(user);

    return res.status(201).json({
      token,
      user: sanitizeUser(user)
    });
  } catch (err) {
    return res.status(500).json({ error: String(err?.message || err) });
  }
});

authRouter.post('/login', async (req, res) => {
  try {
    const email = normalizeEmail(req.body?.email);
    const password = String(req.body?.password || '');
    if (!email || !password) {
      return res.status(400).json({ error: 'Email and password are required' });
    }

    if (email === ADMIN_EMAIL && password === ADMIN_PASSWORD) {
      const adminUser = await ensureHardcodedAdminUser();
      db.prepare(`UPDATE users SET last_login_at = datetime('now'), updated_at = datetime('now') WHERE id = ?`).run(adminUser.id);
      const refreshedAdmin = getUserById(adminUser.id);
      return res.json({
        token: signAccessToken(refreshedAdmin),
        user: sanitizeUser(refreshedAdmin)
      });
    }

    const user = getUserByEmail(email);
    if (!user || !user.password_hash) {
      return res.status(401).json({ error: 'Invalid credentials' });
    }

    const ok = await verifyPassword(password, user.password_hash);
    if (!ok) return res.status(401).json({ error: 'Invalid credentials' });

    db.prepare(`UPDATE users SET last_login_at = datetime('now'), updated_at = datetime('now') WHERE id = ?`).run(user.id);
    const refreshed = getUserById(user.id);

    return res.json({
      token: signAccessToken(refreshed),
      user: sanitizeUser(refreshed)
    });
  } catch (err) {
    return res.status(500).json({ error: String(err?.message || err) });
  }
});

authRouter.get('/me', requireAuth, (req, res) => {
  res.json({ user: sanitizeUser(req.user) });
});
