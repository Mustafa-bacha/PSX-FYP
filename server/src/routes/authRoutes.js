import { Router } from 'express';
import axios from 'axios';
import { db } from '../lib/db.js';
import {
  hashPassword,
  normalizeEmail,
  signAccessToken,
  validatePasswordStrength,
  verifyPassword
} from '../lib/auth.js';
import { requireAuth } from '../middleware/authMiddleware.js';
import { config } from '../config.js';

export const authRouter = Router();

function sanitizeUser(user) {
  if (!user) return null;
  return {
    id: user.id,
    email: user.email,
    full_name: user.full_name || '',
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

function getUserByGoogleId(googleId) {
  return db.prepare('SELECT * FROM users WHERE google_id = ?').get(googleId);
}

function getUserById(id) {
  return db.prepare('SELECT * FROM users WHERE id = ?').get(Number(id));
}

authRouter.post('/signup', async (req, res) => {
  try {
    const email = normalizeEmail(req.body?.email);
    const password = String(req.body?.password || '');
    const fullName = String(req.body?.full_name || '').trim();

    if (!email) return res.status(400).json({ error: 'Email is required' });
    if (!password) return res.status(400).json({ error: 'Password is required' });

    const strengthError = validatePasswordStrength(password);
    if (strengthError) return res.status(400).json({ error: strengthError });

    const existing = getUserByEmail(email);
    if (existing) return res.status(409).json({ error: 'Email already registered' });

    const passwordHash = await hashPassword(password);
    const insert = db.prepare(`
      INSERT INTO users (email, password_hash, full_name, provider, created_at, updated_at, last_login_at)
      VALUES (?, ?, ?, 'local', datetime('now'), datetime('now'), datetime('now'))
    `).run(email, passwordHash, fullName || null);

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

authRouter.get('/google/start', (req, res) => {
  if (!config.googleClientId || !config.googleClientSecret) {
    return res.status(501).json({ error: 'Google OAuth is not configured on server' });
  }

  const params = new URLSearchParams({
    client_id: config.googleClientId,
    redirect_uri: config.googleRedirectUri,
    response_type: 'code',
    scope: 'openid email profile',
    access_type: 'offline',
    prompt: 'consent'
  });

  return res.redirect(`https://accounts.google.com/o/oauth2/v2/auth?${params.toString()}`);
});

authRouter.get('/google/callback', async (req, res) => {
  try {
    if (!config.googleClientId || !config.googleClientSecret) {
      return res.status(501).json({ error: 'Google OAuth is not configured on server' });
    }

    const code = String(req.query.code || '');
    if (!code) return res.status(400).json({ error: 'Missing OAuth code' });

    const tokenResp = await axios.post(
      'https://oauth2.googleapis.com/token',
      {
        code,
        client_id: config.googleClientId,
        client_secret: config.googleClientSecret,
        redirect_uri: config.googleRedirectUri,
        grant_type: 'authorization_code'
      },
      { timeout: 20000 }
    );

    const idToken = tokenResp.data?.id_token;
    if (!idToken) {
      return res.status(401).json({ error: 'Google OAuth did not return id_token' });
    }

    const tokenInfo = await axios.get('https://oauth2.googleapis.com/tokeninfo', {
      params: { id_token: idToken },
      timeout: 20000
    });

    const googleUser = tokenInfo.data || {};
    if (googleUser.aud !== config.googleClientId) {
      return res.status(401).json({ error: 'Google token audience mismatch' });
    }

    const email = normalizeEmail(googleUser.email);
    const googleId = String(googleUser.sub || '');
    const fullName = String(googleUser.name || '').trim();
    const avatarUrl = String(googleUser.picture || '').trim() || null;

    if (!email || !googleId) {
      return res.status(400).json({ error: 'Google account email or id missing' });
    }

    let user = getUserByGoogleId(googleId) || getUserByEmail(email);

    if (!user) {
      const insert = db.prepare(`
        INSERT INTO users (email, full_name, google_id, avatar_url, provider, created_at, updated_at, last_login_at)
        VALUES (?, ?, ?, ?, 'google', datetime('now'), datetime('now'), datetime('now'))
      `).run(email, fullName || null, googleId, avatarUrl);
      user = getUserById(insert.lastInsertRowid);
    } else {
      db.prepare(`
        UPDATE users
        SET google_id = COALESCE(google_id, ?),
            full_name = COALESCE(?, full_name),
            avatar_url = COALESCE(?, avatar_url),
            provider = CASE WHEN provider = 'local' THEN provider ELSE 'google' END,
            last_login_at = datetime('now'),
            updated_at = datetime('now')
        WHERE id = ?
      `).run(googleId, fullName || null, avatarUrl, user.id);
      user = getUserById(user.id);
    }

    const token = signAccessToken(user);
    const sanitized = sanitizeUser(user);
    const redirectUrl = new URL('/auth/callback', config.frontendUrl);
    redirectUrl.searchParams.set('token', token);
    redirectUrl.searchParams.set('email', sanitized.email || '');
    redirectUrl.searchParams.set('name', sanitized.full_name || '');

    return res.redirect(redirectUrl.toString());
  } catch (err) {
    const redirectUrl = new URL('/auth/callback', config.frontendUrl);
    redirectUrl.searchParams.set('error', 'google_auth_failed');
    redirectUrl.searchParams.set('message', String(err?.message || err));
    return res.redirect(redirectUrl.toString());
   }
 });
