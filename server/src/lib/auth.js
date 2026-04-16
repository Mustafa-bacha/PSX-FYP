import jwt from 'jsonwebtoken';
import bcrypt from 'bcryptjs';
import { config } from '../config.js';

export function signAccessToken(user) {
  return jwt.sign(
    {
      sub: String(user.id),
      email: user.email,
      name: user.full_name || ''
    },
    config.jwtSecret,
    { expiresIn: `${Math.max(1, config.jwtExpiresHours)}h` }
  );
}

export function verifyAccessToken(token) {
  return jwt.verify(token, config.jwtSecret);
}

export async function hashPassword(password) {
  return bcrypt.hash(password, 12);
}

export async function verifyPassword(password, hash) {
  return bcrypt.compare(password, hash);
}

export function normalizeEmail(value) {
  return String(value || '').trim().toLowerCase();
}

export function validatePasswordStrength(password) {
  const raw = String(password || '');
  if (raw.length < 8) {
    return 'Password must be at least 8 characters.';
  }
  if (!/[A-Z]/.test(raw) || !/[a-z]/.test(raw) || !/[0-9]/.test(raw)) {
    return 'Password must include uppercase, lowercase, and a number.';
  }
  return '';
}
