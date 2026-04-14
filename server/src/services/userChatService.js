import { db } from '../lib/db.js';

export function listUserChatHistory(userId, stock, limit = 12) {
  return db.prepare(`
    SELECT role, content, created_at
    FROM user_chat_messages
    WHERE user_id = ? AND stock_symbol = ?
    ORDER BY id DESC
    LIMIT ?
  `).all(Number(userId), String(stock || '').toUpperCase(), Math.max(1, Number(limit || 12))).reverse()
    .map((m) => ({
      role: m.role,
      content: m.content,
      created_at: m.created_at
    }));
}

export function addUserChatMessage(userId, stock, role, content) {
  if (!userId || !stock || !role || !content) return;
  db.prepare(`
    INSERT INTO user_chat_messages (user_id, stock_symbol, role, content)
    VALUES (?, ?, ?, ?)
  `).run(Number(userId), String(stock || '').toUpperCase(), String(role), String(content));
}

export function clearUserChatHistory(userId, stock) {
  if (!userId || !stock) return 0;
  const result = db.prepare(`
    DELETE FROM user_chat_messages
    WHERE user_id = ? AND stock_symbol = ?
  `).run(Number(userId), String(stock || '').toUpperCase());
  return Number(result?.changes || 0);
}
