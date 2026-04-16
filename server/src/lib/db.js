import Database from 'better-sqlite3';
import { config } from '../config.js';

export const db = new Database(config.dbPath);

export function initDb() {
	db.exec(`
		CREATE TABLE IF NOT EXISTS stocks (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			symbol TEXT NOT NULL,
			ldcp REAL,
			open REAL,
			high REAL,
			low REAL,
			close REAL,
			change REAL,
			change_pct REAL,
			volume REAL,
			date DATE NOT NULL,
			timestamp DATETIME,
			UNIQUE(symbol, date)
		);

		CREATE INDEX IF NOT EXISTS idx_stocks_symbol_date
			ON stocks(symbol, date DESC);

		CREATE TABLE IF NOT EXISTS sentiment (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			symbol TEXT NOT NULL,
			score REAL,
			label TEXT,
			source TEXT,
			headline TEXT,
			analyzed_at DATETIME DEFAULT CURRENT_TIMESTAMP
		);

		CREATE INDEX IF NOT EXISTS idx_sentiment_symbol_analyzed_at
			ON sentiment(symbol, analyzed_at DESC);

		CREATE TABLE IF NOT EXISTS predictions (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			symbol TEXT NOT NULL,
			predicted_price REAL,
			predicted_direction TEXT,
			confidence REAL,
			prediction_date DATE,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP
		);

		CREATE TABLE IF NOT EXISTS users (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			email TEXT NOT NULL UNIQUE,
			password_hash TEXT,
			full_name TEXT,
			google_id TEXT UNIQUE,
			avatar_url TEXT,
			provider TEXT NOT NULL DEFAULT 'local',
			created_at TEXT NOT NULL DEFAULT (datetime('now')),
			updated_at TEXT NOT NULL DEFAULT (datetime('now')),
			last_login_at TEXT
		);

		CREATE TABLE IF NOT EXISTS user_chat_messages (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			user_id INTEGER NOT NULL,
			stock_symbol TEXT NOT NULL,
			role TEXT NOT NULL,
			content TEXT NOT NULL,
			created_at TEXT NOT NULL DEFAULT (datetime('now')),
			FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
		);

		CREATE INDEX IF NOT EXISTS idx_user_chat_user_symbol_created
			ON user_chat_messages(user_id, stock_symbol, created_at DESC);

			CREATE TABLE IF NOT EXISTS user_watchlist (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				user_id INTEGER NOT NULL,
				stock_symbol TEXT NOT NULL,
				created_at TEXT NOT NULL DEFAULT (datetime('now')),
				FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
				UNIQUE(user_id, stock_symbol)
			);

			CREATE INDEX IF NOT EXISTS idx_user_watchlist_user_symbol
				ON user_watchlist(user_id, stock_symbol);

			CREATE TABLE IF NOT EXISTS user_sim_accounts (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				user_id INTEGER NOT NULL UNIQUE,
				initial_cash REAL NOT NULL DEFAULT 1000000,
				cash_balance REAL NOT NULL DEFAULT 1000000,
				created_at TEXT NOT NULL DEFAULT (datetime('now')),
				updated_at TEXT NOT NULL DEFAULT (datetime('now')),
				FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
			);

			CREATE TABLE IF NOT EXISTS user_sim_positions (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				user_id INTEGER NOT NULL,
				symbol TEXT NOT NULL,
				quantity REAL NOT NULL DEFAULT 0,
				avg_cost REAL NOT NULL DEFAULT 0,
				realized_pnl REAL NOT NULL DEFAULT 0,
				updated_at TEXT NOT NULL DEFAULT (datetime('now')),
				FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
				UNIQUE(user_id, symbol)
			);

			CREATE INDEX IF NOT EXISTS idx_user_sim_positions_user_symbol
				ON user_sim_positions(user_id, symbol);

			CREATE TABLE IF NOT EXISTS user_sim_trades (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				user_id INTEGER NOT NULL,
				symbol TEXT NOT NULL,
				side TEXT NOT NULL,
				quantity REAL NOT NULL,
				price REAL NOT NULL,
				notional REAL NOT NULL,
				realized_pnl REAL NOT NULL DEFAULT 0,
				created_at TEXT NOT NULL DEFAULT (datetime('now')),
				FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
			);

			CREATE INDEX IF NOT EXISTS idx_user_sim_trades_user_created
				ON user_sim_trades(user_id, created_at DESC);

			CREATE TABLE IF NOT EXISTS user_sim_orders (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				user_id INTEGER NOT NULL,
				symbol TEXT NOT NULL,
				side TEXT NOT NULL,
				order_type TEXT NOT NULL,
				quantity REAL NOT NULL,
				limit_price REAL,
				status TEXT NOT NULL DEFAULT 'PENDING',
				filled_price REAL,
				note TEXT,
				created_at TEXT NOT NULL DEFAULT (datetime('now')),
				updated_at TEXT NOT NULL DEFAULT (datetime('now')),
				filled_at TEXT,
				FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
			);

			CREATE INDEX IF NOT EXISTS idx_user_sim_orders_user_status
				ON user_sim_orders(user_id, status, created_at DESC);

			CREATE TABLE IF NOT EXISTS user_sim_equity_snapshots (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				user_id INTEGER NOT NULL,
				equity REAL NOT NULL,
				created_at TEXT NOT NULL DEFAULT (datetime('now')),
				FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
			);

			CREATE INDEX IF NOT EXISTS idx_user_sim_equity_user_created
				ON user_sim_equity_snapshots(user_id, created_at DESC);
	`);
}
