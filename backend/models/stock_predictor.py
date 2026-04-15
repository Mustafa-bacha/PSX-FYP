from __future__ import annotations

import json
import os
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd
import torch
import torch.nn as nn
from sklearn.metrics import accuracy_score, mean_absolute_error, mean_squared_error
from sklearn.preprocessing import StandardScaler

from config import DB_PATH, MODEL_DIR
from services.data_pipeline import compute_technical_indicators, get_all_symbols


FEATURE_COLUMNS = [
    "CLOSE_lag_1",
    "CLOSE_lag_2",
    "CLOSE_lag_3",
    "CLOSE_lag_5",
    "CLOSE_roll_mean_5",
    "CLOSE_roll_std_5",
    "CLOSE_roll_mean_10",
    "VOLUME_ratio",
    "MA_7",
    "MA_20",
    "MA_50",
    "RSI_14",
    "MACD",
    "MACD_signal",
    "BB_upper",
    "BB_lower",
    "BB_mid",
    "day_of_week",
    "month",
]

LOOKBACK_WINDOW = int(os.getenv("PREDICT_LOOKBACK_WINDOW", "30"))
HIDDEN_SIZE = int(os.getenv("PREDICT_LSTM_HIDDEN_SIZE", "64"))
NUM_LAYERS = int(os.getenv("PREDICT_LSTM_NUM_LAYERS", "2"))
DROPOUT = float(os.getenv("PREDICT_LSTM_DROPOUT", "0.2"))
MAX_EPOCHS = int(os.getenv("PREDICT_LSTM_EPOCHS", "12"))
LEARNING_RATE = float(os.getenv("PREDICT_LSTM_LR", "0.001"))
BATCH_SIZE = int(os.getenv("PREDICT_LSTM_BATCH", "32"))
PATIENCE = int(os.getenv("PREDICT_LSTM_PATIENCE", "4"))
CLASS_LOSS_WEIGHT = float(os.getenv("PREDICT_DIRECTION_WEIGHT", "0.4"))
TRAIN_SEED = int(os.getenv("PREDICT_TRAIN_SEED", "42"))
MODEL_VARIANT = os.getenv("PREDICT_MODEL_VARIANT", "lstm").strip().lower()
DEVICE = torch.device("cuda" if torch.cuda.is_available() else "cpu")


@dataclass(frozen=True)
class TrainingConfig:
    lookback: int
    hidden_size: int
    num_layers: int
    dropout: float
    max_epochs: int
    learning_rate: float
    batch_size: int
    patience: int
    class_loss_weight: float
    seed: int
    model_variant: str


def _normalize_variant(value: str) -> str:
    variant = str(value or "lstm").strip().lower()
    if variant not in {"lstm", "bilstm", "gru"}:
        return "lstm"
    return variant


def _runtime_config(overrides: dict[str, Any] | None = None) -> TrainingConfig:
    payload = dict(overrides or {})
    return TrainingConfig(
        lookback=int(payload.get("lookback", LOOKBACK_WINDOW)),
        hidden_size=int(payload.get("hidden_size", HIDDEN_SIZE)),
        num_layers=int(payload.get("num_layers", NUM_LAYERS)),
        dropout=float(payload.get("dropout", DROPOUT)),
        max_epochs=int(payload.get("max_epochs", MAX_EPOCHS)),
        learning_rate=float(payload.get("learning_rate", LEARNING_RATE)),
        batch_size=int(payload.get("batch_size", BATCH_SIZE)),
        patience=int(payload.get("patience", PATIENCE)),
        class_loss_weight=float(payload.get("class_loss_weight", CLASS_LOSS_WEIGHT)),
        seed=int(payload.get("seed", TRAIN_SEED)),
        model_variant=_normalize_variant(str(payload.get("model_variant", MODEL_VARIANT))),
    )


class SequenceHybridModel(nn.Module):
    def __init__(
        self,
        input_size: int,
        hidden_size: int = 64,
        num_layers: int = 2,
        dropout: float = 0.2,
        model_variant: str = "lstm",
    ) -> None:
        super().__init__()
        effective_dropout = dropout if num_layers > 1 else 0.0
        variant = _normalize_variant(model_variant)
        self.model_variant = variant
        self.is_bidirectional = variant == "bilstm"

        recurrent_cls = nn.GRU if variant == "gru" else nn.LSTM
        self.recurrent = recurrent_cls(
            input_size=input_size,
            hidden_size=hidden_size,
            num_layers=num_layers,
            dropout=effective_dropout,
            batch_first=True,
            bidirectional=self.is_bidirectional,
        )
        recurrent_out_size = hidden_size * (2 if self.is_bidirectional else 1)
        self.shared = nn.Sequential(
            nn.Linear(recurrent_out_size, hidden_size),
            nn.ReLU(),
            nn.Dropout(dropout),
        )
        self.price_head = nn.Linear(hidden_size, 1)
        self.direction_head = nn.Linear(hidden_size, 2)

    def forward(self, x: torch.Tensor) -> tuple[torch.Tensor, torch.Tensor]:
        out, _ = self.recurrent(x)
        last_step = out[:, -1, :]
        shared_repr = self.shared(last_step)
        price_pred = self.price_head(shared_repr).squeeze(-1)
        direction_logits = self.direction_head(shared_repr)
        return price_pred, direction_logits


def _model_paths(symbol: str) -> dict[str, Path]:
    sym = symbol.strip().upper()
    return {
        "price": MODEL_DIR / f"{sym}_price.pt",
        "direction": MODEL_DIR / f"{sym}_direction.pt",
        "scaler": MODEL_DIR / f"{sym}_scaler.pkl",
        "meta": MODEL_DIR / f"{sym}_metrics.json",
    }


def _load_symbol_df(symbol: str) -> pd.DataFrame:
    symbol = symbol.strip().upper()
    conn = sqlite3.connect(DB_PATH)
    try:
        df = pd.read_sql_query(
            """
            SELECT symbol, date, open, high, low, close, volume, change, change_pct, ldcp, timestamp
            FROM stocks
            WHERE symbol = ?
            ORDER BY date ASC
            """,
            conn,
            params=(symbol,),
        )
    finally:
        conn.close()

    if df.empty:
        return df
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.sort_values("date").reset_index(drop=True)
    return df


def _build_features(df: pd.DataFrame, include_targets: bool = True) -> pd.DataFrame:
    if df.empty:
        return df.copy()

    work = compute_technical_indicators(df.copy())
    work["close"] = pd.to_numeric(work["close"], errors="coerce")
    work["volume"] = pd.to_numeric(work["volume"], errors="coerce")

    work["CLOSE_lag_1"] = work["close"].shift(1)
    work["CLOSE_lag_2"] = work["close"].shift(2)
    work["CLOSE_lag_3"] = work["close"].shift(3)
    work["CLOSE_lag_5"] = work["close"].shift(5)

    work["CLOSE_roll_mean_5"] = work["close"].rolling(5).mean()
    work["CLOSE_roll_std_5"] = work["close"].rolling(5).std(ddof=0)
    work["CLOSE_roll_mean_10"] = work["close"].rolling(10).mean()

    vol_mean_5 = work["volume"].rolling(5).mean()
    work["VOLUME_ratio"] = work["volume"] / vol_mean_5.replace(0, np.nan)

    work["day_of_week"] = pd.to_datetime(work["date"]).dt.dayofweek
    work["month"] = pd.to_datetime(work["date"]).dt.month

    if include_targets:
        work["target_price"] = work["close"].shift(-1)
        work["target_direction"] = (work["target_price"] > work["close"]).astype(int)

    for col in FEATURE_COLUMNS:
        work[col] = pd.to_numeric(work[col], errors="coerce")

    required_cols = FEATURE_COLUMNS + (["target_price"] if include_targets else [])
    work = work.dropna(subset=required_cols)
    return work


def _make_sequences(
    features: np.ndarray,
    target_price: np.ndarray,
    target_direction: np.ndarray,
    lookback: int,
    start_idx: int,
    end_idx: int,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    x_list: list[np.ndarray] = []
    y_price_list: list[float] = []
    y_dir_list: list[int] = []

    begin = max(lookback, start_idx)
    for idx in range(begin, end_idx):
        x_list.append(features[idx - lookback : idx])
        y_price_list.append(float(target_price[idx]))
        y_dir_list.append(int(target_direction[idx]))

    if not x_list:
        return (
            np.empty((0, lookback, features.shape[1]), dtype=np.float32),
            np.empty((0,), dtype=np.float32),
            np.empty((0,), dtype=np.int64),
        )

    return (
        np.asarray(x_list, dtype=np.float32),
        np.asarray(y_price_list, dtype=np.float32),
        np.asarray(y_dir_list, dtype=np.int64),
    )


def _train_hybrid_model(
    x_train: np.ndarray,
    y_train_price: np.ndarray,
    y_train_direction: np.ndarray,
    x_val: np.ndarray,
    y_val_price: np.ndarray,
    y_val_direction: np.ndarray,
    cfg: TrainingConfig,
) -> SequenceHybridModel:
    model = SequenceHybridModel(
        input_size=x_train.shape[-1],
        hidden_size=cfg.hidden_size,
        num_layers=cfg.num_layers,
        dropout=cfg.dropout,
        model_variant=cfg.model_variant,
    ).to(DEVICE)

    optimizer = torch.optim.AdamW(model.parameters(), lr=cfg.learning_rate, weight_decay=1e-4)
    price_criterion = nn.SmoothL1Loss()
    class_counts = np.bincount(y_train_direction.astype(np.int64), minlength=2).astype(np.float32)
    class_counts = np.maximum(class_counts, 1.0)
    class_weights = class_counts.sum() / (2.0 * class_counts)
    class_weights_t = torch.tensor(class_weights, dtype=torch.float32, device=DEVICE)
    direction_criterion = nn.CrossEntropyLoss(weight=class_weights_t)

    x_train_t = torch.tensor(x_train, dtype=torch.float32, device=DEVICE)
    y_train_price_t = torch.tensor(y_train_price, dtype=torch.float32, device=DEVICE)
    y_train_direction_t = torch.tensor(y_train_direction, dtype=torch.long, device=DEVICE)

    x_val_t = torch.tensor(x_val, dtype=torch.float32, device=DEVICE)
    y_val_price_t = torch.tensor(y_val_price, dtype=torch.float32, device=DEVICE)
    y_val_direction_t = torch.tensor(y_val_direction, dtype=torch.long, device=DEVICE)

    best_state: dict[str, torch.Tensor] | None = None
    best_val_loss = float("inf")
    stale_epochs = 0

    for _ in range(cfg.max_epochs):
        model.train()
        indices = np.random.permutation(len(x_train_t))

        for start in range(0, len(indices), cfg.batch_size):
            batch_idx = indices[start : start + cfg.batch_size]
            xb = x_train_t[batch_idx]
            yb_price = y_train_price_t[batch_idx]
            yb_direction = y_train_direction_t[batch_idx]

            optimizer.zero_grad(set_to_none=True)
            pred_price, pred_direction = model(xb)
            loss_price = price_criterion(pred_price, yb_price)
            loss_direction = direction_criterion(pred_direction, yb_direction)
            loss = loss_price + cfg.class_loss_weight * loss_direction
            loss.backward()
            nn.utils.clip_grad_norm_(model.parameters(), max_norm=1.0)
            optimizer.step()

        model.eval()
        with torch.no_grad():
            val_price_pred, val_direction_logits = model(x_val_t)
            val_loss_price = price_criterion(val_price_pred, y_val_price_t)
            val_loss_direction = direction_criterion(val_direction_logits, y_val_direction_t)
            val_loss = float((val_loss_price + cfg.class_loss_weight * val_loss_direction).item())

        if val_loss < best_val_loss:
            best_val_loss = val_loss
            best_state = {k: v.detach().cpu().clone() for k, v in model.state_dict().items()}
            stale_epochs = 0
        else:
            stale_epochs += 1
            if stale_epochs >= cfg.patience:
                break

    if best_state is not None:
        model.load_state_dict(best_state)

    return model


def _save_metrics(symbol: str, metrics: dict[str, float]) -> None:
    paths = _model_paths(symbol)
    payload = {**metrics, "updated_at": datetime.utcnow().isoformat()}
    paths["meta"].write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _load_metrics(symbol: str) -> dict[str, float]:
    paths = _model_paths(symbol)
    if not paths["meta"].exists():
        return {"mae": 0.0, "rmse": 0.0, "direction_accuracy": 0.0}
    payload = json.loads(paths["meta"].read_text(encoding="utf-8"))
    return {
        "mae": float(payload.get("mae", 0.0)),
        "rmse": float(payload.get("rmse", 0.0)),
        "direction_accuracy": float(payload.get("direction_accuracy", 0.0)),
    }


def train_model(
    symbol: str,
    df: pd.DataFrame | None = None,
    overrides: dict[str, Any] | None = None,
) -> dict[str, float]:
    cfg = _runtime_config(overrides=overrides)
    symbol = symbol.strip().upper()
    if df is None:
        df = _load_symbol_df(symbol)
    if df.empty:
        raise ValueError(f"No data found for symbol {symbol}")

    dataset = _build_features(df, include_targets=True)
    minimum_rows = max(cfg.lookback + 40, 80)
    if len(dataset) < minimum_rows:
        raise ValueError(f"Not enough data to train model for {symbol}")

    split_idx = int(len(dataset) * 0.8)
    if split_idx <= cfg.lookback or split_idx >= len(dataset):
        raise ValueError(f"Data split invalid for symbol {symbol}")

    feature_values = dataset[FEATURE_COLUMNS].to_numpy(dtype=np.float32)
    y_price_all = dataset["target_price"].to_numpy(dtype=np.float32)
    y_direction_all = dataset["target_direction"].to_numpy(dtype=np.int64)

    scaler = StandardScaler()
    scaler.fit(feature_values[:split_idx])
    scaled_features = scaler.transform(feature_values).astype(np.float32)

    x_train, y_train_price, y_train_direction = _make_sequences(
        scaled_features,
        y_price_all,
        y_direction_all,
        lookback=cfg.lookback,
        start_idx=cfg.lookback,
        end_idx=split_idx,
    )
    x_test, y_test_price, y_test_direction = _make_sequences(
        scaled_features,
        y_price_all,
        y_direction_all,
        lookback=cfg.lookback,
        start_idx=split_idx,
        end_idx=len(dataset),
    )

    if len(x_train) == 0 or len(x_test) == 0:
        raise ValueError(f"Not enough sequence data to train model for {symbol}")

    torch.manual_seed(cfg.seed)
    np.random.seed(cfg.seed)
    model = _train_hybrid_model(
        x_train,
        y_train_price,
        y_train_direction,
        x_test,
        y_test_price,
        y_test_direction,
        cfg,
    )

    model.eval()
    with torch.no_grad():
        x_test_t = torch.tensor(x_test, dtype=torch.float32, device=DEVICE)
        pred_price_t, pred_direction_logits_t = model(x_test_t)
        pred_price_np = pred_price_t.detach().cpu().numpy()
        pred_direction_np = torch.argmax(pred_direction_logits_t, dim=1).detach().cpu().numpy()

    mae = float(mean_absolute_error(y_test_price, pred_price_np))
    rmse = float(np.sqrt(mean_squared_error(y_test_price, pred_price_np)))
    direction_accuracy = float(accuracy_score(y_test_direction, pred_direction_np))

    model_payload = {
        "state_dict": model.state_dict(),
        "input_size": len(FEATURE_COLUMNS),
        "hidden_size": cfg.hidden_size,
        "num_layers": cfg.num_layers,
        "dropout": cfg.dropout,
        "lookback": cfg.lookback,
        "seed": cfg.seed,
        "learning_rate": cfg.learning_rate,
        "batch_size": cfg.batch_size,
        "patience": cfg.patience,
        "class_loss_weight": cfg.class_loss_weight,
        "model_variant": cfg.model_variant,
        "feature_columns": FEATURE_COLUMNS,
    }

    paths = _model_paths(symbol)
    MODEL_DIR.mkdir(parents=True, exist_ok=True)
    torch.save(model_payload, paths["price"])
    torch.save(
        {
            "label": "hybrid_sequence_direction_head",
            "class_prior_up": float(np.mean(y_train_direction)),
        },
        paths["direction"],
    )
    joblib.dump(scaler, paths["scaler"])

    metrics = {
        "mae": mae,
        "rmse": rmse,
        "direction_accuracy": direction_accuracy,
        "model_family": "SEQUENCE_HYBRID",
        "model_variant": cfg.model_variant,
        "lookback_window": float(cfg.lookback),
        "epochs": float(cfg.max_epochs),
    }
    _save_metrics(symbol, metrics)

    if direction_accuracy < 0.60:
        print(f"WARNING: Direction accuracy for {symbol} below 60%: {direction_accuracy:.2%}")

    return metrics


def _latest_sequence(symbol: str, scaler: StandardScaler, lookback: int) -> tuple[torch.Tensor, float]:
    df = _load_symbol_df(symbol)
    if df.empty:
        raise ValueError(f"No data found for symbol {symbol}")

    feature_df = _build_features(df, include_targets=False)
    if len(feature_df) < lookback:
        raise ValueError(f"Not enough engineered feature rows for symbol {symbol}")

    latest_features = feature_df[FEATURE_COLUMNS].tail(lookback).to_numpy(dtype=np.float32)
    scaled = scaler.transform(latest_features).astype(np.float32)
    sequence = torch.tensor(scaled[None, :, :], dtype=torch.float32, device=DEVICE)
    current_price = float(df.iloc[-1]["close"])
    return sequence, current_price


def _load_hybrid_model(symbol: str) -> tuple[SequenceHybridModel, StandardScaler, int]:
    paths = _model_paths(symbol)
    payload = torch.load(paths["price"], map_location=DEVICE)

    model = SequenceHybridModel(
        input_size=int(payload.get("input_size", len(FEATURE_COLUMNS))),
        hidden_size=int(payload.get("hidden_size", HIDDEN_SIZE)),
        num_layers=int(payload.get("num_layers", NUM_LAYERS)),
        dropout=float(payload.get("dropout", DROPOUT)),
        model_variant=_normalize_variant(str(payload.get("model_variant", MODEL_VARIANT))),
    ).to(DEVICE)
    model.load_state_dict(payload["state_dict"])
    model.eval()

    scaler: StandardScaler = joblib.load(paths["scaler"])
    lookback = int(payload.get("lookback", LOOKBACK_WINDOW))
    return model, scaler, lookback


def predict_next_day(symbol: str) -> dict[str, Any]:
    symbol = symbol.strip().upper()
    paths = _model_paths(symbol)

    if not paths["price"].exists() or not paths["direction"].exists() or not paths["scaler"].exists():
        raise FileNotFoundError(f"Model files not found for {symbol}")

    model, scaler, lookback = _load_hybrid_model(symbol)

    seq_tensor, current_price = _latest_sequence(symbol, scaler, lookback)

    with torch.no_grad():
        price_pred_t, direction_logits_t = model(seq_tensor)
        predicted_price = float(price_pred_t.detach().cpu().numpy()[0])
        direction_prob = torch.softmax(direction_logits_t, dim=1).detach().cpu().numpy()[0]

    predicted_class = int(np.argmax(direction_prob))
    class_confidence = float(np.max(direction_prob))

    metrics = _load_metrics(symbol)
    rmse = float(metrics["rmse"])
    uncertainty_ratio = min(1.0, rmse / max(current_price, 1e-6))
    confidence = max(0.0, min(1.0, 0.75 * class_confidence + 0.25 * (1.0 - uncertainty_ratio)))

    return {
        "symbol": symbol,
        "predicted_price": round(predicted_price, 2),
        "current_price": round(current_price, 2),
        "predicted_direction": "UP" if predicted_class == 1 else "DOWN",
        "confidence": round(confidence, 4),
        "mae": round(float(metrics["mae"]), 4),
        "rmse": round(rmse, 4),
        "direction_accuracy": round(float(metrics["direction_accuracy"]), 4),
    }


def train_all_symbols(min_rows: int = 252) -> dict[str, dict[str, float]]:
    results: dict[str, dict[str, float]] = {}
    symbols = get_all_symbols()

    conn = sqlite3.connect(DB_PATH)
    try:
        counts = pd.read_sql_query(
            "SELECT symbol, COUNT(*) AS cnt FROM stocks GROUP BY symbol", conn
        )
    finally:
        conn.close()

    allowed = set(
        counts.loc[counts["cnt"] >= int(min_rows), "symbol"].astype(str).str.upper().tolist()
    )

    for symbol in symbols:
        sym = symbol.strip().upper()
        if sym not in allowed:
            continue
        try:
            results[sym] = train_model(sym)
        except Exception as exc:  # noqa: BLE001
            results[sym] = {
                "mae": -1.0,
                "rmse": -1.0,
                "direction_accuracy": -1.0,
                "error": str(exc),
            }
    return results
