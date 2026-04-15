from __future__ import annotations

import logging
import time
import uuid
from datetime import datetime

from flask import Flask, g, jsonify, request
from flask_cors import CORS
from flask_jwt_extended import JWTManager
from werkzeug.exceptions import HTTPException

from config import (
    APP_NAME,
    ASYNC_MODE,
    CORS_ORIGINS,
    CSV_PATH,
    DEBUG,
    JWT_SECRET_KEY,
    LOG_LEVEL,
    JSON_LOGS,
    SECRET_KEY,
)
from database import count_stock_rows, count_symbols, init_db
from init_db import initialize_system
from logging_utils import configure_logging
from routes.auth import auth_bp
from routes.predict import predict_bp
from routes.sentiment import sentiment_bp
from routes.stocks import stocks_bp


def create_app() -> Flask:
    app = Flask(__name__)
    app.config["SECRET_KEY"] = SECRET_KEY
    app.config["JWT_SECRET_KEY"] = JWT_SECRET_KEY

    JWTManager(app)

    CORS(app, resources={r"/*": {"origins": CORS_ORIGINS}})

    configure_logging(level=LOG_LEVEL, json_logs=JSON_LOGS)

    @app.before_request
    def before_request_logging():
        g.start_time = time.perf_counter()
        g.request_id = request.headers.get("X-Request-ID") or str(uuid.uuid4())

    @app.after_request
    def after_request_logging(response):
        elapsed_ms = (time.perf_counter() - g.start_time) * 1000
        response.headers["X-Request-ID"] = g.request_id
        app.logger.info(
            "request_complete",
            extra={
                "request_id": g.request_id,
                "method": request.method,
                "path": request.path,
                "status_code": response.status_code,
                "elapsed_ms": round(elapsed_ms, 2),
            },
        )
        return response

    @app.errorhandler(Exception)
    def handle_unexpected_error(exc: Exception):
        if isinstance(exc, HTTPException):
            return jsonify({"error": exc.description}), exc.code
        app.logger.exception(
            "Unhandled server error",
            extra={"request_id": getattr(g, "request_id", None), "path": request.path},
        )
        return jsonify({"error": str(exc)}), 500

    app.register_blueprint(auth_bp)
    app.register_blueprint(stocks_bp)
    app.register_blueprint(predict_bp)
    app.register_blueprint(sentiment_bp)

    with app.app_context():
        init_db()
        if count_stock_rows() == 0 and CSV_PATH.exists():
            summary = initialize_system(CSV_PATH)
            app.logger.info(
                "DB initialized with %s records across %s symbols",
                summary["rows_loaded"],
                summary["symbols_loaded"],
            )
        else:
            app.logger.info(
                "DB ready with %s records across %s symbols",
                count_stock_rows(),
                count_symbols(),
            )

    @app.route("/health", methods=["GET"])
    def health():
        return (
            jsonify(
                {
                    "app": APP_NAME,
                    "status": "ok",
                    "timestamp": datetime.utcnow().isoformat(),
                    "debug": DEBUG,
                    "async_mode": ASYNC_MODE,
                }
            ),
            200,
        )

    return app


if __name__ == "__main__":
    app = create_app()
    app.run(host="0.0.0.0", port=5000, debug=DEBUG)
