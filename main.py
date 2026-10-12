from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
import signal
import sqlite3
import struct
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import aiohttp
from telegram import Bot
from telegram.error import TelegramError

# --- CONFIGURATION DU LOGGING (RENDER COMPATIBLE) ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] ⚡ %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger("QuantumAlphaBot")

LAMPORTS = 1_000_000_000
TOTAL_SUPPLY_RAW = 1_000_000_000 * 10**6  # 1 Md de tokens, 6 décimales (Pump.fun)


def env(key: str, default: str | None = None) -> str | None:
    value = os.getenv(key)
    return default if value is None or not value.strip() else value.strip()


def env_bool(key: str, default: bool) -> bool:
    value = env(key)
    return default if value is None else value.lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class Config:
    http_url: str = field(repr=False)
    ws_url: str = field(repr=False)
    telegram_token: str = field(repr=False)
    telegram_chat_id: str
    pump_program_id: str
    min_liquidity_sol: float
    max_liquidity_sol: float
    max_top10_percent: float
    max_dev_percent: float
    require_dex_paid: bool
    min_alpha_score: float

    @classmethod
    def from_env(cls) -> "Config":
        return cls(
            http_url=env("SOLANA_HTTP_URL", ""),
            ws_url=env("SOLANA_WS_URL", ""),
            telegram_token=env("TELEGRAM_BOT_TOKEN", ""),
            telegram_chat_id=env("TELEGRAM_CHAT_ID", ""),
            pump_program_id=env("PUMP_PROGRAM_ID", "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"),
            min_liquidity_sol=float(env("MIN_LIQUIDITY_SOL", "3")),
            max_liquidity_sol=float(env("MAX_LIQUIDITY_SOL", "30")),
            max_top10_percent=float(env("MAX_TOP10_PERCENT", "20")),
            max_dev_percent=float(env("MAX_DEV_PERCENT", "5")),
            require_dex_paid=env_bool("REQUIRE_DEX_PAID", True),
            min_alpha_score=float(env("MIN_
