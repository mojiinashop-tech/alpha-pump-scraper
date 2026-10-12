"""
Bot Pump.fun (Solana) : monitoring + filtres anti-scam + achat automatique optionnel.

Pipeline :
  WebSocket Helius (logsSubscribe) -> détection des créations
  -> ingestion (1 getTransaction, filtre dev immédiat)
  -> watchlist (1 getMultipleAccounts pour TOUS les tokens suivis)
  -> validation complète quand la liquidité atteint le seuil
  -> alerte Telegram -> achat (si AUTO_BUY=true et DRY_RUN=false)

Sécurité par défaut : AUTO_BUY=false, DRY_RUN=true.
"""
from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
import random
import signal
import sqlite3
import struct
import time
from collections import Counter
from dataclasses import dataclass, field
from typing import Any

import aiohttp
from aiohttp import web

log = logging.getLogger("bot")

LAMPORTS = 1_000_000_000
TOTAL_SUPPLY_RAW = 1_000_000_000 * 10**6  # 1 Md de tokens, 6 décimales (Pump.fun)
CREATE_LOG_PREFIX = "Program log: Instruction: Create"


# ════════════════════════════════════════════════════════════════════════════
# CONFIG
# ════════════════════════════════════════════════════════════════════════════
def env(key: str, default: str | None = None) -> str | None:
    value = os.getenv(key)
    return default if value is None or not value.strip() else value.strip()


def env_bool(key: str, default: bool) -> bool:
    value = env(key)
    return default if value is None else value.lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class Config:
    # repr=False : ces valeurs ne doivent jamais apparaître dans un log
    http_url: str = field(repr=False)
    ws_url: str = field(repr=False)
    telegram_token: str = field(repr=False)
    telegram_chat_id: str
    pump_program_id: str
    # Filtres
    min_liquidity_sol: float
    max_liquidity_sol: float
    max_top10_percent: float
    max_dev_percent: float
    holder_basis: str  # "total" ou "circulating"
    require_mint_renounced: bool
    max_bundle_txs: int
    max_bundle_percent: float
    bundle_slot_window: int
    # Réseau / perf
    timeout: float
    rpc_max_rps: float
    rpc_concurrency: int
    max_concurrency: int
    watch_interval: float
    watch_ttl: float
    watch_max: int
    reconnect_min: float
    reconnect_max: float
    db_path: str
    # Exécution
    auto_buy: bool
    dry_run: bool
    buy_amount_sol: float
    slippage_percent: int
    priority_fee_sol: float
    max_buys_per_hour: int
    max_daily_spend_sol: float
    min_wallet_reserve_sol: float
    trade_api_url: str
    wallet_private_key: str | None = field(default=None, repr=False)

    @classmethod
    def from_env(cls) -> "Config":
        required = ("SOLANA_HTTP_URL", "SOLANA_WS_URL", "TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID")
        missing = [k for k in required if not env(k)]
        if missing:
            raise RuntimeError(f"Variables manquantes: {', '.join(missing)}")
        cfg = cls(
            http_url=env("SOLANA_HTTP_URL"),
            ws_url=env("SOLANA_WS_URL"),
            telegram_token=env("TELEGRAM_BOT_TOKEN"),
            telegram_chat_id=env("TELEGRAM_CHAT_ID"),
            pump_program_id=env("PUMP_PROGRAM_ID", "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"),
            min_liquidity_sol=float(env("MIN_LIQUIDITY_SOL", "3")),
            max_liquidity_sol=float(env("MAX_LIQUIDITY_SOL", "30")),
            max_top10_percent=float(env("MAX_TOP10_PERCENT", "20")),
            max_dev_percent=float(env("MAX_DEV_PERCENT", "5")),
            holder_basis=env("HOLDER_BASIS", "total").lower(),
            require_mint_renounced=env_bool("REQUIRE_MINT_RENOUNCED", True),
            max_bundle_txs=int(env("MAX_BUNDLE_TXS", "3")),
            max_bundle_percent=float(env("MAX_BUNDLE_PERCENT", "5")),
            bundle_slot_window=int(env("BUNDLE_SLOT_WINDOW", "0")),
            timeout=float(env("RPC_TIMEOUT_SECONDS", "8")),
            rpc_max_rps=float(env("RPC_MAX_RPS", "8")),
            rpc_concurrency=int(env("RPC_CONCURRENCY", "6")),
            max_concurrency=int(env("MAX_CONCURRENT_VALIDATIONS", "6")),
            watch_interval=float(env("WATCH_INTERVAL_SECONDS", "3")),
            watch_ttl=float(env("WATCH_TTL_SECONDS", "600")),
            watch_max=int(env("WATCH_MAX", "400")),
            reconnect_min=float(env("RECONNECT_MIN_SECONDS", "1")),
            reconnect_max=float(env("RECONNECT_MAX_SECONDS", "30")),
            db_path=env("SEEN_DB_PATH", "data/seen.db"),
            auto_buy=env_bool("AUTO_BUY", False),
            dry_run=env_bool("DRY_RUN", True),
            buy_amount_sol=float(env("BUY_AMOUNT_SOL", "0.05")),
            slippage_percent=int(env("SLIPPAGE_PERCENT", "15")),
            priority_fee_sol=float(env("PRIORITY_FEE_SOL", "0.0005")),
            max_buys_per_hour=int(env("MAX_BUYS_PER_HOUR", "5")),
            max_daily_spend_sol=float(env("MAX_DAILY_SPEND_SOL", "0.5")),
            min_wallet_reserve_sol=float(env("MIN_WALLET_RESERVE_SOL", "0.02")),
            trade_api_url=env("TRADE_API_URL", "https://pumpportal.fun/api/trade-local"),
            wallet_private_key=env("WALLET_PRIVATE_KEY"),
        )
        cfg.check()
        return cfg

    def check(self) -> None:
        if self.holder_basis not in {"total", "circulating"}:
            raise RuntimeError("HOLDER_BASIS doit valoir 'total' ou 'circulating'")
        if self.min_liquidity_sol >= self.max_liquidity_sol:
            raise RuntimeError("MIN_LIQUIDITY_SOL doit être < MAX_LIQUIDITY_SOL")
        if self.auto_buy and not self.dry_run and not self.wallet_private_key:
            raise RuntimeError("AUTO_BUY=true et DRY_RUN=false exigent WALLET_PRIVATE_KEY")
        if self.buy_amount_sol > self.max_daily_spend_sol:
            raise RuntimeError("BUY_AMOUNT_SOL dépasse MAX_DAILY_SPEND_SOL")

    @property
    def mode_label(self) -> str:
        if not self.auto_buy:
            return "ALERTES SEULES"
        return "DRY-RUN (achats simulés)" if self.dry_run else "ACHAT RÉEL"


# ════════════════════════════════════════════════════════════════════════════
# MODÈLES
# ════════════════════════════════════════════════════════════════════════════
@dataclass
class Candidate:
    mint: str
    creator: str
    bonding_curve: str
    assoc_curve: str  # token account de la courbe (détient les tokens non vendus)
    signature: str
    slot: int
    dev_initial_raw: int = 0
    created_at: float = field(default_factory=time.time)


@dataclass
class CurveState:
    real_token_reserves: int
    real_sol_reserves: int
    total_supply: int
    complete: bool

    @property
    def liquidity_sol(self) -> float:
        return self.real_sol_reserves / LAMPORTS


@dataclass
class Check:
    name: str
    passed: bool
    detail: str


@dataclass
class Verdict:
    candidate: Candidate
    liquidity_sol: float = 0.0
    top10_percent: float = 0.0
    dev_percent: float = 0.0
    bundle_percent: float = 0.0
    checks: list[Check] = field(default_factory=list)

    def add(self, name: str, passed: bool, detail: str) -> None:
        self.checks.append(Check(name, passed, detail))

    @property
    def ok(self) -> bool:
        return bool(self.checks) and all(c.passed for c in self.checks)

    @property
    def failure(self) -> str:
        return "; ".join(f"{c.name}: {c.detail}" for c in self.checks if not c.passed)


def parse_curve(data: bytes) -> CurveState | None:
    """Layout du compte bonding curve : 8 octets discriminant + 5 u64 + 1 bool."""
    if len(data) < 49:
        return None
    _vt, _vs, real_tokens, real_sol, supply, complete = struct.unpack_from("<5QB", data, 8)
    return CurveState(real_tokens, real_sol, supply, bool(complete))


def iter_instructions(tx: dict[str, Any]):
    yield from tx["transaction"]["message"].get("instructions", [])
    for inner in (tx.get("meta") or {}).get("innerInstructions") or []:
        yield from inner.get("instructions", [])


def parse_creation(tx: dict[str, Any], signature: str, program_id: str) -> Candidate | None:
    """Extrait mint / courbe / créateur de la transaction de création."""
    meta = tx.get("meta") or {}
    if meta.get("err"):
        return None
    post = meta.get("postTokenBalances") or []
    first = tx["transaction"]["message"]["accountKeys"][0]
    creator = first["pubkey"] if isinstance(first, dict) else first
    for ix in iter_instructions(tx):
        accts = ix.get("accounts") or []
        if ix.get("programId") != program_id or len(accts) < 4:
            continue
        mint = accts[0]
        if not any(b.get("mint") == mint for b in post):
            continue
        dev_raw = sum(
            int(b["uiTokenAmount"]["amount"])
            for b in post
            if b.get("mint") == mint and b.get("owner") == creator
        )
        return Candidate(
            mint=mint, creator=creator, bonding_curve=accts[2], assoc_curve=accts[3],
            signature=signature, slot=int(tx.get("slot", 0)), dev_initial_raw=dev_raw,
        )
    return None


# ════════════════════════════════════════════════════════════════════════════
# BASE DE DONNÉES
# ════════════════════════════════════════════════════════════════════════════
class SeenDB:
    def __init__(self, path: str):
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        self.db = sqlite3.connect(path, check_same_thread=False)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute(
            "CREATE TABLE IF NOT EXISTS seen (mint TEXT PRIMARY KEY, signature TEXT, seen_at INTEGER NOT NULL)"
        )
        self.db.execute(
            "CREATE TABLE IF NOT EXISTS trades (id INTEGER PRIMARY KEY AUTOINCREMENT, "
            "mint TEXT NOT NULL, signature TEXT, amount_sol REAL NOT NULL, ts INTEGER NOT NULL)"
        )
        self.db.commit()
        self.lock = asyncio.Lock()

    async def _run(self, fn, *args):
        async with self.lock:
            return await asyncio.to_thread(fn, *args)

    def _claim(self, mint: str, signature: str) -> bool:
        try:
            self.db.execute("INSERT INTO seen(mint, signature, seen_at) VALUES (?, ?, ?)",
                            (mint, signature, int(time.time())))
            self.db.commit()
            return True
        except sqlite3.IntegrityError:
            return False

    def _add_trade(self, mint: str, signature: str, amount: float) -> None:
        self.db.execute("INSERT INTO trades(mint, signature, amount_sol, ts) VALUES (?, ?, ?, ?)",
                        (mint, signature, amount, int(time.time())))
        self.db.commit()

    def _count_trades(self, since: float) -> int:
        return self.db.execute("SELECT COUNT(*) FROM trades WHERE ts >= ?", (int(since),)).fetchone()[0]

    def _spent(self, since: float) -> float:
        return self.db.execute("SELECT COALESCE(SUM(amount_sol), 0) FROM trades WHERE ts >= ?",
                               (int(since),)).fetchone()[0]

    async def claim(self, mint: str, signature: str) -> bool:
        return await self._run(self._claim, mint, signature)

    async def add_trade(self, mint: str, signature: str, amount: float) -> None:
        await self._run(self._add_trade, mint, signature, amount)

    async def count_trades(self, since: float) -> int:
        return await self._run(self._count_trades, since)

    async def spent(self, since: float) -> float:
        return await self._run(self._spent, since)

    def close(self) -> None:
        self.db.close()


# ════════════════════════════════════════════════════════════════════════════
# RPC HELIUS
# ════════════════════════════════════════════════════════════════════════════
class RpcError(Exception):
    pass


class RateLimiter:
    def __init__(self, rps: float):
        self.interval = 1.0 / max(rps, 0.1)
        self._next = 0.0
        self._lock = asyncio.Lock()

    async def wait(self) -> None:
        async with self._lock:
            now = time.monotonic()
            delay = self._next - now
            self._next = max(now, self._next) + self.interval
        if delay > 0:
            await asyncio.sleep(delay)

    def penalize(self, seconds: float) -> None:
        self._next = max(self._next, time.monotonic() + seconds)


def retry_after(headers: Any, fallback: float) -> float:
    try:
        return min(float(headers.get("Retry-After", fallback)), 30.0)
    except (TypeError, ValueError):
        return fallback


class SolanaRPC:
    def __init__(self, cfg: Config, session: aiohttp.ClientSession):
        self.cfg = cfg
        self.session = session
        self._id = 0
        self._sem = asyncio.Semaphore(cfg.rpc_concurrency)
        self._limiter = RateLimiter(cfg.rpc_max_rps)

    async def call(self, method: str, params: list[Any], retries: int = 5) -> Any:
        delay = 1.0
        last = "inconnu"
        for _ in range(retries):
            await self._limiter.wait()
            self._id += 1
            body = {"jsonrpc": "2.0", "id": self._id, "method": method, "params": params}
            wait: float | None = None
            payload: dict[str, Any] = {}
            try:
                async with self._sem, self.session.post(
                    self.cfg.http_url, json=body, timeout=aiohttp.ClientTimeout(total=self.cfg.timeout)
                ) as resp:
                    if resp.status == 429 or resp.status >= 500:
                        wait = retry_after(resp.headers, delay)
                        if resp.status == 429:
                            self._limiter.penalize(wait)
                        last = f"HTTP {resp.status}"
                    else:
                        resp.raise_for_status()
                        payload = await resp.json(content_type=None)
            except aiohttp.ClientResponseError as exc:
                raise RpcError(f"{method}: HTTP {exc.status}") from None
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                wait, last = delay, type(exc).__name__
            if wait is None:
                err = payload.get("error")
                if err:
                    if isinstance(err, dict) and err.get("code") == -32005:
                        wait, last = delay, "-32005"
                    else:
                        raise RpcError(f"{method}: {err}")
                else:
                    return payload.get("result")
            await asyncio.sleep(wait)
            delay = min(delay * 2, 15.0)
        raise RpcError(f"{method}: échec après {retries} essais ({last})")

    async def transaction(self, signature: str) -> dict[str, Any] | None:
        return await self.call("getTransaction", [signature, {
            "encoding": "jsonParsed", "commitment": "confirmed", "maxSupportedTransactionVersion": 1}])


async def fetch_curves(rpc: SolanaRPC, addresses: list[str], program_id: str) -> list[CurveState | None]:
    res = await rpc.call("getMultipleAccounts", [addresses, {"encoding": "base64", "commitment": "confirmed"}])
    out: list[CurveState | None] = []
    for acc in (res or {}).get("value", []):
        state = None
        if acc and acc.get("owner") == program_id:
            try:
                state = parse_curve(base64.b64decode(acc["data"][0]))
            except (KeyError, IndexError, TypeError, ValueError):
                state = None
        out.append(state)
    return out


# ════════════════════════════════════════════════════════════════════════════
# VALIDATION ANTI-SCAM
# ════════════════════════════════════════════════════════════════════════════
class Validator:
    def __init__(self, cfg: Config, rpc: SolanaRPC):
        self.cfg = cfg
        self.rpc = rpc

    def _denominator(self, total: int, curve_balance: int) -> int:
        if self.cfg.holder_basis == "circulating":
            return max(total - curve_balance, 1)
        return max(total, 1)

    async def _mint_authorities(self, mint: str) -> tuple[Any, Any]:
        res = await self.rpc.call("getAccountInfo", [mint, {"encoding": "jsonParsed", "commitment": "confirmed"}])
        value = (res or {}).get("value")
        data = (value or {}).get("data")
        parsed = data.get("parsed") if isinstance(data, dict) else None
        if not parsed:
            raise RpcError("mint illisible")
        info = parsed.get("info", {})
        return info.get("mintAuthority"), info.get("freezeAuthority")

    async def _largest(self, mint: str) -> list[dict[str, Any]]:
        res = await self.rpc.call("getTokenLargestAccounts", [mint, {"commitment": "confirmed"}])
        return (res or {}).get("value", [])

    async def _dev_balance(self, cand: Candidate) -> int:
        res = await self.rpc.call("getTokenAccountsByOwner", [
            cand.creator, {"mint": cand.mint}, {"encoding": "jsonParsed", "commitment": "confirmed"}])
        total = 0
        for acc in (res or {}).get("value", []):
            total += int(acc["account"]["data"]["parsed"]["info"]["tokenAmount"]["amount"])
        return total

    async def _bundles(self, cand: Candidate) -> tuple[bool, str, float]:
        sigs: list[dict[str, Any]] = []
        before = None
        complete = False
        for _ in range(3):
            opts: dict[str, Any] = {"limit": 1000, "commitment": "confirmed"}
            if before:
                opts["before"] = before
            batch = await self.rpc.call("getSignaturesForAddress", [cand.bonding_curve, opts]) or []
            sigs.extend(batch)
            if len(batch) < 1000:
                complete = True
                break
            before = batch[-1]["signature"]
        if not complete:
            return False, "historique trop long pour être vérifié", 0.0
        window_end = cand.slot + self.cfg.bundle_slot_window
        early = [s for s in sigs
                 if cand.slot <= s["slot"] <= window_end and s["signature"] != cand.signature and not s.get("err")]
        if len(early) > self.cfg.max_bundle_txs:
            return False, f"{len(early)} achats dans le bloc de lancement (max {self.cfg.max_bundle_txs})", 0.0
        bundle_raw = 0
        linked = False
        for s in early:
            tx = await self.rpc.transaction(s["signature"])
            for b in ((tx or {}).get("meta") or {}).get("postTokenBalances") or []:
                if b.get("mint") == cand.mint and b.get("owner") != cand.bonding_curve:
                    bundle_raw += int(b["uiTokenAmount"]["amount"])
                    linked = linked or b.get("owner") == cand.creator
        pct = bundle_raw / TOTAL_SUPPLY_RAW * 100
        if linked:
            return False, "achat du bloc 0 effectué par le wallet du créateur", pct
        if pct > self.cfg.max_bundle_percent:
            return False, f"{pct:.1f}% du supply acheté au bloc 0 (max {self.cfg.max_bundle_percent}%)", pct
        return True, f"{len(early)} achat(s) bloc 0, {pct:.1f}% du supply", pct

    async def validate(self, cand: Candidate, curve: CurveState) -> Verdict:
        cfg = self.cfg
        v = Verdict(cand, liquidity_sol=curve.liquidity_sol)

        liq = curve.liquidity_sol
        v.add("Liquidité", cfg.min_liquidity_sol <= liq <= cfg.max_liquidity_sol,
              f"{liq:.2f} SOL réels (min {cfg.min_liquidity_sol}, max {cfg.max_liquidity_sol})")
        v.add("Courbe verrouillée", not curve.complete and curve.real_token_reserves > 0,
              "liquidité détenue par le programme Pump.fun")
        if not v.ok:
            return v

        (mint_auth, freeze_auth), largest, dev_raw = await asyncio.gather(
            self._mint_authorities(cand.mint), self._largest(cand.mint), self._dev_balance(cand))

        v.add("Mint authority", mint_auth is None or not cfg.require_mint_renounced,
              "renoncée" if mint_auth is None else "ACTIVE")
        v.add("Freeze authority", freeze_auth is None, "aucune" if freeze_auth is None else "ACTIVE")

        curve_balance = next((int(a["amount"]) for a in largest if a["address"] == cand.assoc_curve), 0)
        denom = self._denominator(curve.total_supply or TOTAL_SUPPLY_RAW, curve_balance)
        holders = sorted((int(a["amount"]) for a in largest if a["address"] != cand.assoc_curve), reverse=True)
        v.top10_percent = sum(holders[:10]) / denom * 100
        v.add("Top 10 holders", v.top10_percent <= cfg.max_top10_percent,
              f"{v.top10_percent:.1f}% (max {cfg.max_top10_percent}%, base {cfg.holder_basis})")

        v.dev_percent = dev_raw / denom * 100
        v.add("Wallet dev", v.dev_percent <= cfg.max_dev_percent,
              f"{v.dev_percent:.1f}% (max {cfg.max_dev_percent}%, base {cfg.holder_basis})")
        if not v.ok:
            return v

        ok, detail, v.bundle_percent = await self._bundles(cand)
        v.add("Bundles bloc 0", ok, detail)
        return v


# ════════════════════════════════════════════════════════════════════════════
# TELEGRAM
# ════════════════════════════════════════════════════════════════════════════
class Telegram:
    def __init__(self, cfg: Config, session: aiohttp.ClientSession):
        self.cfg = cfg
        self.session = session
        self.base = f"https://api.telegram.org/bot{cfg.telegram_token}"

    async def send(self, text: str) -> bool:
        payload = {"chat_id": self.cfg.telegram_chat_id, "text": text,
                   "parse_mode": "HTML", "disable_web_page_preview": True}
        for _ in range(3):
            try:
                async with self.session.post(f"{self.base}/sendMessage", json=payload,
                                             timeout=aiohttp.ClientTimeout(total=15)) as resp:
                    if resp.status == 429:
                        data = await resp.json(content_type=None)
                        await asyncio.sleep(float(data.get("parameters", {}).get("retry_after", 3)))
                        continue
                    if resp.status >= 400:
                        log.warning("Telegram HTTP %s", resp.status)
                        return False
                    return True
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                log.warning("Telegram: %s", type(exc).__name__)
                await asyncio.sleep(2)
        return False

    async def poll_commands(self, handler) -> None:
        offset = 0
        while True:
            try:
                async with self.session.get(f"{self.base}/getUpdates", params={"offset": offset, "timeout": 25},
                                            timeout=aiohttp.ClientTimeout(total=40)) as resp:
                    data = await resp.json(content_type=None)
                for upd in data.get("result", []):
                    offset = upd["update_id"] + 1
                    msg = upd.get("message") or {}
                    if str(msg.get("chat", {}).get("id")) != self.cfg.telegram_chat_id:
                        continue
                    text = (msg.get("text") or "").strip().lower()
                    if text.startswith("/"):
                        await self.send(await handler(text.split()[0].split("@")[0]))
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.warning("Telegram polling: %s", type(exc).__name__)
                await asyncio.sleep(5)


def format_alert(v: Verdict) -> str:
    c = v.candidate
    m = c.mint
    links = (
        f'<a href="https://dexscreener.com/solana/{m}">Dexscreener</a> · '
        f'<a href="https://gmgn.ai/sol/token/{m}">GMGN</a> · '
        f'<a href="https://axiom.trade/meme/{c.bonding_curve}">Axiom</a> · '
        f'<a href="https://pump.fun/coin/{m}">Pump.fun</a> · '
        f'<a href="https://solscan.io/token/{m}">Solscan</a>'
    )
    lines = [
        "🟢 <b>Token validé (tous les filtres)</b>",
        f"<code>{m}</code>",
        f"💧 {v.liquidity_sol:.2f} SOL · 👥 Top10 {v.top10_percent:.1f}% · "
        f"👨‍💻 Dev {v.dev_percent:.1f}% · 📦 Bundle {v.bundle_percent:.1f}%",
        "",
    ]
    lines += [f"{'✅' if ch.passed else '❌'} {ch.name} — {ch.detail}" for ch in v.checks]
    lines += ["", links]
    return "\n".join(lines)


# ════════════════════════════════════════════════════════════════════════════
# EXÉCUTION & GARDE-FOUS
# ════════════════════════════════════════════════════════════════════════════
class PumpPortalExecutor:
    def __init__(self, cfg: Config, session: aiohttp.ClientSession, rpc: SolanaRPC):
        from solders.keypair import Keypair

        self.cfg, self.session, self.rpc = cfg, session, rpc
        self._keypair = Keypair.from_base58_string(cfg.wallet_private_key)
        self.pubkey = str(self._keypair.pubkey())

    async def balance_sol(self) -> float:
        res = await self.rpc
