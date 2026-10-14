from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
import signal
import sqlite3
import struct
import sys
import time
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import aiohttp
from aiohttp import web
from telegram import Bot, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.error import TelegramError

logging.basicConfig(
    stream=sys.stdout,
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO),
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("pumpfun-onchain")
for name in ("telegram", "httpx", "httpcore"):
    logging.getLogger(name).setLevel(logging.WARNING)

LAMPORTS_PER_SOL = 1_000_000_000
TOKEN_DECIMALS = 6
TOTAL_SUPPLY_RAW = 1_000_000_000 * 10**TOKEN_DECIMALS
PUMP_INITIAL_REAL_TOKEN_RESERVES = 793_100_000 * 10**TOKEN_DECIMALS
PUMP_PROGRAM_ID = os.getenv("PUMP_PROGRAM_ID", "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P")
COMMITMENT = os.getenv("SOLANA_COMMITMENT", "confirmed")


def env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    return default if value is None else value.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class Candidate:
    mint: str
    creator: str
    bonding_curve: str
    associated_curve: str
    signature: str
    slot: int
    name: str = "TOKEN"
    ticker: str = "TICKER"


@dataclass(frozen=True)
class CurveState:
    virtual_token_reserves: int
    virtual_sol_reserves: int
    real_token_reserves: int
    real_sol_reserves: int
    token_total_supply: int
    complete: bool

    @property
    def liquidity_sol(self) -> float:
        return self.real_sol_reserves / LAMPORTS_PER_SOL


class RpcError(RuntimeError):
    pass


class RateLimiter:
    def __init__(self, max_rps: float):
        self.interval = 1.0 / max(0.1, max_rps)
        self.next_at = 0.0
        self.lock = asyncio.Lock()

    async def wait(self) -> None:
        async with self.lock:
            now = time.monotonic()
            delay = max(0.0, self.next_at - now)
            self.next_at = max(now, self.next_at) + self.interval
        if delay:
            await asyncio.sleep(delay)

    def penalize(self, seconds: float) -> None:
        self.next_at = max(self.next_at, time.monotonic() + seconds)


class SolanaRPC:
    def __init__(self, session: aiohttp.ClientSession, url: str):
        self.session = session
        self.url = url
        self.request_id = 0
        self.limiter = RateLimiter(float(os.getenv("RPC_MAX_RPS", "3")))
        self.semaphore = asyncio.Semaphore(max(1, int(os.getenv("RPC_CONCURRENCY", "2"))))
        self.timeout = aiohttp.ClientTimeout(total=float(os.getenv("RPC_TIMEOUT_SECONDS", "12")))

    async def call(self, method: str, params: list[Any], retries: int = 3) -> Any:
        backoff = 0.8
        for attempt in range(retries):
            await self.limiter.wait()
            self.request_id += 1
            body = {"jsonrpc": "2.0", "id": self.request_id, "method": method, "params": params}
            try:
                async with self.semaphore:
                    async with self.session.post(self.url, json=body, timeout=self.timeout) as response:
                        if response.status == 429:
                            wait = min(30.0, float(response.headers.get("Retry-After", backoff)))
                            self.limiter.penalize(wait)
                            await asyncio.sleep(wait)
                            continue
                        response.raise_for_status()
                        payload = await response.json(content_type=None)
                if payload.get("error"):
                    raise RpcError(f"{method}: réponse RPC en erreur")
                return payload.get("result")
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                if attempt + 1 == retries:
                    raise RpcError(f"{method}: {type(exc).__name__}") from None
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 8.0)
        raise RpcError(f"{method}: échec RPC")

    async def transaction(self, signature: str) -> dict[str, Any] | None:
        return await self.call("getTransaction", [signature, {
            "encoding": "jsonParsed", "commitment": COMMITMENT,
            "maxSupportedTransactionVersion": 1,
        }])

    async def balance_sol(self, address: str) -> float | None:
        if not address:
            return None
        result = await self.call("getBalance", [address, {"commitment": COMMITMENT}])
        return float((result or {}).get("value", 0)) / LAMPORTS_PER_SOL

    async def signatures(self, address: str, limit: int = 100) -> list[dict[str, Any]]:
        return await self.call("getSignaturesForAddress", [address, {
            "limit": min(1000, max(1, limit)), "commitment": COMMITMENT,
        }]) or []


def parse_curve_account(raw: bytes) -> CurveState | None:
    if len(raw) < 49:
        return None
    try:
        return CurveState(*struct.unpack_from("<5QB", raw, 8))
    except struct.error:
        return None


def parse_candidate(tx: dict[str, Any], signature: str, logs: list[str]) -> Candidate | None:
    if (tx.get("meta") or {}).get("err"):
        return None
    text = " ".join(logs).lower()
    if "instruction: create" not in text and "instruction: createv2" not in text:
        return None
    message = (tx.get("transaction") or {}).get("message") or {}
    raw_keys = message.get("accountKeys", [])
    keys = [item.get("pubkey") if isinstance(item, dict) else item for item in raw_keys]
    for instruction in message.get("instructions", []):
        if instruction.get("programId") != PUMP_PROGRAM_ID:
            continue
        accounts = instruction.get("accounts") or []
        if all(isinstance(item, int) for item in accounts):
            if not accounts or max(accounts) >= len(keys):
                continue
            accounts = [keys[item] for item in accounts]
        if len(accounts) < 4 or not all(isinstance(item, str) for item in accounts):
            continue
        return Candidate(
            mint=accounts[0], creator=accounts[-1], bonding_curve=accounts[2],
            associated_curve=accounts[3], signature=signature,
            slot=int(tx.get("slot", 0) or 0),
        )
    return None


class SeenStore:
    def __init__(self, path: str):
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        self.db = sqlite3.connect(path, check_same_thread=False)
        self.db.execute("CREATE TABLE IF NOT EXISTS seen (mint TEXT PRIMARY KEY, signature TEXT NOT NULL)")
        self.db.execute("CREATE TABLE IF NOT EXISTS dev_cache (dev TEXT PRIMARY KEY, launches INTEGER NOT NULL, rugs INTEGER NOT NULL DEFAULT 0, analyzed_at INTEGER NOT NULL)")
        self.db.execute("CREATE TABLE IF NOT EXISTS rejection_events (created_at INTEGER NOT NULL, mint TEXT NOT NULL, reason TEXT NOT NULL)")
        self.db.commit()
        self.lock = asyncio.Lock()

    async def claim(self, mint: str, signature: str) -> bool:
        async with self.lock:
            try:
                self.db.execute("INSERT INTO seen VALUES (?, ?)", (mint, signature))
                self.db.commit()
                return True
            except sqlite3.IntegrityError:
                return False

    async def get_dev(self, creator: str) -> tuple[int, int] | None:
        async with self.lock:
            row = self.db.execute("SELECT launches, rugs FROM dev_cache WHERE dev=?", (creator,)).fetchone()
            return (int(row[0]), int(row[1])) if row else None

    async def put_dev(self, creator: str, launches: int, rugs: int) -> None:
        async with self.lock:
            self.db.execute("INSERT OR REPLACE INTO dev_cache VALUES (?, ?, ?, ?)", (creator, launches, rugs, int(time.time())))
            self.db.commit()

    async def reject(self, mint: str, reason: str) -> None:
        async with self.lock:
            self.db.execute("INSERT INTO rejection_events VALUES (?, ?, ?)", (int(time.time()), mint, reason[:240]))
            self.db.commit()

    def close(self) -> None:
        self.db.close()


class Validator:
    def __init__(self, cfg: dict[str, Any], rpc: SolanaRPC, store: SeenStore):
        self.cfg, self.rpc, self.store = cfg, rpc, store

    async def reject(self, candidate: Candidate, reason: str) -> None:
        logger.info("[FILTER REJECTED] %s — %s", candidate.mint, reason)
        await self.store.reject(candidate.mint, reason)

    async def curve(self, address: str) -> CurveState | None:
        result = await self.rpc.call("getAccountInfo", [address, {"encoding": "base64", "commitment": COMMITMENT}])
        value = (result or {}).get("value") or {}
        if value.get("owner") != PUMP_PROGRAM_ID:
            return None
        data = value.get("data")
        if not isinstance(data, list) or not data:
            return None
        try:
            return parse_curve_account(base64.b64decode(data[0]))
        except (ValueError, TypeError):
            return None

    async def mint_authorities(self, mint: str) -> tuple[Any, Any]:
        result = await self.rpc.call("getMultipleAccounts", [[mint], {"encoding": "jsonParsed", "commitment": COMMITMENT}])
        value = ((result or {}).get("value") or [None])[0] or {}
        info = (((value.get("data") or {}).get("parsed") or {}).get("info") or {})
        return info.get("mintAuthority"), info.get("freezeAuthority")

    async def largest_accounts(self, mint: str) -> list[dict[str, Any]]:
        result = await self.rpc.call("getTokenLargestAccounts", [mint, {"commitment": COMMITMENT}])
        return (result or {}).get("value", [])

    async def dev_balance(self, candidate: Candidate) -> int:
        result = await self.rpc.call("getTokenAccountsByOwner", [
            candidate.creator, {"mint": candidate.mint}, {"encoding": "jsonParsed", "commitment": COMMITMENT},
        ])
        total = 0
        for item in (result or {}).get("value", []):
            try:
                total += int(item["account"]["data"]["parsed"]["info"]["tokenAmount"]["amount"])
            except (KeyError, TypeError, ValueError):
                continue
        return total

    async def early_buyers(self, candidate: Candidate) -> tuple[set[str], int]:
        buyers: set[str] = set()
        early_count = 0
        for row in await self.rpc.signatures(candidate.bonding_curve, 100):
            slot = int(row.get("slot", -1))
            if row.get("err") or not candidate.slot <= slot <= candidate.slot + self.cfg["block_window"]:
                continue
            if row.get("signature") == candidate.signature:
                continue
            tx = await self.rpc.transaction(row["signature"])
            message = ((tx or {}).get("transaction") or {}).get("message") or {}
            signer = next((key.get("pubkey") for key in message.get("accountKeys", []) if isinstance(key, dict) and key.get("signer")), None)
            if signer and signer != candidate.creator:
                buyers.add(signer)
                early_count += 1
        return buyers, early_count

    async def creator_launches(self, creator: str) -> tuple[int, int]:
        cached = await self.store.get_dev(creator)
        if cached is not None:
            return cached
        launches = 0
        for row in await self.rpc.signatures(creator, self.cfg["max_dev_launches"] + 1):
            tx = await self.rpc.transaction(row.get("signature", "")) if row.get("signature") else None
            logs = ((tx or {}).get("meta") or {}).get("logMessages") or []
            if any("instruction: create" in str(log).lower() for log in logs):
                launches += 1
        await self.store.put_dev(creator, launches, 0)
        return launches, 0

    async def validate(self, candidate: Candidate, tx: dict[str, Any]) -> dict[str, Any] | None:
        curve = await self.curve(candidate.bonding_curve)
        if not curve or curve.complete or curve.real_token_reserves <= 0:
            await self.reject(candidate, "bonding curve absente ou invalide")
            return None
        liquidity = curve.liquidity_sol
        if liquidity < self.cfg["min_liquidity"]:
            await self.reject(candidate, f"liquidité {liquidity:.2f} SOL < {self.cfg['min_liquidity']:.2f}")
            return None
        sol_price = self.cfg["sol_price_usd"]
        market_cap = (curve.virtual_sol_reserves / max(curve.virtual_token_reserves, 1)) * TOTAL_SUPPLY_RAW / 10**TOKEN_DECIMALS * sol_price / LAMPORTS_PER_SOL
        if market_cap < self.cfg["min_market_cap_usd"]:
            await self.reject(candidate, f"MC ${market_cap:,.0f} < ${self.cfg['min_market_cap_usd']:,.0f}")
            return None
        mint_authority, freeze_authority = await self.mint_authorities(candidate.mint)
        if mint_authority is not None or freeze_authority is not None:
            await self.reject(candidate, "mint/freeze authority active")
            return None
        largest, dev_raw = await asyncio.gather(self.largest_accounts(candidate.mint), self.dev_balance(candidate))
        denominator = max(curve.token_total_supply or TOTAL_SUPPLY_RAW, 1)
        holders = sorted((int(item.get("amount", 0)) for item in largest if item.get("address") != candidate.associated_curve), reverse=True)
        top10_pct = sum(holders[:10]) / denominator * 100
        single_pct = (holders[0] / denominator * 100) if holders else 100.0
        dev_pct = dev_raw / denominator * 100
        if single_pct >= self.cfg["max_single_holder"]:
            await self.reject(candidate, f"holder unique {single_pct:.2f}% >= {self.cfg['max_single_holder']:.2f}%")
            return None
        launches, rugs = await self.creator_launches(candidate.creator)
        if launches > self.cfg["max_dev_previous_launches"]:
            await self.reject(candidate, f"historique dev {launches} lancements > {self.cfg['max_dev_previous_launches']}")
            return None
        if dev_pct > self.cfg["max_dev_pct"]:
            await self.reject(candidate, f"dev {dev_pct:.2f}% > {self.cfg['max_dev_pct']:.2f}%")
            return None
        buyers, block_buys = await self.early_buyers(candidate)
        if len(buyers) < self.cfg["min_unique_buyers"]:
            await self.reject(candidate, f"acheteurs distincts {len(buyers)} < {self.cfg['min_unique_buyers']}")
            return None
        if self.cfg["enable_sybil_check"] and candidate.creator in buyers:
            await self.reject(candidate, "acheteur identique au créateur")
            return None
        if top10_pct >= self.cfg["max_top10_pct"]:
            await self.reject(candidate, f"Top 10 {top10_pct:.2f}% >= {self.cfg['max_top10_pct']:.2f}%")
            return None
        progress = max(0.0, min(100.0, (PUMP_INITIAL_REAL_TOKEN_RESERVES - curve.real_token_reserves) / PUMP_INITIAL_REAL_TOKEN_RESERVES * 100))
        block_time = tx.get("blockTime")
        age_seconds = max(0.0, time.time() - float(block_time)) if block_time else 0.0
        return {
            "mint": candidate.mint, "name": candidate.name, "ticker": candidate.ticker,
            "bonding_curve": candidate.bonding_curve, "liquidity_sol": liquidity,
            "market_cap_usd": market_cap, "age_seconds": age_seconds,
            "top10_pct": top10_pct, "dev_pct": dev_pct, "single_holder_pct": single_pct,
            "buyers": len(buyers), "block0_buys": block_buys, "curve_progress_pct": progress,
        }


class TelegramNotifier:
    def __init__(self, token: str, chat_id: str):
        self.bot, self.chat_id = Bot(token), chat_id

    @staticmethod
    def trade_keyboard(mint_address: str) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup([
            [InlineKeyboardButton("📈 Acheter / Gérer sur GMGN", url=f"https://gmgn.ai/sol/token/{mint_address}")],
            [InlineKeyboardButton("⚡ Acheter / Gérer sur Axiom", url=f"https://axiom.trade/token/{mint_address}")],
        ])

    @staticmethod
    def format_alert(data: dict[str, Any]) -> str:
        return (
            f"🎯 <b>{data.get('name', 'TOKEN')} (${data.get('ticker', 'TICKER')}) — Pump.fun</b>\n\n"
            f"💧 Liq: {data['liquidity_sol']:.2f} SOL\n"
            f"📊 MC: ${data['market_cap_usd']:,.0f}\n"
            f"⏳ Âge: {data['age_seconds']:.2f}s (Block 0)\n"
            f"👥 Top 10: {data['top10_pct']:.2f}% | Dev: {data['dev_pct']:.2f}%\n\n"
            f"<code>{data['mint']}</code>"
        )

    async def send_alert(self, data: dict[str, Any]) -> None:
        try:
            await self.bot.send_message(
                chat_id=self.chat_id, text=self.format_alert(data), parse_mode="HTML",
                disable_web_page_preview=True, reply_markup=self.trade_keyboard(data["mint"]),
            )
            logger.info("[TELEGRAM] Première/alerte validée envoyée: %s", data["mint"])
        except TelegramError as exc:
            logger.error("Telegram indisponible pour %s: %s", data["mint"], type(exc).__name__)


class Monitor:
    def __init__(self, session: aiohttp.ClientSession, rpc: SolanaRPC, notifier: TelegramNotifier, store: SeenStore, cfg: dict[str, Any]):
        self.session, self.rpc, self.notifier, self.store, self.cfg = session, rpc, notifier, store, cfg
        self.ws_url = os.getenv("HELIUS_WS_URL", "").strip() or rpc.url.replace("https://", "wss://", 1)
        self.stop = asyncio.Event()
        self.queue: asyncio.Queue[tuple[str, list[str]]] = asyncio.Queue(maxsize=500)
        self.enqueued: set[str] = set()
        self.paused = False
        self.stats = {"tracked": 0, "valid": 0, "rejected": 0, "errors": 0}
        self.validator = Validator(cfg, rpc, store)

    async def process(self, signature: str, logs: list[str]) -> None:
        try:
            tx = await self.rpc.transaction(signature)
            candidate = parse_candidate(tx or {}, signature, logs)
            if not candidate or not await self.store.claim(candidate.mint, signature):
                return
            self.stats["tracked"] += 1
            if self.paused:
                return
            data = await self.validator.validate(candidate, tx or {})
            if data:
                self.stats["valid"] += 1
                logger.info("[FILTER PASSED] %s — Pump.fun on-chain", candidate.mint)
                await self.notifier.send_alert(data)
        except (RpcError, KeyError, TypeError, ValueError) as exc:
            self.stats["errors"] += 1
            if self.stats["errors"] <= 5 or self.stats["errors"] % 100 == 0:
                logger.warning("Erreur traitement #%s: %s", self.stats["errors"], type(exc).__name__)

    async def worker(self) -> None:
        while not self.stop.is_set():
            signature, logs = await self.queue.get()
            try:
                await self.process(signature, logs)
            finally:
                self.enqueued.discard(signature)
                self.queue.task_done()

    async def enqueue(self, signature: str, logs: list[str]) -> None:
        if signature in self.enqueued or self.queue.full():
            return
        self.enqueued.add(signature)
        await self.queue.put((signature, logs))

    async def run(self) -> None:
        workers = [asyncio.create_task(self.worker()) for _ in range(max(1, int(os.getenv("RPC_WORKERS", "2"))))]
        delay = 1.0
        try:
            while not self.stop.is_set():
                try:
                    async with self.session.ws_connect(self.ws_url, heartbeat=20, receive_timeout=45) as ws:
                        await ws.send_json({"jsonrpc": "2.0", "id": 1, "method": "logsSubscribe", "params": [{"mentions": [PUMP_PROGRAM_ID]}, {"commitment": COMMITMENT}]})
                        ack = await ws.receive_json()
                        if "error" in ack:
                            raise RuntimeError("logsSubscribe refusé")
                        logger.info("[PUMPFUN] Stream actif; workers=%s", len(workers))
                        delay = 1.0
                        async for message in ws:
                            if message.type != aiohttp.WSMsgType.TEXT:
                                continue
                            value = json.loads(message.data).get("params", {}).get("result", {}).get("value", {})
                            if not value.get("err") and value.get("signature"):
                                await self.enqueue(value["signature"], value.get("logs", []))
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    logger.warning("[PUMPFUN] Stream interrompu (%s), reconnexion %.1fs", type(exc).__name__, delay)
                    with suppress(asyncio.TimeoutError):
                        await asyncio.wait_for(self.stop.wait(), timeout=delay)
                    delay = min(30.0, delay * 2)
        finally:
            self.stop.set()
            for worker in workers:
                worker.cancel()
            await asyncio.gather(*workers, return_exceptions=True)


async def health(request: web.Request) -> web.Response:
    monitor = request.app.get("monitor")
    payload = {"status": "ok", "service": "pumpfun-onchain", "source": "pump.fun", "dry_run": True}
    if monitor:
        payload.update(monitor.stats)
    return web.json_response(payload)


async def main() -> None:
    rpc_url = os.getenv("HELIUS_RPC_URL", "").strip()
    telegram_token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip() or os.getenv("TELEGRAM_CHANNEL_ID", "").strip()
    if not rpc_url or not telegram_token or not chat_id:
        raise RuntimeError("HELIUS_RPC_URL, TELEGRAM_BOT_TOKEN et TELEGRAM_CHAT_ID sont obligatoires")
    cfg = {
        "min_liquidity": float(os.getenv("MIN_LIQUIDITY_SOL", "1.5")),
        "min_market_cap_usd": float(os.getenv("MIN_MARKET_CAP_USD", "4000")),
        "sol_price_usd": float(os.getenv("SOL_PRICE_USD", "150")),
        "min_unique_buyers": int(os.getenv("MIN_UNIQUE_BUYERS", "3")),
        "max_single_holder": float(os.getenv("MAX_SINGLE_HOLDER_PCT", "15.0")),
        "max_top10_pct": float(os.getenv("MAX_TOP10_PCT", "18.0")),
        "max_dev_pct": float(os.getenv("MAX_DEV_PCT", "5")),
        "max_dev_previous_launches": int(os.getenv("MAX_DEV_PREVIOUS_LAUNCHES", "8")),
        "max_dev_launches": int(os.getenv("MAX_DEV_PREVIOUS_LAUNCHES", "8")),
        "enable_sybil_check": env_bool("ENABLE_SYBIL_CHECK", True),
        "block_window": int(os.getenv("BLOCK_WINDOW", "3")),
    }
    dry_run = env_bool("DRY_RUN", True)
    if not dry_run:
        raise RuntimeError("DRY_RUN=false est interdit dans cette architecture d’alerte-only")
    logger.info("[SAFETY] DRY_RUN=True; aucune transaction signée ou transmise")
    logger.info("[CONFIG] source=Pump.fun; liq>=%.2f SOL; MC>= $%.0f; buyers>=%s; single<%.1f%%; dev_launches<=%s; sybil=%s; momentum_required=%s", cfg["min_liquidity"], cfg["min_market_cap_usd"], cfg["min_unique_buyers"], cfg["max_single_holder"], cfg["max_dev_previous_launches"], cfg["enable_sybil_check"], env_bool("REQUIRE_MOMENTUM", False))

    app = web.Application()
    app.router.add_get("/", health)
    app.router.add_get("/health", health)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", int(os.getenv("PORT", "10000"))).start()
    store = SeenStore(os.getenv("SEEN_DB_PATH", "data/seen.sqlite3"))
    stop = asyncio.Event()
    try:
        async with aiohttp.ClientSession() as session:
            rpc = SolanaRPC(session, rpc_url)
            notifier = TelegramNotifier(telegram_token, chat_id)
            monitor = Monitor(session, rpc, notifier, store, cfg)
            app["monitor"] = monitor
            loop = asyncio.get_running_loop()
            for sig in (signal.SIGINT, signal.SIGTERM):
                loop.add_signal_handler(sig, stop.set)
            monitor_task = asyncio.create_task(monitor.run())
            logger.info("[PUMPFUN] Stream démarré; boutons GMGN/Axiom prêts")
            await stop.wait()
            monitor.stop.set()
            monitor_task.cancel()
            with suppress(asyncio.CancelledError):
                await monitor_task
    finally:
        store.close()
        await runner.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
