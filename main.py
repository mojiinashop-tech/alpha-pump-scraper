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
from typing import Any, Optional

import aiohttp
from aiohttp import web
from telegram import Bot
from telegram.error import TelegramError

logging.basicConfig(
    stream=sys.stdout,
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO),
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("alpha-pump")
for name in ("telegram", "httpx", "httpcore"):
    logging.getLogger(name).setLevel(logging.WARNING)

LAMPORTS_PER_SOL = 1_000_000_000
TOKEN_DECIMALS = 6
TOTAL_SUPPLY_RAW = 1_000_000_000 * 10**TOKEN_DECIMALS
PUMP_PROGRAM_ID = os.getenv("PUMP_PROGRAM_ID", "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P")
COMMITMENT = os.getenv("SOLANA_COMMITMENT", "confirmed")


def env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    return default if value is None else value.strip().lower() in {"1", "true", "yes", "on"}


class RpcError(RuntimeError):
    pass


class RateLimiter:
    def __init__(self, rps: float):
        self.interval = 1.0 / max(0.1, rps)
        self.next_at = 0.0
        self.lock = asyncio.Lock()

    async def wait(self) -> None:
        async with self.lock:
            now = time.monotonic()
            delay = self.next_at - now
            self.next_at = max(now, self.next_at) + self.interval
        if delay > 0:
            await asyncio.sleep(delay)

    def penalize(self, seconds: float) -> None:
        self.next_at = max(self.next_at, time.monotonic() + seconds)


class SolanaRPC:
    def __init__(self, session: aiohttp.ClientSession, url: str):
        self.session = session
        self.url = url
        self.counter = 0
        self.limiter = RateLimiter(float(os.getenv("RPC_MAX_RPS", "3")))
        self.semaphore = asyncio.Semaphore(max(1, int(os.getenv("RPC_CONCURRENCY", "2"))))
        self.timeout = aiohttp.ClientTimeout(total=float(os.getenv("RPC_TIMEOUT_SECONDS", "10")))
        self.rate_limits = 0

    async def call(self, method: str, params: list[Any], retries: int = 3) -> Any:
        delay = 1.0
        last_error = "unknown"
        for attempt in range(retries):
            await self.limiter.wait()
            self.counter += 1
            payload = {"jsonrpc": "2.0", "id": self.counter, "method": method, "params": params}
            try:
                async with self.semaphore:
                    async with self.session.post(self.url, json=payload, timeout=self.timeout) as response:
                        if response.status == 429:
                            self.rate_limits += 1
                            wait = min(30.0, float(response.headers.get("Retry-After", delay)))
                            self.limiter.penalize(wait)
                            if self.rate_limits <= 3 or self.rate_limits % 25 == 0:
                                logger.warning("Helius 429 #%s sur %s; attente %.1fs", self.rate_limits, method, wait)
                            if method == "getTransaction":
                                return None
                            await asyncio.sleep(wait)
                            last_error = "HTTP 429"
                            continue
                        if response.status >= 500:
                            last_error = f"HTTP {response.status}"
                            await asyncio.sleep(delay)
                            delay = min(delay * 2, 8.0)
                            continue
                        response.raise_for_status()
                        body = await response.json(content_type=None)
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                last_error = type(exc).__name__
                if attempt + 1 < retries:
                    await asyncio.sleep(delay)
                    delay = min(delay * 2, 8.0)
                    continue
                raise RpcError(f"{method}: {last_error}") from None
            if body.get("error"):
                raise RpcError(f"{method}: {body['error']}")
            return body.get("result")
        raise RpcError(f"{method}: échec après {retries} essais ({last_error})")

    async def transaction(self, signature: str) -> Optional[dict[str, Any]]:
        return await self.call("getTransaction", [signature, {
            "encoding": "jsonParsed", "commitment": COMMITMENT,
            "maxSupportedTransactionVersion": 0,
        }])


@dataclass
class Candidate:
    mint: str
    creator: str
    bonding_curve: str
    associated_curve: str
    signature: str
    slot: int


@dataclass
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


def parse_curve_account(raw: bytes) -> CurveState | None:
    # Anchor account discriminator (8 bytes) + five u64 + bool.
    if len(raw) < 8 + 5 * 8 + 1:
        return None
    try:
        values = struct.unpack_from("<5QB", raw, 8)
        return CurveState(*values)
    except struct.error:
        return None


def account_keys(tx: dict[str, Any]) -> list[str]:
    keys = tx["transaction"]["message"].get("accountKeys", [])
    return [item.get("pubkey") if isinstance(item, dict) else item for item in keys]


def parse_candidate(tx: dict[str, Any], signature: str, logs: list[str]) -> Candidate | None:
    if (tx.get("meta") or {}).get("err"):
        return None
    log_text = " ".join(logs).lower()
    if "instruction: create" not in log_text and "instruction: createv2" not in log_text:
        return None
    keys = account_keys(tx)
    message = tx.get("transaction", {}).get("message", {})
    for instruction in message.get("instructions", []):
        if instruction.get("programId") != PUMP_PROGRAM_ID:
            continue
        accounts = instruction.get("accounts") or []
        if not accounts:
            continue
        if all(isinstance(a, int) for a in accounts):
            if max(accounts) >= len(keys):
                continue
            accounts = [keys[a] for a in accounts]
        if len(accounts) < 4 or not all(isinstance(a, str) for a in accounts):
            continue
        return Candidate(
            mint=accounts[0],
            creator=accounts[-1],
            bonding_curve=accounts[2],
            associated_curve=accounts[3],
            signature=signature,
            slot=int(tx.get("slot", 0) or 0),
        )
    return None


class Validator:
    def __init__(self, cfg: dict[str, Any], rpc: SolanaRPC):
        self.cfg = cfg
        self.rpc = rpc

    async def curve(self, address: str) -> CurveState | None:
        result = await self.rpc.call("getAccountInfo", [address, {
            "encoding": "base64", "commitment": COMMITMENT,
        }])
        value = (result or {}).get("value")
        if not value or value.get("owner") != PUMP_PROGRAM_ID:
            return None
        data = value.get("data")
        if not isinstance(data, list) or not data:
            return None
        try:
            return parse_curve_account(base64.b64decode(data[0]))
        except (ValueError, TypeError):
            return None

    async def mint_authorities(self, mint: str) -> tuple[Any, Any]:
        result = await self.rpc.call("getAccountInfo", [mint, {
            "encoding": "jsonParsed", "commitment": COMMITMENT,
        }])
        value = (result or {}).get("value") or {}
        data = value.get("data") or {}
        info = ((data.get("parsed") or {}).get("info") or {}) if isinstance(data, dict) else {}
        return info.get("mintAuthority"), info.get("freezeAuthority")

    async def largest_accounts(self, mint: str) -> list[dict[str, Any]]:
        result = await self.rpc.call("getTokenLargestAccounts", [mint, {"commitment": COMMITMENT}])
        return (result or {}).get("value", [])

    async def dev_balance(self, candidate: Candidate, mint: str) -> int:
        result = await self.rpc.call("getTokenAccountsByOwner", [
            candidate.creator, {"mint": mint},
            {"encoding": "jsonParsed", "commitment": COMMITMENT},
        ])
        total = 0
        for item in (result or {}).get("value", []):
            try:
                total += int(item["account"]["data"]["parsed"]["info"]["tokenAmount"]["amount"])
            except (KeyError, TypeError, ValueError):
                continue
        return total

    async def current_slot(self) -> int:
        return int(await self.rpc.call("getSlot", [{"commitment": COMMITMENT}]) or 0)

    async def bundles(self, candidate: Candidate) -> tuple[int, float, str]:
        max_txs = int(self.cfg["max_block0_buys"])
        window = int(self.cfg["bundle_slot_window"])
        result = await self.rpc.call("getSignaturesForAddress", [candidate.bonding_curve, {
            "limit": 100, "commitment": COMMITMENT,
        }]) or []
        early = [row for row in result if candidate.slot <= int(row.get("slot", -1)) <= candidate.slot + window
                 and row.get("signature") != candidate.signature and not row.get("err")]
        if len(early) > max_txs:
            return len(early), 100.0, f"{len(early)} achats au bloc 0"
        bundle_raw = 0
        for row in early:
            tx = await self.rpc.transaction(row["signature"])
            for balance in ((tx or {}).get("meta") or {}).get("postTokenBalances") or []:
                if balance.get("mint") == candidate.mint:
                    try:
                        bundle_raw += int(balance["uiTokenAmount"]["amount"])
                    except (KeyError, TypeError, ValueError):
                        pass
        percentage = bundle_raw / TOTAL_SUPPLY_RAW * 100
        return len(early), percentage, f"{len(early)} achat(s), {percentage:.2f}% supply"

    async def validate(self, candidate: Candidate) -> dict[str, Any] | None:
        curve = await self.curve(candidate.bonding_curve)
        if not curve or curve.complete or curve.real_token_reserves <= 0:
            return None
        liquidity = curve.liquidity_sol
        if not (self.cfg["min_liquidity"] <= liquidity <= self.cfg["max_liquidity"]):
            return None
        mint_auth, freeze_auth = await self.mint_authorities(candidate.mint)
        if self.cfg["require_mint_renounced"] and mint_auth is not None:
            return None
        if freeze_auth is not None:
            return None
        largest, dev_raw, slot = await asyncio.gather(
            self.largest_accounts(candidate.mint),
            self.dev_balance(candidate, candidate.mint),
            self.current_slot(),
        )
        curve_raw = next((int(x.get("amount", 0)) for x in largest if x.get("address") == candidate.associated_curve), 0)
        denominator = max(curve.token_total_supply or TOTAL_SUPPLY_RAW, 1)
        holders = sorted((int(x.get("amount", 0)) for x in largest if x.get("address") != candidate.associated_curve), reverse=True)
        top10_pct = sum(holders[:10]) / denominator * 100
        dev_pct = dev_raw / denominator * 100
        if top10_pct > self.cfg["max_top10"] or dev_pct > self.cfg["max_dev"]:
            return None
        block0_buys, bundle_pct, bundle_detail = await self.bundles(candidate)
        if block0_buys > self.cfg["max_block0_buys"] or bundle_pct > self.cfg["max_bundle"]:
            return None
        age_minutes = max(0.0, (slot - candidate.slot) / 150.0 / 60.0)
        return {
            "mint": candidate.mint, "signature": candidate.signature,
            "liquidity_sol": liquidity, "top10_pct": top10_pct, "dev_pct": dev_pct,
            "bundle_pct": bundle_pct, "block0_buys": block0_buys,
            "age_minutes": age_minutes, "bundle_detail": bundle_detail,
        }


class DexScreener:
    def __init__(self, session: aiohttp.ClientSession):
        self.session = session

    async def enrich(self, data: dict[str, Any]) -> dict[str, Any] | None:
        url = f"https://api.dexscreener.com/latest/dex/tokens/{data['mint']}"
        try:
            async with self.session.get(url, timeout=8) as response:
                if response.status >= 400:
                    return None
                body = await response.json(content_type=None)
        except (aiohttp.ClientError, asyncio.TimeoutError, ValueError):
            return None
        pairs = [p for p in body.get("pairs", []) if p.get("chainId") == "solana"]
        if not pairs:
            return None
        pair = max(pairs, key=lambda p: float((p.get("liquidity") or {}).get("usd") or 0))
        boosts = (pair.get("boosts") or {}).get("active", 0)
        liquidity_usd = float((pair.get("liquidity") or {}).get("usd") or 0)
        market_cap = float(pair.get("marketCap") or pair.get("fdv") or 0)
        if boosts <= 0:
            return None
        return {**data, "dex_paid": True, "market_cap_usd": market_cap,
                "dex_liquidity_usd": liquidity_usd, "name": (pair.get("baseToken") or {}).get("name", "Inconnu"),
                "symbol": (pair.get("baseToken") or {}).get("symbol", "N/A")}


class TelegramNotifier:
    def __init__(self, token: str, chat_id: str):
        self.bot = Bot(token=token)
        self.chat_id = chat_id

    @staticmethod
    def format_alert(data: dict[str, Any], score: float) -> str:
        mint = data["mint"]
        return (
            f"🚀 <b>NOUVEAU TOKEN VALIDE</b> — Score {score:.1f}/100\n\n"
            f"🪙 <b>{data.get('name', 'Inconnu')}</b> (${data.get('symbol', 'N/A')})\n"
            f"💧 Liquidité bonding curve : {data['liquidity_sol']:.2f} SOL\n"
            f"📊 Market Cap : ${data['market_cap_usd']:,.0f}\n"
            f"👥 Top 10 : {data['top10_pct']:.2f}% | Dev : {data['dev_pct']:.2f}%\n"
            f"📦 Bloc 0 : {data['block0_buys']} achat(s), {data['bundle_pct']:.2f}% supply\n\n"
            f"<code>{mint}</code>\n\n"
            f"🔗 <a href='https://dexscreener.com/solana/{mint}'>Dexscreener</a> • "
            f"<a href='https://gmgn.ai/sol/token/{mint}'>GMGN</a> • "
            f"<a href='https://axiom.trade/t/{mint}'>Axiom</a>\n"
            f"🔗 <a href='https://pump.fun/coin/{mint}'>Pump.fun</a> • "
            f"<a href='https://solscan.io/token/{mint}'>Solscan</a>"
        )

    async def send(self, data: dict[str, Any], score: float) -> None:
        try:
            await self.bot.send_message(chat_id=self.chat_id, text=self.format_alert(data, score), parse_mode="HTML", disable_web_page_preview=True)
            logger.info("[TELEGRAM] Alerte envoyée %s", data["mint"])
        except TelegramError as exc:
            logger.error("Telegram indisponible: %s", type(exc).__name__)


class SeenStore:
    def __init__(self, path: str):
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        self.db = sqlite3.connect(path, check_same_thread=False)
        self.db.execute("CREATE TABLE IF NOT EXISTS seen (mint TEXT PRIMARY KEY, signature TEXT NOT NULL)")
        self.db.commit()
        self.lock = asyncio.Lock()

    async def claim(self, mint: str, signature: str) -> bool:
        async with self.lock:
            try:
                self.db.execute("INSERT INTO seen(mint, signature) VALUES (?, ?)", (mint, signature))
                self.db.commit()
                return True
            except sqlite3.IntegrityError:
                return False

    def close(self) -> None:
        self.db.close()


class Monitor:
    def __init__(self, session: aiohttp.ClientSession, rpc: SolanaRPC, validator: Validator,
                 dex: DexScreener, notifier: TelegramNotifier, store: SeenStore):
        self.session, self.rpc, self.validator = session, rpc, validator
        self.dex, self.notifier, self.store = dex, notifier, store
        self.ws_url = os.getenv("HELIUS_WS_URL", "").strip()
        if not self.ws_url and rpc.url.startswith("https://"):
            self.ws_url = rpc.url.replace("https://", "wss://", 1)
        self.stop = asyncio.Event()
        self.queue: asyncio.Queue[tuple[str, list[str]]] = asyncio.Queue(maxsize=500)
        self.enqueued: set[str] = set()
        self.lock = asyncio.Lock()
        self.workers_count = max(1, int(os.getenv("RPC_WORKERS", "1")))

    async def process(self, signature: str, logs: list[str]) -> None:
        try:
            tx = await self.rpc.transaction(signature)
            if not tx:
                return
            candidate = parse_candidate(tx, signature, logs)
            if not candidate or not await self.store.claim(candidate.mint, signature):
                return
            data = await self.validator.validate(candidate)
            if not data:
                logger.info("[FILTER REJECTED] %s — validation on-chain", candidate.mint)
                return
            data = await self.dex.enrich(data)
            if not data:
                logger.info("[FILTER REJECTED] %s — DEX non payé ou métriques DexScreener absentes", candidate.mint)
                return
            mc = float(data.get("market_cap_usd", 0))
            if mc < float(os.getenv("MIN_MARKET_CAP_USD", "5000")):
                logger.info("[FILTER REJECTED] %s — Market Cap %.0f inférieur au minimum", candidate.mint, mc)
                return
            score = 100.0
            await self.notifier.send(data, score)
        except (RpcError, KeyError, TypeError, ValueError) as exc:
            logger.warning("Traitement ignoré (%s): %s", type(exc).__name__, signature)

    async def worker(self) -> None:
        while not self.stop.is_set():
            signature, logs = await self.queue.get()
            try:
                await self.process(signature, logs)
            finally:
                self.enqueued.discard(signature)
                self.queue.task_done()

    async def enqueue(self, signature: str, logs: list[str]) -> None:
        async with self.lock:
            if signature in self.enqueued or self.queue.full():
                return
            self.enqueued.add(signature)
            await self.queue.put((signature, logs))

    async def run(self) -> None:
        if not self.ws_url:
            raise RuntimeError("HELIUS_WS_URL manquant")
        workers = [asyncio.create_task(self.worker()) for _ in range(self.workers_count)]
        delay = 1.0
        try:
            while not self.stop.is_set():
                try:
                    async with self.session.ws_connect(self.ws_url, heartbeat=20, receive_timeout=45) as ws:
                        await ws.send_json({"jsonrpc": "2.0", "id": 1, "method": "logsSubscribe",
                                            "params": [{"mentions": [PUMP_PROGRAM_ID]}, {"commitment": COMMITMENT}]})
                        ack = await ws.receive_json()
                        if "error" in ack:
                            raise RuntimeError(ack["error"])
                        logger.info("Souscription Helius active; mode observation=False; workers=%s", self.workers_count)
                        delay = 1.0
                        async for message in ws:
                            if message.type != aiohttp.WSMsgType.TEXT:
                                continue
                            value = json.loads(message.data).get("params", {}).get("result", {}).get("value", {})
                            if value.get("err") or not value.get("signature"):
                                continue
                            await self.enqueue(value["signature"], value.get("logs", []))
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    logger.error("WebSocket coupé (%s), reconnexion dans %.1fs", type(exc).__name__, delay)
                    with suppress(asyncio.TimeoutError):
                        await asyncio.wait_for(self.stop.wait(), timeout=delay)
                    delay = min(30.0, delay * 2)
        finally:
            self.stop.set()
            for task in workers:
                task.cancel()
            await asyncio.gather(*workers, return_exceptions=True)


async def health(request: web.Request) -> web.Response:
    return web.json_response({"status": "ok", "service": "alpha-pump-scraper", "observation_mode": False})


async def main() -> None:
    rpc_url = os.getenv("HELIUS_RPC_URL", "").strip()
    ws_url = os.getenv("HELIUS_WS_URL", "").strip()
    if not ws_url and rpc_url.startswith("https://"):
        ws_url = rpc_url.replace("https://", "wss://", 1)
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = (os.getenv("TELEGRAM_CHAT_ID", "").strip() or os.getenv("TELEGRAM_CHANNEL_ID", "").strip())
    if not rpc_url or not ws_url or not token or not chat_id:
        raise RuntimeError("HELIUS_RPC_URL, HELIUS_WS_URL, TELEGRAM_BOT_TOKEN et TELEGRAM_CHAT_ID sont obligatoires")
    cfg = {
        "min_liquidity": float(os.getenv("MIN_LIQUIDITY_SOL", "1.5")),
        "max_liquidity": float(os.getenv("MAX_LIQUIDITY_SOL", "30")),
        "max_top10": float(os.getenv("MAX_TOP10_PCT", "20")),
        "max_dev": float(os.getenv("MAX_DEV_PCT", "5")),
        "max_bundle": float(os.getenv("MAX_BUNDLE_PCT", "5")),
        "max_block0_buys": int(os.getenv("MAX_BLOCK0_BUYS", "1")),
        "bundle_slot_window": int(os.getenv("BUNDLE_SLOT_WINDOW", "0")),
        "require_mint_renounced": env_bool("REQUIRE_MINT_RENOUNCED", True),
    }
    app = web.Application()
    app.router.add_get("/", health)
    app.router.add_get("/health", health)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", int(os.getenv("PORT", "10000")))
    await site.start()
    store = SeenStore(os.getenv("SEEN_DB_PATH", "data/seen.sqlite3"))
    try:
        async with aiohttp.ClientSession() as session:
            rpc = SolanaRPC(session, rpc_url)
            validator = Validator(cfg, rpc)
            notifier = TelegramNotifier(token, chat_id)
            monitor = Monitor(session, rpc, validator, DexScreener(session), notifier, store)
            logger.info("Configuration stricte active: liquidité %.2f-%.2f SOL; MC minimum $%.0f; DEX payé obligatoire", cfg["min_liquidity"], cfg["max_liquidity"], float(os.getenv("MIN_MARKET_CAP_USD", "5000")))
            loop = asyncio.get_running_loop()
            stop = asyncio.Event()
            for sig in (signal.SIGINT, signal.SIGTERM):
                loop.add_signal_handler(sig, stop.set)
            task = asyncio.create_task(monitor.run())
            try:
                await asyncio.wait_for(
                    notifier.bot.send_message(
                        chat_id=chat_id,
                        text="💎 <b>QUANTUM ALPHA BOT — MODE DIRECT PRÊT</b>\n<i>Validation on-chain active, observation OFF.</i>",
                        parse_mode="HTML",
                    ),
                    timeout=10,
                )
            except (TelegramError, asyncio.TimeoutError) as exc:
                logger.warning("Notification Telegram de démarrage ignorée: %s", type(exc).__name__)
            await stop.wait()
            monitor.stop.set()
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task
    finally:
        store.close()
        await runner.cleanup()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
