
from __future__ import annotations

import asyncio
import json
import logging
import os
import random
import re
import signal
import sqlite3
import time
from collections import OrderedDict
from dataclasses import dataclass
from typing import Any

import aiohttp
from aiohttp import web
import websockets
from dotenv import load_dotenv

load_dotenv()

LOG = logging.getLogger("pumpfun-alert")
LAMPORTS_PER_SOL = 1_000_000_000
DEFAULT_COMMITMENT = "confirmed"
DEFAULT_PUMP_PROGRAM_ID = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"

TX_FETCH_DELAYS = (0.0, 0.5, 1.0)

_CREATE_LOG_RE = re.compile(r"instruction:\s*create(v2)?\s*$", re.IGNORECASE)
_SECRET_PATTERNS = [
    (re.compile(r"(api[-_]?key=)[^&\s'\"]+", re.IGNORECASE), r"\1***"),
    (re.compile(r"(bot)\d+:[A-Za-z0-9_-]+"), r"\1***"),
]


def redact(text: str) -> str:
    for pattern, repl in _SECRET_PATTERNS:
        text = pattern.sub(repl, text)
    return text


class RedactingFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        return redact(super().format(record))


def setup_logging() -> None:
    handler = logging.StreamHandler()
    handler.setFormatter(RedactingFormatter("%(asctime)s %(levelname)s %(message)s"))
    level = getattr(logging, env("LOG_LEVEL", "INFO").upper(), logging.INFO)
    logging.basicConfig(level=level, handlers=[handler], force=True)


def env(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


class RPCError(Exception):
    pass


@dataclass(frozen=True)
class Config:
    http_url: str
    ws_url: str
    telegram_token: str
    telegram_chat_id: str
    pump_program_id: str
    min_liquidity_sol: float
    max_top10_percent: float
    max_dev_percent: float
    lp_mode: str
    lp_status_url_template: str
    timeout: float
    max_concurrency: int
    reconnect_min: float
    reconnect_max: float
    db_path: str
    port: int
    rpc_max_rps: float
    rpc_max_retries: int
    rpc_backoff_max: float
    queue_size: int

    @classmethod
    def from_env(cls) -> "Config":
        required = ["SOLANA_HTTP_URL", "SOLANA_WS_URL", "TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID"]
        missing = [key for key in required if not env(key)]
        if missing:
            raise RuntimeError(f"Variables manquantes: {', '.join(missing)}")
        return cls(
            http_url=env("SOLANA_HTTP_URL"),
            ws_url=env("SOLANA_WS_URL"),
            telegram_token=env("TELEGRAM_BOT_TOKEN"),
            telegram_chat_id=env("TELEGRAM_CHAT_ID"),
            pump_program_id=env("PUMP_PROGRAM_ID", DEFAULT_PUMP_PROGRAM_ID),
            min_liquidity_sol=float(env("MIN_LIQUIDITY_SOL", "3")),
            max_top10_percent=float(env("MAX_TOP10_PERCENT", "20")),
            max_dev_percent=float(env("MAX_DEV_PERCENT", "5")),
            lp_mode=env("LP_VERIFICATION_MODE", "strict").lower(),
            lp_status_url_template=env("LP_STATUS_URL_TEMPLATE"),
            timeout=float(env("RPC_TIMEOUT_SECONDS", "8")),
            max_concurrency=int(env("MAX_CONCURRENT_VALIDATIONS", "4")),
            reconnect_min=float(env("RECONNECT_MIN_SECONDS", "0.25")),
            reconnect_max=float(env("RECONNECT_MAX_SECONDS", "5")),
            db_path=env("DATABASE_PATH", "./data/alerts.sqlite3"),
            port=int(env("PORT", "10000")),
            rpc_max_rps=float(env("RPC_MAX_RPS", "8")),
            rpc_max_retries=int(env("RPC_MAX_RETRIES", "4")),
            rpc_backoff_max=float(env("RPC_BACKOFF_MAX_SECONDS", "30")),
            queue_size=int(env("QUEUE_MAX_SIZE", "200")),
        )


@dataclass
class Candidate:
    mint: str
    creator: str
    bonding_curve: str
    signature: str
    name: str = "Inconnu"
    symbol: str = "?"


@dataclass
class ValidatedToken:
    candidate: Candidate
    liquidity_sol: float
    dev_percent: float
    top10_percent: float


class SeenDB:
    def __init__(self, path: str):
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        self.db = sqlite3.connect(path, check_same_thread=False)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("CREATE TABLE IF NOT EXISTS seen (mint TEXT PRIMARY KEY, signature TEXT, seen_at INTEGER NOT NULL)")
        self.db.commit()
        self.lock = asyncio.Lock()

    async def claim(self, mint: str, signature: str) -> bool:
        async with self.lock:
            try:
                self.db.execute("INSERT INTO seen(mint, signature, seen_at) VALUES (?, ?, ?)", (mint, signature, int(time.time())))
                self.db.commit()
                return True
            except sqlite3.IntegrityError:
                return False

    def close(self):
        self.db.close()


class RateLimiter:
    def __init__(self, rate: float):
        self.interval = 1.0 / rate if rate > 0 else 0.0
        self._next = 0.0
        self._blocked_until = 0.0
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        loop = asyncio.get_running_loop()
        while True:
            async with self._lock:
                now = loop.time()
                start = max(now, self._next, self._blocked_until)
                self._next = start + self.interval
            delay = start - now
            if delay > 0:
                await asyncio.sleep(delay)
            if self._blocked_until <= loop.time():
                return

    def penalize(self, seconds: float) -> None:
        loop = asyncio.get_running_loop()
        self._blocked_until = max(self._blocked_until, loop.time() + seconds)


def _retry_after(response: aiohttp.ClientResponse, fallback: float) -> float:
    raw = response.headers.get("Retry-After")
    if not raw:
        return fallback
    try:
        return max(float(raw), 0.0)
    except ValueError:
        return fallback


class SolanaRPC:
    def __init__(self, cfg: Config, session: aiohttp.ClientSession, limiter: RateLimiter):
        self.cfg = cfg
        self.session = session
        self.limiter = limiter
        self.rpc_id = 0

    def _backoff(self, attempt: int, base: float = 0.5) -> float:
        return min(self.cfg.rpc_backoff_max, base * (2 ** attempt)) + random.uniform(0, 0.25)

    async def call(self, method: str, params: list[Any]) -> Any:
        self.rpc_id += 1
        body = {"jsonrpc": "2.0", "id": self.rpc_id, "method": method, "params": params}
        timeout = aiohttp.ClientTimeout(total=self.cfg.timeout)
        attempts = self.cfg.rpc_max_retries + 1
        last_error = "inconnue"

        for attempt in range(attempts):
            await self.limiter.acquire()
            retry_delay: float | None = None
            payload: Any = None
            try:
                async with self.session.post(self.cfg.http_url, json=body, timeout=timeout) as response:
                    status = response.status
                    if status == 429:
                        wait = _retry_after(response, self._backoff(attempt, base=1.0))
                        self.limiter.penalize(wait)
                        last_error = "HTTP 429"
                        retry_delay = 0.0
                        LOG.warning("429 Too Many Requests (%s) - pause globale %.1fs", method, wait)
                    elif status >= 500:
                        last_error = f"HTTP {status}"
                        retry_delay = self._backoff(attempt)
                    elif status >= 400:
                        raise RPCError(f"{method}: HTTP {status}")
                    else:
                        payload = await response.json(content_type=None)
            except (aiohttp.ClientError, asyncio.TimeoutError, ValueError) as exc:
                last_error = type(exc).__name__
                retry_delay = self._backoff(attempt)

            if retry_delay is not None:
                if retry_delay > 0:
                    await asyncio.sleep(retry_delay)
                continue

            if not isinstance(payload, dict):
                raise RPCError(f"{method}: réponse invalide")
            if payload.get("error"):
                raise RPCError(f"{method}: {payload['error']}")
            return payload.get("result")

        raise RPCError(f"{method}: échec après {attempts} tentatives ({last_error})")

    async def transaction(self, signature: str) -> dict[str, Any] | None:
        return await self.call("getTransaction", [signature, {"encoding": "jsonParsed", "commitment": DEFAULT_COMMITMENT, "maxSupportedTransactionVersion": 0}])

    async def account(self, pubkey: str) -> dict[str, Any] | None:
        return await self.call("getAccountInfo", [pubkey, {"encoding": "jsonParsed", "commitment": DEFAULT_COMMITMENT}])

    async def balance(self, pubkey: str) -> int:
        result = await self.call("getBalance", [pubkey, {"commitment": DEFAULT_COMMITMENT}])
        return int(result["value"])

    async def supply(self, mint: str) -> int:
        result = await self.call("getTokenSupply", [mint, {"commitment": DEFAULT_COMMITMENT}])
        return int(result["value"]["amount"])

    async def largest_accounts(self, mint: str) -> list[dict[str, Any]]:
        result = await self.call("getTokenLargestAccounts", [mint, {"commitment": DEFAULT_COMMITMENT}])
        return result["value"]

    async def token_accounts_by_owner(self, owner: str, mint: str) -> list[dict[str, Any]]:
        result = await self.call("getTokenAccountsByOwner", [owner, {"mint": mint}, {"encoding": "jsonParsed", "commitment": DEFAULT_COMMITMENT}])
        return result["value"]


class LPVerifier:
    def __init__(self, cfg: Config, session: aiohttp.ClientSession):
        self.cfg = cfg
        self.session = session

    async def burned_100(self, mint: str) -> bool:
        if self.cfg.lp_mode != "strict" or not self.cfg.lp_status_url_template:
            return False
        url = self.cfg.lp_status_url_template.format(mint=mint)
        timeout = aiohttp.ClientTimeout(total=self.cfg.timeout)
        try:
            async with self.session.get(url, timeout=timeout) as response:
                response.raise_for_status()
                data = await response.json()
            if data.get("lp_burned") is True:
                return True
            return float(data.get("burned_percent", -1)) >= 100.0
        except Exception as exc:
            LOG.warning("Statut LP indisponible pour %s: %s", mint, redact(str(exc)))
            return False


class Validator:
    def __init__(self, cfg: Config, rpc: SolanaRPC, lp: LPVerifier):
        self.cfg, self.rpc, self.lp = cfg, rpc, lp

    @staticmethod
    def _parsed_account(account: dict[str, Any] | None) -> dict[str, Any] | None:
        try:
            return account["value"]["data"]["parsed"]["info"]
        except (TypeError, KeyError):
            return None

    async def validate(self, c: Candidate) -> ValidatedToken | None:
        try:
            curve_balance = await self.rpc.balance(c.bonding_curve)
            liquidity_sol = curve_balance / LAMPORTS_PER_SOL
            if liquidity_sol <= self.cfg.min_liquidity_sol:
                return None

            mint_info = self._parsed_account(await self.rpc.account(c.mint))
            if not mint_info or mint_info.get("mintAuthority") is not None:
                return None

            if not await self.lp.burned_100(c.mint):
                return None

            total_supply = await self.rpc.supply(c.mint)
            if total_supply <= 0:
                return None
            
            largest = await self.rpc.largest_accounts(c.mint)
            curve_accounts = await self.rpc.token_accounts_by_owner(c.bonding_curve, c.mint)
            curve_token_accounts = {x["pubkey"] for x in curve_accounts}
            holders = [x for x in largest if x["address"] not in curve_token_accounts]
            top10_amount = sum(int(x["amount"]) for x in holders[:10])
            top10_percent = 100 * top10_amount / total_supply
            if top10_percent >= self.cfg.max_top10_percent:
                return None

            creator_accounts = await self.rpc.token_accounts_by_owner(c.creator, c.mint)
            dev_amount = sum(int(x["account"]["data"]["parsed"]["info"]["tokenAmount"]["amount"]) for x in creator_accounts)
            dev_percent = 100 * dev_amount / total_supply
            if dev_percent >= self.cfg.max_dev_percent:
                return None

            return ValidatedToken(c, liquidity_sol, dev_percent, top10_percent)
        except asyncio.CancelledError:
            raise
        except RPCError as exc:
            LOG.warning("Validation impossible pour %s (API): %s", c.mint, redact(str(exc)))
            return None
        except (KeyError, IndexError, TypeError, ValueError) as exc:
            LOG.warning("Réponse inattendue pour %s: %s: %s", c.mint, type(exc).__name__, redact(str(exc)))
            return None
        except Exception as exc:
            LOG.warning("Validation rejetée pour %s: %s", c.mint, redact(str(exc)))
            return None


class Telegram:
    def __init__(self, cfg: Config, session: aiohttp.ClientSession):
        self.cfg, self.session = cfg, session
        self.url = f"https://api.telegram.org/bot{cfg.telegram_token}/sendMessage"

    async def send(self, token: ValidatedToken) -> None:
        c = token.candidate
        text = (
            "🚀 *NOUVEAU TOKEN VALIDE \\(Pump\\.fun\\)*\n\n"
            f"Nom: `${escape_md(c.symbol)}`\n"
            f"Liquide: `{token.liquidity_sol:.3f} SOL` | Dev: `{token.dev_percent:.2f}%` | Top10: `{token.top10_percent:.2f}%`\n\n"
            "CA:\n"
            f"`{c.mint}`\n\n"
            f"📈 [Axiom](https://axiom.trade/t/{c.mint})\n"
            f"📊 [GMGN](https://gmgn.ai/sol/token/{c.mint})"
        )
        payload = {"chat_id": self.cfg.telegram_chat_id, "text": text, "parse_mode": "MarkdownV2", "disable_web_page_preview": True}
        timeout = aiohttp.ClientTimeout(total=10)
        for attempt in range(3):
            async with self.session.post(self.url, json=payload, timeout=timeout) as response:
                body = await response.text()
                if response.status == 429 and attempt < 2:
                    await asyncio.sleep(_retry_after(response, 1.5 * (attempt + 1)))
                    continue
                if response.status >= 300:
                    raise RuntimeError(f"Telegram {response.status}: {body[:500]}")
                return


def escape_md(value: str) -> str:
    return "".join("\\" + ch if ch in r"_[]()~`>#+-=|{}.!" else ch for ch in value)


def is_create_logs(logs: list[str]) -> bool:
    return any(_CREATE_LOG_RE.search(line.strip()) for line in logs)


def candidate_from_transaction(tx: dict[str, Any], signature: str, program_id: str, logs: list[str]) -> Candidate | None:
    try:
        message = tx["transaction"]["message"]
        account_keys = [x["pubkey"] if isinstance(x, dict) else x for x in message["accountKeys"]]
        if not is_create_logs(logs):
            return None
        for ix in message.get("instructions", []):
            if ix.get("programId") != program_id:
                continue
            parsed = ix.get("parsed")
            accounts = ix.get("accounts", [])
            if accounts and (not parsed or parsed.get("type", "").lower() == "create"):
                mint = accounts[0]
                curve = accounts[2] if len(accounts) > 2 else ""
                creator = accounts[7] if len(accounts) > 7 else account_keys[0]
                if isinstance(mint, dict) or isinstance(curve, dict):
                    return None
                return Candidate(mint=mint, creator=creator, bonding_curve=curve, signature=signature)
        return None
    except (KeyError, IndexError, TypeError):
        return None


class Monitor:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.stop_event = asyncio.Event()
        self.queue: asyncio.Queue[tuple[str, list[str]]] = asyncio.Queue(maxsize=cfg.queue_size)
        self.recent: OrderedDict[str, None] = OrderedDict()
        self.dropped = 0

    def _already_queued(self, signature: str) -> bool:
        if signature in self.recent:
            return True
        self.recent[signature] = None
        if len(self.recent) > 5000:
            self.recent.popitem(last=False)
        return False

    async def run(self) -> None:
        async with aiohttp.ClientSession() as session:
            limiter = RateLimiter(self.cfg.rpc_max_rps)
            rpc = SolanaRPC(self.cfg, session, limiter)
            db = SeenDB(self.cfg.db_path)
            validator = Validator(self.cfg, rpc, LPVerifier(self.cfg, session))
            telegram = Telegram(self.cfg, session)
            LOG.info(
                "Démarrage: %.1f req/s max, %d workers, file %d, retries %d",
                self.cfg.rpc_max_rps, self.cfg.max_concurrency, self.cfg.queue_size, self.cfg.rpc_max_retries,
            )
            workers = [
                asyncio.create_task(self.worker(rpc, db, validator, telegram))
                for _ in range(max(1, self.cfg.max_concurrency))
            ]
            try:
                delay = self.cfg.reconnect_min
                while not self.stop_event.is_set():
                    try:
                        await self.consume_or_stop()
                        delay = self.cfg.reconnect_min
                    except asyncio.CancelledError:
                        raise
                    except Exception as exc:
                        LOG.error("WebSocket interrompu: %s; reconnexion dans %.2fs", redact(str(exc)), delay)
                        if delay:
                            try:
                                await asyncio.wait_for(self.stop_event.wait(), timeout=delay)
                            except asyncio.TimeoutError:
                                pass
                        delay = min(self.cfg.reconnect_max, max(self.cfg.reconnect_min, delay * 2))
            finally:
                for w in workers:
                    w.cancel()
                await asyncio.gather(*workers, return_exceptions=True)
                db.close()

    async def consume_or_stop(self) -> None:
        consume_task = asyncio.create_task(self.consume())
        stop_task = asyncio.create_task(self.stop_event.wait())
        try:
            done, _ = await asyncio.wait({consume_task, stop_task}, return_when=asyncio.FIRST_COMPLETED)
            if consume_task in done:
                consume_task.result()
        finally:
            for t in (consume_task, stop_task):
                if not t.done():
                    t.cancel()
            await asyncio.gather(consume_task, stop_task, return_exceptions=True)

    async def consume(self) -> None:
        async with websockets.connect(self.cfg.ws_url, ping_interval=10, ping_timeout=5, close_timeout=1, max_queue=10000) as ws:
            subscription_payload = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "logsSubscribe",
                "params": [
                    {"mentions": [str(self.cfg.pump_program_id)]},
                    {"commitment": DEFAULT_COMMITMENT},
                ],
            }
            await ws.send(json.dumps(subscription_payload))
            ack = json.loads(await ws.recv())

            if "error" in ack:
                LOG.warning("Abonnement ciblé rejeté, tentative sur 'all'...")
                fallback_payload = {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "logsSubscribe",
                    "params": ["all", {"commitment": DEFAULT_COMMITMENT}],
                }
                await ws.send(json.dumps(fallback_payload))
                ack = json.loads(await ws.recv())
                if "error" in ack:
                    raise RuntimeError(f"Erreur fatale WebSocket: {ack}")

            LOG.info("Connecté et abonné aux flux WebSocket Solana avec succès !")
            async for raw in ws:
              
