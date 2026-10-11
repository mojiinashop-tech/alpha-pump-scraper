from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
import sqlite3
import time
from dataclasses import dataclass
from typing import Any

import aiohttp
import websockets
from dotenv import load_dotenv

load_dotenv()

LOG = logging.getLogger("pumpfun-alert")
LAMPORTS_PER_SOL = 1_000_000_000
DEFAULT_COMMITMENT = "confirmed"


def env(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


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
            pump_program_id=env("PUMP_PROGRAM_ID"),
            min_liquidity_sol=float(env("MIN_LIQUIDITY_SOL", "3")),
            max_top10_percent=float(env("MAX_TOP10_PERCENT", "20")),
            max_dev_percent=float(env("MAX_DEV_PERCENT", "5")),
            lp_mode=env("LP_VERIFICATION_MODE", "strict").lower(),
            lp_status_url_template=env("LP_STATUS_URL_TEMPLATE"),
            timeout=float(env("RPC_TIMEOUT_SECONDS", "8")),
            max_concurrency=int(env("MAX_CONCURRENT_VALIDATIONS", "100")),
            reconnect_min=float(env("RECONNECT_MIN_SECONDS", "0.25")),
            reconnect_max=float(env("RECONNECT_MAX_SECONDS", "5")),
            db_path=env("DATABASE_PATH", "./data/alerts.sqlite3"),
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


class SolanaRPC:
    def __init__(self, cfg: Config, session: aiohttp.ClientSession):
        self.cfg = cfg
        self.session = session
        self.rpc_id = 0

    async def call(self, method: str, params: list[Any]) -> Any:
        self.rpc_id += 1
        body = {"jsonrpc": "2.0", "id": self.rpc_id, "method": method, "params": params}
        timeout = aiohttp.ClientTimeout(total=self.cfg.timeout)
        async with self.session.post(self.cfg.http_url, json=body, timeout=timeout) as response:
            response.raise_for_status()
            payload = await response.json()
        if payload.get("error"):
            raise RuntimeError(f"RPC {method}: {payload['error']}")
        return payload.get("result")

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
    """Vérifie exclusivement une preuve explicite. Inconnu = rejet, jamais acceptation implicite."""

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
            LOG.warning("Statut LP indisponible pour %s: %s", mint, exc)
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
            mint_account, total_supply, largest, curve_balance, creator_accounts, lp_ok = await asyncio.gather(
                self.rpc.account(c.mint), self.rpc.supply(c.mint), self.rpc.largest_accounts(c.mint),
                self.rpc.balance(c.bonding_curve), self.rpc.token_accounts_by_owner(c.creator, c.mint), self.lp.burned_100(c.mint)
            )
            mint_info = self._parsed_account(mint_account)
            if not mint_info or mint_info.get("mintAuthority") is not None:
                return None
            if not lp_ok:
                return None
            if total_supply <= 0:
                return None
            top10_amount = sum(int(x["amount"]) for x in largest[:10])
            top10_percent = 100 * top10_amount / total_supply
            if top10_percent >= self.cfg.max_top10_percent:
                return None
            dev_amount = sum(int(x["account"]["data"]["parsed"]["info"]["tokenAmount"]["amount"]) for x in creator_accounts)
            dev_percent = 100 * dev_amount / total_supply
            if dev_percent >= self.cfg.max_dev_percent:
                return None
            liquidity_sol = curve_balance / LAMPORTS_PER_SOL
            if liquidity_sol <= self.cfg.min_liquidity_sol:
                return None
            return ValidatedToken(c, liquidity_sol, dev_percent, top10_percent)
        except Exception as exc:
            LOG.debug("Validation rejetée pour %s: %s", c.mint, exc)
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
        async with self.session.post(self.url, json=payload, timeout=timeout) as response:
            body = await response.text()
            if response.status >= 300:
                raise RuntimeError(f"Telegram {response.status}: {body[:500]}")


def escape_md(value: str) -> str:
    return "".join("\\" + ch if ch in r"_[]()~`>#+-=|{}.!" else ch for ch in value)


def candidate_from_transaction(tx: dict[str, Any], signature: str, program_id: str, logs: list[str]) -> Candidate | None:
    try:
        message = tx["transaction"]["message"]
        account_keys = [x["pubkey"] if isinstance(x, dict) else x for x in message["accountKeys"]]
        if not any("instruction: create" in line.lower() for line in logs):
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
        self.semaphore = asyncio.Semaphore(cfg.max_concurrency)

    async def run(self) -> None:
        logging.basicConfig(level=getattr(logging, env("LOG_LEVEL", "INFO").upper(), logging.INFO), format="%(asctime)s %(levelname)s %(message)s")
        async with aiohttp.ClientSession() as session:
            rpc, db = SolanaRPC(self.cfg, session), SeenDB(self.cfg.db_path)
            validator, telegram = Validator(self.cfg, rpc, LPVerifier(self.cfg, session)), Telegram(self.cfg, session)
            try:
                delay = self.cfg.reconnect_min
                while not self.stop_event.is_set():
                    try:
                        await self.consume(rpc, db, validator, telegram)
                        delay = self.cfg.reconnect_min
                    except asyncio.CancelledError:
                        raise
                    except Exception as exc:
                        LOG.error("WebSocket interrompu: %s; reconnexion dans %.2fs", exc, delay)
                        if delay:
                            try:
                                await asyncio.wait_for(self.stop_event.wait(), timeout=delay)
                            except asyncio.TimeoutError:
                                pass
                        delay = min(self.cfg.reconnect_max, max(self.cfg.reconnect_min, delay * 2))
            finally:
                db.close()

    async def consume(self, rpc: SolanaRPC, db: SeenDB, validator: Validator, telegram: Telegram) -> None:
        async with websockets.connect(self.cfg.ws_url, ping_interval=10, ping_timeout=5, close_timeout=1, max_queue=10000) as ws:
            await ws.send(json.dumps({"jsonrpc": "2.0", "id": 1, "method": "logsSubscribe", "params": [{"mentions": [self.cfg.pump_program_id]}, {"commitment": DEFAULT_COMMITMENT}]}))
            ack = json.loads(await ws.recv())
            if "error" in ack:
                raise RuntimeError(ack)
            LOG.info("Abonné au programme %s", self.cfg.pump_program_id)
            async for raw in ws:
                msg = json.loads(raw)
                value = msg.get("params", {}).get("result", {}).get("value", {})
                signature = value.get("signature")
                if not signature or value.get("err"):
                    continue
                asyncio.create_task(self.process(signature, value.get("logs", []), rpc, db, validator, telegram))

    async def process(self, signature: str, logs: list[str], rpc: SolanaRPC, db: SeenDB, validator: Validator, telegram: Telegram) -> None:
        async with self.semaphore:
            try:
                tx = await rpc.transaction(signature)
                if not tx:
                    return
                candidate = candidate_from_transaction(tx, signature, self.cfg.pump_program_id, logs)
                if not candidate or not await db.claim(candidate.mint, signature):
                    return
                validated = await validator.validate(candidate)
                if validated:
                    await telegram.send(validated)
                    LOG.info("Alerte envoyée: %s", candidate.mint)
            except Exception:
                LOG.exception("Erreur de traitement de %s", signature)


async def main() -> None:
    cfg = Config.from_env()
    monitor = Monitor(cfg)
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, monitor.stop_event.set)
    await monitor.run()


if __name__ == "__main__":
    asyncio.run(main())