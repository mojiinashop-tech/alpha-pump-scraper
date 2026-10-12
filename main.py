from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
import sqlite3
import sys
from contextlib import suppress
from typing import Any, Dict, List, Optional

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
logger = logging.getLogger("QuantumAlphaSniper")

PUMP_PROGRAM_ID = os.getenv("PUMP_PROGRAM_ID", "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P")
DEFAULT_COMMITMENT = os.getenv("SOLANA_COMMITMENT", "confirmed")


class QuantumAlphaEngine:
    """Moteur de scoring conservé, avec filtres durs et rejet par défaut."""

    def __init__(self, min_liq: float = 3.0, max_liq: float = 30.0, max_top10: float = 20.0,
                 max_dev: float = 5.0, max_bundle: float = 5.0, min_score: float = 80.0):
        self.min_liq = min_liq
        self.max_liq = max_liq
        self.max_top10 = max_top10
        self.max_dev = max_dev
        self.max_bundle = max_bundle
        self.min_score = min_score
        self.elite_wallets = {w.strip() for w in os.getenv("ELITE_WALLETS", "").split(",") if w.strip()}

    def evaluate_token(self, data: Dict[str, Any]) -> tuple[float, bool, str]:
        mint = data.get("mint", "Inconnu")
        liq = float(data.get("liquidity_sol", 0.0) or 0.0)
        mc = float(data.get("market_cap_usd", 0.0) or 0.0)
        top10 = float(data.get("top10_pct", 0.0) or 0.0)
        dev = float(data.get("dev_pct", 0.0) or 0.0)
        bundle = float(data.get("bundle_pct", 0.0) or 0.0)
        age_minutes = float(data.get("age_minutes", 1.0) or 1.0)
        dex_paid = bool(data.get("dex_paid", False))
        tax_pct = float(data.get("tax_pct", 0.0) or 0.0)
        early_buyers: List[str] = data.get("early_buyers", []) or []

        if not mint or mint == "Inconnu":
            return 0.0, False, "Mint absent"
        if not dex_paid:
            return 0.0, False, "DEX non payé (donnée absente ou non vérifiée)"
        if not (self.min_liq < liq <= self.max_liq):
            return 0.0, False, f"Liquidité hors zone ({liq:.2f} SOL)"
        if top10 >= self.max_top10:
            return 0.0, False, f"Top 10 trop élevé ({top10}%)"
        if dev >= self.max_dev:
            return 0.0, False, f"Wallet Dev trop lourd ({dev}%)"
        if bundle > self.max_bundle:
            return 0.0, False, f"Bundlers trop élevés ({bundle}%)"
        if tax_pct > 2.0:
            return 0.0, False, f"Taxe excessive ({tax_pct}%)"
        if mc <= 0:
            return 0.0, False, "Market Cap invalide ou non vérifié"

        lm_ratio = liq / mc
        ratio_score = max(0.0, 1.0 - abs(lm_ratio - 0.25) / 0.25)
        concentration_penalty = (top10 / 100.0) * 1.5 + (dev / 100.0) * 2.0 + (bundle / 100.0) * 3.0
        safety_score = max(0.0, 1.0 - concentration_penalty)
        if age_minutes <= 0.5:
            time_score = 0.6
        elif age_minutes <= 5.0:
            time_score = 1.0
        else:
            time_score = max(0.1, 1.0 - (age_minutes - 5.0) / 25.0)
        elite_count = sum(1 for wallet in early_buyers if wallet in self.elite_wallets)
        elite_bonus = 10.0 if elite_count >= 2 else (5.0 if elite_count == 1 else 0.0)
        final_score = min(100.0, (0.35 * ratio_score + 0.45 * safety_score + 0.20 * time_score) * 100.0 + elite_bonus)
        return round(final_score, 2), final_score >= self.min_score, "Validation réussie"


class TelegramNotifier:
    def __init__(self, token: str, channel_id: str):
        self.bot = Bot(token=token)
        self.channel_id = channel_id

    async def send_alpha_alert(self, data: Dict[str, Any], score: float):
        mint = data.get("mint")
        name = data.get("name", "Inconnu")
        symbol = data.get("symbol", "N/A")
        liq = float(data.get("liquidity_sol", 0.0) or 0.0)
        mc = float(data.get("market_cap_usd", 0.0) or 0.0)
        top10 = float(data.get("top10_pct", 0.0) or 0.0)
        dev = float(data.get("dev_pct", 0.0) or 0.0)
        bundle = float(data.get("bundle_pct", 0.0) or 0.0)
        age_min = float(data.get("age_minutes", 1.0) or 1.0)
        tax = float(data.get("tax_pct", 0.0) or 0.0)
        age_str = f"{int(age_min)} minute(s)" if age_min >= 1 else f"{int(age_min * 60)} secondes"
        message = (
            f"🟢 <b>QUANTUM ALPHA DÉTECTÉ [Score: {score}/100]</b>\n\n"
            f"⏱️ <b>Âge du jeton :</b> {age_str}\n"
            f"🪙 <b>Nom :</b> {name} (<b>${symbol}</b>)\n"
            f"💧 <b>Liquidité :</b> {liq:.2f} SOL\n"
            f"📊 <b>Market Cap :</b> ${mc:,.2f}\n"
            f"⚙️ <b>Statut DEX :</b> Payé ✅ | Taxe : {tax}%\n"
            f"👥 <b>Top 10 Holders :</b> {top10}%\n"
            f"💻 <b>Dev Wallet :</b> {dev}%\n"
            f"📦 <b>Bundlers :</b> {bundle}%\n\n"
            f"<code>{mint}</code>\n\n"
            f"🔗 <a href='https://dexscreener.com/solana/{mint}'>Dexscreener</a> • "
            f"<a href='https://gmgn.ai/solana/token/{mint}'>GMGN</a> • "
            f"<a href='https://axiom.trade/t/{mint}'>Axiom</a> • "
            f"<a href='https://pump.fun/coin/{mint}'>Pump.fun</a>"
        )
        try:
            await self.bot.send_message(chat_id=self.channel_id, text=message, parse_mode="HTML", disable_web_page_preview=True)
            logger.info("Alerte envoyée pour %s", symbol)
        except TelegramError as exc:
            logger.error("Erreur Telegram pour %s: %s", mint, type(exc).__name__)


class SeenStore:
    def __init__(self, path: str = "data/seen.sqlite3"):
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

    def close(self):
        self.db.close()


class HeliusMonitor:
    def __init__(self, session: aiohttp.ClientSession, engine: QuantumAlphaEngine,
                 notifier: TelegramNotifier, store: SeenStore):
        self.session = session
        self.engine = engine
        self.notifier = notifier
        self.store = store
        self.api_key = os.getenv("HELIUS_API_KEY", "").strip()
        self.ws_url = os.getenv("HELIUS_WS_URL", "").strip() or (
            f"wss://mainnet.helius-rpc.com/?api-key={self.api_key}" if self.api_key else ""
        )
        self.rpc_url = os.getenv("HELIUS_RPC_URL", "").strip() or (
            f"https://mainnet.helius-rpc.com/?api-key={self.api_key}" if self.api_key else ""
        )
        self.stop_event = asyncio.Event()
        self.rpc_workers = max(1, int(os.getenv("RPC_WORKERS", "4")))
        self.event_queue: asyncio.Queue[tuple[str, list[str]]] = asyncio.Queue(
            maxsize=max(10, int(os.getenv("EVENT_QUEUE_SIZE", "500")))
        )
        self.enqueued_signatures: set[str] = set()
        self.enqueue_lock = asyncio.Lock()

    async def rpc(self, method: str, params: list[Any]) -> Any:
        payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
        for attempt in range(4):
            try:
                async with self.session.post(self.rpc_url, json=payload, timeout=10) as response:
                    if response.status == 429:
                        retry_after = response.headers.get("Retry-After")
                        wait = float(retry_after) if retry_after else min(4.0, 0.5 * (2 ** attempt))
                        logger.warning("RPC rate limité pour %s; nouvelle tentative dans %.1fs", method, wait)
                        await asyncio.sleep(wait)
                        continue
                    if response.status >= 400:
                        logger.warning("RPC HTTP %s pour %s", response.status, method)
                        raise RuntimeError(f"RPC HTTP {response.status} for {method}") from None
                    body = await response.json()
                    break
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                if attempt == 3:
                    logger.warning("RPC indisponible pour %s après retries: %s", method, type(exc).__name__)
                    raise RuntimeError(f"RPC unavailable for {method}") from None
                await asyncio.sleep(min(4.0, 0.5 * (2 ** attempt)))
        else:
            raise RuntimeError(f"RPC rate limité pour {method}") from None
        if body.get("error"):
            raise RuntimeError(body["error"])
        return body.get("result")

    async def transaction(self, signature: str) -> Optional[dict[str, Any]]:
        params = [signature, {
            "encoding": "jsonParsed", "commitment": DEFAULT_COMMITMENT,
            "maxSupportedTransactionVersion": 0,
        }]
        for attempt in range(3):
            tx = await self.rpc("getTransaction", params)
            if tx:
                return tx
            await asyncio.sleep((0.25, 0.5, 1.0)[attempt])
        return None

    @staticmethod
    def candidate_from_transaction(tx: dict[str, Any], signature: str) -> Optional[Dict[str, Any]]:
        try:
            message = tx["transaction"]["message"]
            keys = [item.get("pubkey") if isinstance(item, dict) else item for item in message["accountKeys"]]
            for instruction in message.get("instructions", []):
                if instruction.get("programId") != PUMP_PROGRAM_ID:
                    continue
                accounts = instruction.get("accounts", [])
                if accounts and all(isinstance(item, str) for item in accounts):
                    mint = accounts[0]
                    creator = accounts[-1]
                elif accounts and all(isinstance(item, int) for item in accounts):
                    mint = keys[accounts[0]]
                    creator = keys[accounts[-1]]
                else:
                    continue
                return {"mint": mint, "creator": creator, "signature": signature}
        except (KeyError, IndexError, TypeError):
            logger.debug("Transaction non décodable: %s", signature, exc_info=True)
        return None

    async def enrich(self, candidate: Dict[str, Any]) -> Dict[str, Any]:
        """Charge les métriques d’un fournisseur explicite; absent = rejet fail-closed."""
        template = os.getenv("ALPHA_DATA_URL_TEMPLATE", "").strip()
        if not template:
            return {**candidate, "dex_paid": False, "market_cap_usd": 0}
        url = template.format(mint=candidate["mint"])
        try:
            async with self.session.get(url, timeout=10) as response:
                if response.status >= 400:
                    logger.warning("Source métriques HTTP %s", response.status)
                    return {**candidate, "dex_paid": False, "market_cap_usd": 0}
                data = await response.json()
        except (aiohttp.ClientError, asyncio.TimeoutError):
            logger.warning("Source métriques indisponible pour %s", candidate["mint"])
            return {**candidate, "dex_paid": False, "market_cap_usd": 0}
        return {**candidate, **data}

    async def process(self, signature: str, logs: list[str]) -> None:
        try:
            tx = await self.transaction(signature)
            candidate = self.candidate_from_transaction(tx, signature) if tx else None
            if not candidate or not await self.store.claim(candidate["mint"], signature):
                return
            data = await self.enrich(candidate)
            score, valid, reason = self.engine.evaluate_token(data)
            if valid:
                logger.info("[FILTER PASSED] %s score=%.2f", candidate["mint"], score)
                await self.notifier.send_alpha_alert(data, score)
            else:
                logger.info("[FILTER REJECTED: %s] %s", reason, candidate["mint"])
        except Exception as exc:
            logger.warning("Erreur de traitement de %s: %s", signature, type(exc).__name__)

    async def enqueue(self, signature: str, logs: list[str]) -> None:
        async with self.enqueue_lock:
            if signature in self.enqueued_signatures:
                return
            if self.event_queue.full():
                logger.warning("File d’événements pleine; événement ignoré pour préserver le service")
                return
            self.enqueued_signatures.add(signature)
            await self.event_queue.put((signature, logs))

    async def worker(self) -> None:
        while not self.stop_event.is_set():
            signature, logs = await self.event_queue.get()
            try:
                await self.process(signature, logs)
            finally:
                self.enqueued_signatures.discard(signature)
                self.event_queue.task_done()

    async def run(self):
        if not self.ws_url or not self.rpc_url:
            raise RuntimeError("HELIUS_API_KEY ou HELIUS_WS_URL/HELIUS_RPC_URL manquant")
        workers = [asyncio.create_task(self.worker()) for _ in range(self.rpc_workers)]
        delay = 0.25
        try:
            while not self.stop_event.is_set():
                try:
                    async with self.session.ws_connect(self.ws_url, heartbeat=10, receive_timeout=30, max_msg_size=2**20) as ws:
                        await ws.send_json({
                            "jsonrpc": "2.0", "id": 1, "method": "logsSubscribe",
                            "params": [{"mentions": [PUMP_PROGRAM_ID]}, {"commitment": DEFAULT_COMMITMENT}],
                        })
                        ack = await ws.receive_json()
                        if "error" in ack:
                            raise RuntimeError(ack["error"])
                        logger.info("Souscription Helius active sur %s; workers=%s", PUMP_PROGRAM_ID, self.rpc_workers)
                        delay = 0.25
                        async for message in ws:
                            if message.type != aiohttp.WSMsgType.TEXT:
                                continue
                            payload = json.loads(message.data)
                            value = payload.get("params", {}).get("result", {}).get("value", {})
                            if value.get("err") or not value.get("signature"):
                                continue
                            await self.enqueue(value["signature"], value.get("logs", []))
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    logger.error("WebSocket Helius coupé: %s (%s); reconnexion dans %.2fs", type(exc).__name__, str(exc)[:120], delay)
                    with suppress(asyncio.TimeoutError):
                        await asyncio.wait_for(self.stop_event.wait(), timeout=delay)
                    delay = min(5.0, delay * 2)
        finally:
            for task in workers:
                task.cancel()
            await asyncio.gather(*workers, return_exceptions=True)


async def handle_health(request: web.Request) -> web.Response:
    return web.json_response({"status": "ok", "service": "alpha-pump-scraper"})


async def start_health_server() -> web.AppRunner:
    app = web.Application()
    app.router.add_get("/", handle_health)
    app.router.add_get("/health", handle_health)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", int(os.getenv("PORT", "10000")))
    await site.start()
    logger.info("Serveur de santé actif sur le port %s", os.getenv("PORT", "10000"))
    return runner


async def main():
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    channel = os.getenv("TELEGRAM_CHANNEL_ID", "").strip() or os.getenv("TELEGRAM_CHAT_ID", "").strip()
    runner = await start_health_server()
    if not token or not channel:
        logger.error("TELEGRAM_BOT_TOKEN ou TELEGRAM_CHANNEL_ID manquant")
        await asyncio.Event().wait()
    store = SeenStore(os.getenv("SEEN_DB_PATH", "data/seen.sqlite3"))
    engine = QuantumAlphaEngine(min_liq=3.0, max_liq=30.0, min_score=80.0)
    notifier = TelegramNotifier(token, channel)
    monitor: HeliusMonitor | None = None
    try:
        await notifier.bot.send_message(
            chat_id=channel,
            text="💎 <b>QUANTUM ALPHA BOT SÉCURISÉ — PRÊT</b>\n<i>Surveillance Helius active en cours...</i>",
            parse_mode="HTML",
        )
    except TelegramError as exc:
        logger.error("Erreur de notification de démarrage: %s", type(exc).__name__)
    loop = asyncio.get_running_loop()
    stop = asyncio.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    try:
        async with aiohttp.ClientSession() as session:
            monitor = HeliusMonitor(session, engine, notifier, store)
            if not monitor.ws_url or not monitor.rpc_url:
                logger.error("[CONFIG] Helius absent: alertes désactivées, serveur de santé maintenu")
                await stop.wait()
                return
            task = asyncio.create_task(monitor.run())
            await stop.wait()
            monitor.stop_event.set()
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
        logger.info("Arrêt du script.")
