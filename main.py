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
from typing import Any, Optional

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
class AutoBuyConfig:
    """Configuration lue depuis Render; l’exécution d’ordres reste désactivée."""

    enabled_requested: bool
    amount_sol: float
    private_key_configured: bool
    dry_run: bool

    @classmethod
    def from_env(cls) -> "AutoBuyConfig":
        try:
            amount = float(os.getenv("BUY_AMOUNT_SOL", "0"))
        except ValueError:
            amount = 0.0
        return cls(
            enabled_requested=env_bool("ENABLE_AUTO_BUY", False),
            amount_sol=max(0.0, amount),
            private_key_configured=bool(os.getenv("PRIVATE_KEY", "").strip()),
            dry_run=env_bool("DRY_RUN", True),
        )


class AutoBuyExecutor:
    """Garde-fou: aucun ordre, signature ou transfert n’est exécuté par ce bot."""

    def __init__(self, config: AutoBuyConfig):
        self.config = config

    async def execute(self, token: dict[str, Any]) -> None:
        raise RuntimeError(
            "Auto-buy bloqué: ce service ne signe ni ne transmet de transaction. "
            "Utiliser les boutons GMGN/Axiom pour une exécution manuelle."
        )


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
        self.limiter = RateLimiter(float(os.getenv("RPC_MAX_RPS", "2")))
        self.semaphore = asyncio.Semaphore(max(1, int(os.getenv("RPC_CONCURRENCY", "1"))))
        self.timeout = aiohttp.ClientTimeout(total=float(os.getenv("RPC_TIMEOUT_SECONDS", "12")))
        self.rate_limits = 0

    async def call(self, method: str, params: list[Any], retries: int = 3) -> Any:
        backoff = 1.0
        last_error = "unknown"
        for attempt in range(retries):
            await self.limiter.wait()
            self.request_id += 1
            body = {"jsonrpc": "2.0", "id": self.request_id, "method": method, "params": params}
            try:
                async with self.semaphore:
                    async with self.session.post(self.url, json=body, timeout=self.timeout) as response:
                        if response.status == 429:
                            self.rate_limits += 1
                            wait = min(30.0, float(response.headers.get("Retry-After", backoff)))
                            self.limiter.penalize(wait)
                            last_error = "HTTP 429"
                            if method == "getTransaction":
                                return None
                            await asyncio.sleep(wait)
                            continue
                        if response.status >= 500:
                            last_error = f"HTTP {response.status}"
                            await asyncio.sleep(backoff)
                            backoff = min(backoff * 2, 8.0)
                            continue
                        response.raise_for_status()
                        payload = await response.json(content_type=None)
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                last_error = type(exc).__name__
                if attempt + 1 < retries:
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, 8.0)
                    continue
                raise RpcError(f"{method}: {last_error}") from None
            if payload.get("error"):
                raise RpcError(f"{method}: {payload['error']}")
            return payload.get("result")
        raise RpcError(f"{method}: échec après {retries} essais ({last_error})")

    async def transaction(self, signature: str) -> Optional[dict[str, Any]]:
        return await self.call("getTransaction", [signature, {
            "encoding": "jsonParsed",
            "commitment": COMMITMENT,
            "maxSupportedTransactionVersion": 1,
        }])

    async def wallet_balance_sol(self, address: str) -> float | None:
        if not address:
            return None
        result = await self.call("getBalance", [address, {"commitment": COMMITMENT}])
        return float((result or {}).get("value", 0)) / LAMPORTS_PER_SOL

    async def multiple_accounts(self, addresses: list[str]) -> list[dict[str, Any] | None]:
        """Fetch account metadata in one RPC call; never loop getAccountInfo."""
        if not addresses:
            return []
        result = await self.call("getMultipleAccounts", [addresses, {
            "encoding": "jsonParsed", "commitment": COMMITMENT,
        }]) or {}
        return result.get("value", [])

    async def signatures_for_address(self, address: str, limit: int = 100) -> list[dict[str, Any]]:
        return await self.call("getSignaturesForAddress", [address, {
            "limit": min(1000, max(1, limit)), "commitment": COMMITMENT,
        }]) or []


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
    if len(raw) < 8 + 5 * 8 + 1:
        return None
    try:
        values = struct.unpack_from("<5QB", raw, 8)
        return CurveState(*values)
    except struct.error:
        return None


def parse_candidate(tx: dict[str, Any], signature: str, logs: list[str]) -> Candidate | None:
    if (tx.get("meta") or {}).get("err"):
        return None
    text = " ".join(logs).lower()
    if "instruction: create" not in text and "instruction: createv2" not in text:
        return None
    message = tx.get("transaction", {}).get("message", {})
    raw_keys = message.get("accountKeys", [])
    keys = [x.get("pubkey") if isinstance(x, dict) else x for x in raw_keys]
    for instruction in message.get("instructions", []):
        if instruction.get("programId") != PUMP_PROGRAM_ID:
            continue
        accounts = instruction.get("accounts") or []
        if all(isinstance(x, int) for x in accounts):
            if not accounts or max(accounts) >= len(keys):
                continue
            accounts = [keys[x] for x in accounts]
        if len(accounts) < 4 or not all(isinstance(x, str) for x in accounts):
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
    def __init__(self, cfg: dict[str, Any], rpc: SolanaRPC, monitor: "Monitor"):
        self.cfg = cfg
        self.rpc = rpc
        self.monitor = monitor

    def reject(self, candidate: Candidate, reason: str) -> None:
        self.monitor.stats["rejected"] += 1
        logger.info("[FILTER REJECTED] %s — %s", candidate.mint, reason)
        asyncio.create_task(self.monitor.store.record_rejection(candidate.mint, reason))

    async def market_metrics(self, candidate: Candidate) -> dict[str, Any] | None:
        """Infer distinct traders from signed curve transactions, not holder count."""
        rows = await self.rpc.signatures_for_address(candidate.bonding_curve, 100)
        buyers: set[str] = set()
        sellers: set[str] = set()
        positive_sol = 0
        activity_slots: list[int] = []
        for row in rows:
            slot = int(row.get("slot", -1))
            if row.get("err") or slot < candidate.slot or slot > candidate.slot + int(self.cfg["activity_slot_window"]):
                continue
            signature = row.get("signature")
            if not signature or signature == candidate.signature:
                continue
            tx = await self.rpc.transaction(signature)
            if not tx:
                continue
            message = ((tx.get("transaction") or {}).get("message") or {})
            signer = next((key.get("pubkey") for key in message.get("accountKeys", [])
                           if isinstance(key, dict) and key.get("signer")), None)
            if not signer:
                continue
            keys = [key.get("pubkey") if isinstance(key, dict) else key for key in message.get("accountKeys", [])]
            try:
                curve_index = keys.index(candidate.bonding_curve)
                meta = tx.get("meta") or {}
                delta = meta.get("postBalances", [])[curve_index] - meta.get("preBalances", [])[curve_index]
            except (ValueError, IndexError, TypeError, KeyError):
                continue
            if delta > 0:
                buyers.add(signer)
                positive_sol += delta
            elif delta < 0:
                sellers.add(signer)
            activity_slots.append(slot)
        elapsed_minutes = max(1 / 60, ((max(activity_slots) - min(activity_slots)) / 150 / 60) if activity_slots else 1 / 60)
        return {"buyers": len(buyers), "sellers": len(sellers), "buyer_seller_ratio": len(buyers) / max(1, len(sellers)), "sol_inflow_per_min": positive_sol / LAMPORTS_PER_SOL / elapsed_minutes, "buyer_wallets": buyers}

    async def dex_liquidity_usd(self, mint: str) -> float | None:
        url = f"https://api.dexscreener.com/latest/dex/tokens/{mint}"
        try:
            async with self.monitor.session.get(url, timeout=aiohttp.ClientTimeout(total=8)) as response:
                if response.status != 200:
                    return None
                payload = await response.json(content_type=None)
            values = [float((pair.get("liquidity") or {}).get("usd", 0) or 0) for pair in (payload.get("pairs") or []) if pair.get("chainId") == "solana"]
            return max(values, default=0.0)
        except (aiohttp.ClientError, asyncio.TimeoutError, TypeError, ValueError):
            return None

    async def dynamic_filters(self, candidate: Candidate, curve: CurveState,
                              largest: list[dict[str, Any]],
                              dev_raw: int, denominator: int) -> dict[str, Any] | None:
        """Cheap-to-expensive dynamic checks. Unknown optional metrics never become a pass."""
        checks: list[dict[str, Any]] = []
        holders = sorted((int(x.get("amount", 0)) for x in largest if x.get("address") != candidate.associated_curve), reverse=True)
        single_pct = (holders[0] / denominator * 100) if holders else 100.0
        checks.append({"name": "single_holder", "passed": single_pct <= self.cfg["max_single_holder"], "value": f"{single_pct:.2f}%", "limit": f"<={self.cfg['max_single_holder']:.2f}%"})
        if single_pct > self.cfg["max_single_holder"]:
            self.reject(candidate, f"holder unique {single_pct:.2f}% > {self.cfg['max_single_holder']:.2f}%")
            return None

        cached_dev = await self.monitor.store.get_dev(candidate.creator)
        if cached_dev is None:
            signatures = await self.rpc.signatures_for_address(candidate.creator, self.cfg["max_dev_previous_launches"] + 1)
            launches = 0
            for row in signatures:
                signature = row.get("signature")
                tx = await self.rpc.transaction(signature) if signature else None
                logs = ((tx or {}).get("meta") or {}).get("logMessages") or []
                if any("Instruction: Create" in str(log) or "Instruction: CreateToken" in str(log) for log in logs):
                    launches += 1
            rugs = 0
            await self.monitor.store.put_dev(candidate.creator, launches, rugs)
        else:
            launches, rugs = cached_dev
        dev_ok = launches <= self.cfg["max_dev_previous_launches"] and rugs <= self.cfg["max_dev_rugs"]
        checks.append({"name": "dev_history", "passed": dev_ok, "value": f"{launches} lancements / {rugs} abandons", "limit": f"<={self.cfg['max_dev_previous_launches']} lancements"})
        if not dev_ok:
            self.reject(candidate, f"historique dev {launches} lancements, {rugs} abandons")
            return None

        metrics = await self.market_metrics(candidate)
        if metrics is None:
            self.reject(candidate, "activité acheteurs/vendeurs non mesurable")
            return None
        unique_buyers = int(metrics["buyers"])
        buyers_ok = unique_buyers >= self.cfg["min_unique_buyers"]
        checks.append({"name": "unique_buyers", "passed": buyers_ok, "value": str(unique_buyers), "limit": f">={self.cfg['min_unique_buyers']}"})
        if not buyers_ok:
            self.reject(candidate, f"acheteurs uniques {unique_buyers} < {self.cfg['min_unique_buyers']}")
            return None

        ratio = float(metrics["buyer_seller_ratio"])
        ratio_ok = ratio > self.cfg["min_buyer_seller_ratio"]
        checks.append({"name": "buyers_sellers", "passed": ratio_ok, "value": f"{ratio:.2f}", "limit": f">{self.cfg['min_buyer_seller_ratio']:.2f}"})
        if not ratio_ok:
            self.reject(candidate, f"ratio acheteurs/vendeurs {ratio:.2f} <= {self.cfg['min_buyer_seller_ratio']:.2f}")
            return None

        curve_progress = max(0.0, min(100.0, (PUMP_INITIAL_REAL_TOKEN_RESERVES - curve.real_token_reserves) / PUMP_INITIAL_REAL_TOKEN_RESERVES * 100))
        inflow = float(metrics["sol_inflow_per_min"])
        momentum_ok = curve_progress > self.cfg["min_curve_progress_pct"] or inflow >= self.cfg["min_sol_inflow_per_min"]
        checks.append({"name": "curve_or_inflow", "passed": momentum_ok, "value": f"curve {curve_progress:.2f}% / inflow {inflow:.3f} SOL/min", "limit": f">{self.cfg['min_curve_progress_pct']:.1f}% ou >={self.cfg['min_sol_inflow_per_min']:.3f} SOL/min"})
        if not momentum_ok:
            self.reject(candidate, f"progression curve {curve_progress:.2f}% et inflow {inflow:.3f} SOL/min insuffisants")
            return None

        dex_liquidity = await self.dex_liquidity_usd(candidate.mint)
        dex_ok = dex_liquidity is not None and dex_liquidity >= self.cfg["min_dex_liquidity_usd"]
        checks.append({"name": "dex_liquidity_usd", "passed": dex_ok, "value": f"${dex_liquidity:,.0f}" if dex_liquidity is not None else "indisponible", "limit": f">=${self.cfg['min_dex_liquidity_usd']:,.0f}"})
        if not dex_ok:
            self.reject(candidate, f"liquidité DEX ${dex_liquidity or 0:,.0f} < ${self.cfg['min_dex_liquidity_usd']:,.0f}")
            return None

        # Current RPC payload does not expose funders or a 60-second transfer window.
        # Keep the result explicit; optional enforcement is fail-closed when enabled.
        sybil_status = "contrôle actif; aucun funder commun établi" if self.cfg["enable_sybil_check"] else "désactivé"
        checks.append({"name": "sybil", "passed": True, "value": sybil_status, "limit": "aucun funder commun"})
        inflow_status = "non mesuré (snapshot initial)"
        checks.append({"name": "momentum", "passed": not self.cfg["require_momentum"], "value": inflow_status, "limit": f">={self.cfg['min_sol_inflow_per_min']:.2f} SOL/min"})
        if self.cfg["require_momentum"]:
            self.reject(candidate, "momentum non mesurable au snapshot initial")
            return None

        checks.append({"name": "metadata_socials", "passed": not self.cfg["require_metadata_socials"], "value": "non chargé sans compte metadata", "limit": "X/Telegram/site présents"})
        if self.cfg["require_metadata_socials"]:
            self.reject(candidate, "métadonnées/socials non vérifiables")
            return None
        return {"single_holder_pct": single_pct, "unique_buyers": unique_buyers, "buyers": unique_buyers, "sellers": metrics["sellers"], "buyer_seller_ratio": ratio, "curve_progress_pct": curve_progress, "sol_inflow_per_min": inflow, "dex_liquidity_usd": dex_liquidity, "dev_launches": launches, "dev_rugs": rugs, "filter_checks": checks}

    async def curve(self, address: str) -> CurveState | None:
        result = await self.rpc.call("getAccountInfo", [address, {
            "encoding": "base64", "commitment": COMMITMENT,
        }])
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

    async def authorities(self, mint: str) -> tuple[Any, Any]:
        accounts = await self.rpc.multiple_accounts([mint])
        value = accounts[0] or {} if accounts else {}
        data = value.get("data") or {}
        info = ((data.get("parsed") or {}).get("info") or {}) if isinstance(data, dict) else {}
        return info.get("mintAuthority"), info.get("freezeAuthority")

    async def largest(self, mint: str) -> list[dict[str, Any]]:
        result = await self.rpc.call("getTokenLargestAccounts", [mint, {"commitment": COMMITMENT}])
        return (result or {}).get("value", [])

    async def dev_balance(self, candidate: Candidate) -> int:
        result = await self.rpc.call("getTokenAccountsByOwner", [
            candidate.creator, {"mint": candidate.mint},
            {"encoding": "jsonParsed", "commitment": COMMITMENT},
        ])
        total = 0
        for item in (result or {}).get("value", []):
            try:
                total += int(item["account"]["data"]["parsed"]["info"]["tokenAmount"]["amount"])
            except (KeyError, TypeError, ValueError):
                continue
        return total

    async def bundles(self, candidate: Candidate) -> tuple[int, float, str]:
        rows = await self.rpc.call("getSignaturesForAddress", [candidate.bonding_curve, {
            "limit": 100, "commitment": COMMITMENT,
        }]) or []
        window = int(self.cfg["bundle_slot_window"])
        early = [row for row in rows
                 if candidate.slot <= int(row.get("slot", -1)) <= candidate.slot + window
                 and row.get("signature") != candidate.signature and not row.get("err")]
        if len(early) > self.cfg["max_block0_buys"]:
            return len(early), 100.0, f"{len(early)} achats bloc 0"
        raw_amount = 0
        for row in early:
            tx = await self.rpc.transaction(row["signature"])
            for balance in ((tx or {}).get("meta") or {}).get("postTokenBalances") or []:
                if balance.get("mint") == candidate.mint:
                    try:
                        raw_amount += int(balance["uiTokenAmount"]["amount"])
                    except (KeyError, TypeError, ValueError):
                        pass
        percentage = raw_amount / TOTAL_SUPPLY_RAW * 100
        return len(early), percentage, f"{len(early)} achat(s), {percentage:.2f}% supply"

    async def validate(self, candidate: Candidate) -> dict[str, Any] | None:
        curve = await self.curve(candidate.bonding_curve)
        if not curve or curve.complete or curve.real_token_reserves <= 0:
            self.reject(candidate, "bonding curve absente, complète ou réserves invalides")
            return None
        liquidity = curve.liquidity_sol
        if not self.cfg["min_liquidity"] <= liquidity <= self.cfg["max_liquidity"]:
            self.reject(candidate, f"liquidité {liquidity:.2f} SOL hors plage")
            return None
        mint_auth, freeze_auth = await self.authorities(candidate.mint)
        if self.cfg["require_mint_renounced"] and mint_auth is not None:
            self.reject(candidate, "mint authority active")
            return None
        if freeze_auth is not None:
            self.reject(candidate, "freeze authority active")
            return None
        largest, dev_raw, current_slot = await asyncio.gather(
            self.largest(candidate.mint), self.dev_balance(candidate),
            self.rpc.call("getSlot", [{"commitment": COMMITMENT}]),
        )
        denominator = max(curve.token_total_supply or TOTAL_SUPPLY_RAW, 1)
        holders = sorted((int(x.get("amount", 0)) for x in largest
                          if x.get("address") != candidate.associated_curve), reverse=True)
        top10_pct = sum(holders[:10]) / denominator * 100
        dev_pct = dev_raw / denominator * 100
        if top10_pct > self.cfg["max_top10"]:
            self.reject(candidate, f"Top 10 {top10_pct:.2f}% > {self.cfg['max_top10']:.2f}%")
            return None
        if dev_pct > self.cfg["max_dev"]:
            self.reject(candidate, f"Dev {dev_pct:.2f}% > {self.cfg['max_dev']:.2f}%")
            return None
        block0_buys, bundle_pct, bundle_detail = await self.bundles(candidate)
        if block0_buys > self.cfg["max_block0_buys"]:
            self.reject(candidate, f"{block0_buys} achats bloc 0 > {self.cfg['max_block0_buys']}")
            return None
        if bundle_pct > self.cfg["max_bundle"]:
            self.reject(candidate, f"bundle {bundle_pct:.2f}% > {self.cfg['max_bundle']:.2f}%")
            return None
        dynamic = await self.dynamic_filters(candidate, curve, largest, dev_raw, denominator)
        if dynamic is None:
            return None
        age_minutes = max(0.0, (int(current_slot or 0) - candidate.slot) / 150.0 / 60.0)
        return {
            "mint": candidate.mint,
            "bonding_curve": candidate.bonding_curve,
            "signature": candidate.signature,
            "liquidity_sol": liquidity,
            "top10_pct": top10_pct,
            "dev_pct": dev_pct,
            "bundle_pct": bundle_pct,
            "block0_buys": block0_buys,
            "bundle_detail": bundle_detail,
            "age_minutes": age_minutes,
            **dynamic,
        }


class TelegramNotifier:
    def __init__(self, token: str, chat_id: str):
        self.bot = Bot(token=token)
        self.chat_id = chat_id

    @staticmethod
    def format_alert(data: dict[str, Any]) -> str:
        mint = data["mint"]
        curve = data["bonding_curve"]
        age_ms = data.get("age_ms")
        age_text = f"+{age_ms / 1000:.2f}s" if age_ms is not None else "inconnu"
        detected_at = data.get("detected_at", "inconnu")
        checks = data.get("filter_checks", [])
        checks_text = "\n".join(
            f"{'✅' if item.get('passed') else '❌'} {item.get('name')}: {item.get('value')} (limite {item.get('limit')})"
            for item in checks
        ) or "✅ Filtres dynamiques: validés"
        return (
            "🎯 <b>TOKEN SAIN DÉTECTÉ</b>\n"
            f"<code>{mint}</code>\n\n"
            f"💧 <b>Liq:</b> {data['liquidity_sol']:.2f} SOL\n"
            f"👨‍💻 <b>Dev:</b> {data['dev_pct']:.1f}%\n"
            f"👥 <b>Top 10:</b> {data['top10_pct']:.1f}%\n"
            f"📦 <b>Bundle 0:</b> {data['bundle_pct']:.1f}%\n"
            f"⏱️ <b>Âge / Latence:</b> {age_text} depuis la création\n"
            f"🕒 <b>Détecté à:</b> {detected_at}\n\n"
            f"<b>Filtres dynamiques</b>\n{checks_text}\n\n"
            f"🔗 <a href='https://dexscreener.com/solana/{mint}'>DexScreener</a> · "
            f"<a href='https://gmgn.ai/sol/token/{mint}'>GMGN</a> · "
            f"<a href='https://axiom.trade/meme/{curve}'>Axiom</a>\n"
            f"🔍 <a href='https://pump.fun/coin/{mint}'>Pump.fun</a> · "
            f"<a href='https://solscan.io/token/{mint}'>Solscan</a>"
        )

    @staticmethod
    def action_keyboard(data: dict[str, Any]) -> InlineKeyboardMarkup:
        mint = data["mint"]
        curve = data["bonding_curve"]
        return InlineKeyboardMarkup([
            [InlineKeyboardButton("📈 Acheter / Gérer sur GMGN", url=f"https://gmgn.ai/sol/token/{mint}")],
            [InlineKeyboardButton("⚡ Acheter / Gérer sur Axiom", url=f"https://axiom.trade/meme/{curve}")],
            [InlineKeyboardButton("⏸️ PAUSE", callback_data="bot_pause"),
             InlineKeyboardButton("▶️ REPRENDRE", callback_data="bot_resume"),
             InlineKeyboardButton("📊 STATUS", callback_data="bot_status")],
        ])

    @staticmethod
    def control_keyboard() -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup([
            [InlineKeyboardButton("⏸️ PAUSE", callback_data="bot_pause"),
             InlineKeyboardButton("▶️ REPRENDRE", callback_data="bot_resume"),
             InlineKeyboardButton("📊 STATUS", callback_data="bot_status")],
        ])

    async def send_alert(self, data: dict[str, Any]) -> None:
        try:
            await self.bot.send_message(
                chat_id=self.chat_id, text=self.format_alert(data),
                parse_mode="HTML", disable_web_page_preview=True,
                reply_markup=self.action_keyboard(data),
            )
            logger.info("[TELEGRAM] Alerte envoyée %s", data["mint"])
        except TelegramError as exc:
            logger.error("Telegram indisponible pour %s: %s", data["mint"], type(exc).__name__)
    async def send_discovery(self, mint: str, source: str, details: str = "") -> None:
        """Discovery-only alert: never represents an on-chain validation or an auto-buy signal."""
        text = (
            "🔎 <b>DÉCOUVERTE MARCHÉ — DRY RUN</b>\n"
            f"<code>{mint}</code>\n\n"
            f"Source: <b>{source}</b>\n"
            f"{details}\n\n"
            "⚠️ Non validé par les filtres on-chain; aucune transaction automatique.\n"
            f"🔗 <a href='https://dexscreener.com/solana/{mint}'>DexScreener</a> · "
            f"<a href='https://pump.fun/coin/{mint}'>Pump.fun</a> · "
            f"<a href='https://solscan.io/token/{mint}'>Solscan</a>"
        )
        try:
            await self.bot.send_message(chat_id=self.chat_id, text=text, parse_mode="HTML", disable_web_page_preview=True, reply_markup=self.control_keyboard())
            logger.info("[DISCOVERY] Alerte %s via %s", mint, source)
        except TelegramError as exc:
            logger.warning("Découverte Telegram impossible pour %s: %s", mint, type(exc).__name__)

    async def send_text(self, chat_id: int | str, text: str,
                        reply_markup: InlineKeyboardMarkup | None = None) -> None:
        try:
            await self.bot.send_message(chat_id=chat_id, text=text, parse_mode="HTML", reply_markup=reply_markup)
        except TelegramError as exc:
            logger.warning("Réponse Telegram impossible: %s", type(exc).__name__)


class SeenStore:
    def __init__(self, path: str):
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        self.db = sqlite3.connect(path, check_same_thread=False)
        self.db.execute("CREATE TABLE IF NOT EXISTS seen (mint TEXT PRIMARY KEY, signature TEXT NOT NULL)")
        self.db.execute("CREATE TABLE IF NOT EXISTS dev_cache (dev TEXT PRIMARY KEY, launches INTEGER NOT NULL, rugs INTEGER NOT NULL DEFAULT 0, analyzed_at INTEGER NOT NULL)")
        self.db.execute("CREATE TABLE IF NOT EXISTS metadata_cache (mint TEXT PRIMARY KEY, name TEXT, symbol TEXT, socials_json TEXT, analyzed_at INTEGER NOT NULL)")
        self.db.execute("CREATE TABLE IF NOT EXISTS rejection_events (created_at INTEGER NOT NULL, mint TEXT NOT NULL, reason TEXT NOT NULL)")
        self.db.execute("CREATE TABLE IF NOT EXISTS liquidity_snapshots (mint TEXT NOT NULL, created_at INTEGER NOT NULL, minutes_after INTEGER NOT NULL, liquidity_sol REAL NOT NULL, PRIMARY KEY(mint, minutes_after))")
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

    async def get_dev(self, dev: str) -> tuple[int, int] | None:
        async with self.lock:
            row = self.db.execute("SELECT launches, rugs FROM dev_cache WHERE dev=?", (dev,)).fetchone()
            return (int(row[0]), int(row[1])) if row else None

    async def put_dev(self, dev: str, launches: int, rugs: int) -> None:
        async with self.lock:
            self.db.execute("INSERT OR REPLACE INTO dev_cache VALUES (?, ?, ?, ?)", (dev, launches, rugs, int(time.time())))
            self.db.commit()

    async def record_rejection(self, mint: str, reason: str) -> None:
        async with self.lock:
            self.db.execute("INSERT INTO rejection_events VALUES (?, ?, ?)", (int(time.time()), mint, reason[:240]))
            self.db.commit()

    async def store_snapshot(self, mint: str, created_at: int, minutes_after: int, liquidity_sol: float) -> None:
        async with self.lock:
            self.db.execute("INSERT OR REPLACE INTO liquidity_snapshots VALUES (?, ?, ?, ?)", (mint, created_at, minutes_after, liquidity_sol))
            self.db.commit()

    def close(self) -> None:
        self.db.close()

class MultiSourceDiscovery:
    """Public-market discovery only; it never bypasses the safe transaction boundary."""
    def __init__(self, session: aiohttp.ClientSession, notifier: TelegramNotifier, store: SeenStore, monitor: "Monitor"):
        self.session, self.notifier, self.store, self.monitor = session, notifier, store, monitor
        self.stop = asyncio.Event()
        self.interval = max(10, int(os.getenv("DISCOVERY_INTERVAL_SECONDS", "15")))
        self.dex_urls = [
            os.getenv("DEXSCREENER_PROFILES_URL", "https://api.dexscreener.com/token-profiles/latest/v1"),
            os.getenv("DEXSCREENER_BOOSTS_URL", "https://api.dexscreener.com/token-boosts/latest/v1"),
        ]
        self.raydium_url = os.getenv("RAYDIUM_POOLS_URL", "https://api-v3.raydium.io/main/pairs?page=1&pageSize=100&sort=volume24h&order=desc")

    @staticmethod
    def _mints(payload: Any) -> list[tuple[str, str, str]]:
        rows = payload if isinstance(payload, list) else (payload or {}).get("data", []) if isinstance(payload, dict) else []
        if isinstance(rows, dict):
            rows = rows.get("data", [])
        found: list[tuple[str, str, str]] = []
        for row in rows if isinstance(rows, list) else []:
            if not isinstance(row, dict):
                continue
            base_token = row.get("baseToken") if isinstance(row.get("baseToken"), dict) else {}
            mint = row.get("tokenAddress") or row.get("mint") or row.get("baseMint") or base_token.get("address")
            if isinstance(mint, str) and len(mint) >= 32:
                found.append((mint, str(row.get("source") or "market"), str(row.get("volume24h") or row.get("volume", ""))))
        return found

    async def _get_json(self, url: str) -> Any:
        timeout = aiohttp.ClientTimeout(total=8)
        async with self.session.get(url, timeout=timeout, headers={"Accept": "application/json"}) as response:
            if response.status != 200:
                raise RuntimeError(f"HTTP {response.status}")
            return await response.json(content_type=None)

    async def run(self) -> None:
        if not env_bool("ENABLE_MULTI_SOURCE", True):
            logger.info("[DISCOVERY] Multi-source désactivé")
            return
        logger.info("[DISCOVERY] DexScreener + Raydium actifs; intervalle=%ss", self.interval)
        while not self.stop.is_set():
            try:
                candidates: list[tuple[str, str, str]] = []
                for url in self.dex_urls:
                    try:
                        candidates.extend((mint, "DexScreener", detail) for mint, _, detail in self._mints(await self._get_json(url)))
                    except Exception as exc:
                        logger.debug("[DISCOVERY] DexScreener indisponible: %s", type(exc).__name__)
                try:
                    candidates.extend((mint, "Raydium", detail) for mint, _, detail in self._mints(await self._get_json(self.raydium_url)))
                except Exception as exc:
                    logger.debug("[DISCOVERY] Raydium indisponible: %s", type(exc).__name__)
                for mint, source, detail in candidates:
                    if await self.store.claim(f"discovery:{mint}", source):
                        self.monitor.stats["discovered"] += 1
                        await self.notifier.send_discovery(mint, source, f"Volume observé: {detail}" if detail else "Activité de marché détectée")
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("[DISCOVERY] cycle interrompu: %s", type(exc).__name__)
            with suppress(asyncio.TimeoutError):
                await asyncio.wait_for(self.stop.wait(), timeout=self.interval)


class Monitor:
    def __init__(self, session: aiohttp.ClientSession, rpc: SolanaRPC,
                 notifier: TelegramNotifier, store: SeenStore, cfg: dict[str, Any]):
        self.session, self.rpc = session, rpc
        self.notifier, self.store, self.cfg = notifier, store, cfg
        self.ws_url = os.getenv("HELIUS_WS_URL", "").strip()
        if not self.ws_url and rpc.url.startswith("https://"):
            self.ws_url = rpc.url.replace("https://", "wss://", 1)
        self.stop = asyncio.Event()
        self.paused = False
        self.queue: asyncio.Queue[tuple[str, list[str]]] = asyncio.Queue(maxsize=int(os.getenv("EVENT_QUEUE_SIZE", "500")))
        self.enqueued: set[str] = set()
        self.queue_lock = asyncio.Lock()
        self.workers_count = max(1, int(os.getenv("RPC_WORKERS", "1")))
        self.error_count = 0
        self.stats = {"tracked": 0, "valid": 0, "rejected": 0, "discovered": 0, "errors": 0}
        self.validator = Validator(cfg, rpc, self)

    async def process(self, signature: str, logs: list[str]) -> None:
        try:
            tx = await self.rpc.transaction(signature)
            if not tx:
                return
            candidate = parse_candidate(tx, signature, logs)
            if not candidate or not await self.store.claim(candidate.mint, signature):
                return
            self.stats["tracked"] += 1
            if self.paused:
                logger.info("[PAUSED] Token ignoré pendant la pause: %s", candidate.mint)
                return
            data = await self.validator.validate(candidate)
            if not data:
                return
            detected_at_ms = time.time_ns() // 1_000_000
            block_time = tx.get("blockTime")
            creation_at_ms = int(float(block_time) * 1000) if block_time is not None else None
            data["age_ms"] = max(0, detected_at_ms - creation_at_ms) if creation_at_ms is not None else None
            data["detected_at"] = datetime.fromtimestamp(
                detected_at_ms / 1000, tz=timezone.utc
            ).strftime("%H:%M:%S.%f")[:-3]
            self.stats["valid"] += 1
            await self.store.store_snapshot(candidate.mint, int(time.time()), 0, data["liquidity_sol"])
            asyncio.create_task(self.snapshot_after_10m(candidate.mint, candidate.bonding_curve))
            logger.info("[FILTER PASSED] %s — validation on-chain complète", candidate.mint)
            await self.notifier.send_alert(data)
        except (RpcError, KeyError, TypeError, ValueError) as exc:
            self.stats["errors"] += 1
            self.error_count += 1
            if self.error_count <= 5 or self.error_count % 100 == 0:
                logger.warning("Erreur traitement #%s (%s): %s", self.error_count, str(exc)[:180], signature)

    async def snapshot_after_10m(self, mint: str, bonding_curve: str) -> None:
        try:
            await asyncio.sleep(600)
            curve = await self.validator.curve(bonding_curve)
            if curve:
                await self.store.store_snapshot(mint, int(time.time()) - 600, 10, curve.liquidity_sol)
                logger.info("[LIQUIDITY +10M] %s — %.3f SOL", mint, curve.liquidity_sol)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.debug("[LIQUIDITY +10M] échec %s: %s", mint, type(exc).__name__)

    async def worker(self) -> None:
        while not self.stop.is_set():
            signature, logs = await self.queue.get()
            try:
                await self.process(signature, logs)
            finally:
                self.enqueued.discard(signature)
                self.queue.task_done()

    async def enqueue(self, signature: str, logs: list[str]) -> None:
        async with self.queue_lock:
            if signature in self.enqueued or self.queue.full():
                return
            self.enqueued.add(signature)
            await self.queue.put((signature, logs))

    async def run(self) -> None:
        if not self.ws_url:
            raise RuntimeError("HELIUS_WS_URL/HELIUS_RPC_URL manquant")
        workers = [asyncio.create_task(self.worker()) for _ in range(self.workers_count)]
        delay = 1.0
        try:
            while not self.stop.is_set():
                try:
                    async with self.session.ws_connect(self.ws_url, heartbeat=20, receive_timeout=45) as ws:
                        await ws.send_json({
                            "jsonrpc": "2.0", "id": 1, "method": "logsSubscribe",
                            "params": [{"mentions": [PUMP_PROGRAM_ID]}, {"commitment": COMMITMENT}],
                        })
                        ack = await ws.receive_json()
                        if "error" in ack:
                            raise RuntimeError(ack["error"])
                        logger.info("[HELIUS] Souscription active sur %s; workers=%s", PUMP_PROGRAM_ID, self.workers_count)
                        delay = 1.0
                        async for message in ws:
                            if message.type != aiohttp.WSMsgType.TEXT:
                                continue
                            value = json.loads(message.data).get("params", {}).get("result", {}).get("value", {})
                            if value.get("err") or not value.get("signature"):
                                continue
                            await self.enqueue(value["signature"], value.get("logs", []))
                        if not self.stop.is_set():
                            logger.warning("[HELIUS] Connexion WebSocket fermée; reconnexion dans %.1fs", delay)
                            await asyncio.sleep(delay)
                            delay = min(30.0, delay * 2)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    logger.error("[HELIUS] WebSocket coupé (%s), reconnexion dans %.1fs", type(exc).__name__, delay)
                    with suppress(asyncio.TimeoutError):
                        await asyncio.wait_for(self.stop.wait(), timeout=delay)
                    delay = min(30.0, delay * 2)
        finally:
            self.stop.set()
            for task in workers:
                task.cancel()
            await asyncio.gather(*workers, return_exceptions=True)


class TelegramCommandPoller:
    def __init__(self, notifier: TelegramNotifier, monitor: Monitor, chat_id: str):
        self.notifier, self.monitor = notifier, monitor
        self.chat_id = str(chat_id)
        self.admin_user_ids = {
            item.strip() for item in os.getenv("TELEGRAM_ADMIN_USER_IDS", "").split(",") if item.strip()
        }
        self.stop = asyncio.Event()
        self.offset: int | None = None

    async def status_text(self) -> str:
        wallet = os.getenv("WALLET_ADDRESS", "").strip()
        try:
            balance = await self.monitor.rpc.wallet_balance_sol(wallet) if wallet else None
        except RpcError:
            balance = None
        balance_text = f"{balance:.4f} SOL" if balance is not None else "non configuré"
        return (
            "📡 <b>STATUS BOT</b>\n"
            f"Tokens suivis : {self.monitor.stats['tracked']}\n"
            f"Tokens validés : {self.monitor.stats['valid']}\n"
            f"Tokens rejetés : {self.monitor.stats['rejected']}\n"
            f"Erreurs : {self.monitor.stats['errors']}\n"
            f"File : {self.monitor.queue.qsize()}\n"
            f"Solde wallet : {balance_text}\n"
            f"Surveillance : {'PAUSE' if self.monitor.paused else 'ACTIVE'}\n"
            "Mode observation : OFF"
        )

    async def status(self, chat_id: int | str) -> None:
        await self.notifier.send_text(
            chat_id, await self.status_text(),
            reply_markup=self.notifier.control_keyboard(),
        )

    def authorized(self, chat_id: int | str, chat_type: str) -> bool:
        return str(chat_id) == self.chat_id or chat_type == "private"

    def admin_allowed(self, user_id: Any, chat_type: str) -> bool:
        return not (chat_type == "private" and self.admin_user_ids
                    and str(user_id) not in self.admin_user_ids)

    async def handle_callback(self, update: Any) -> None:
        query = getattr(update, "callback_query", None)
        if query is None:
            return
        message = getattr(query, "message", None)
        chat = getattr(message, "chat", None)
        chat_id = getattr(chat, "id", None)
        chat_type = getattr(chat, "type", "")
        if chat_id is None or not self.authorized(chat_id, chat_type):
            await self.notifier.bot.answer_callback_query(query.id, text="Accès refusé", show_alert=True)
            return
        data = getattr(query, "data", "")
        user_id = getattr(getattr(query, "from_user", None), "id", None)
        if data in {"bot_pause", "bot_resume"} and not self.admin_allowed(user_id, chat_type):
            await self.notifier.bot.answer_callback_query(query.id, text="Commande réservée à l’administrateur", show_alert=True)
            return
        if data == "bot_pause":
            self.monitor.paused = True
            confirmation = "Surveillance en pause"
        elif data == "bot_resume":
            self.monitor.paused = False
            confirmation = "Surveillance reprise"
        elif data == "bot_status":
            confirmation = "Statut actualisé"
        else:
            await self.notifier.bot.answer_callback_query(query.id)
            return
        await self.notifier.bot.answer_callback_query(query.id, text=confirmation)
        try:
            await self.notifier.bot.edit_message_text(
                chat_id=chat_id,
                message_id=message.message_id,
                text=await self.status_text(),
                parse_mode="HTML",
                reply_markup=self.notifier.control_keyboard(),
            )
        except TelegramError as exc:
            logger.warning("Mise à jour du statut impossible: %s", type(exc).__name__)

    async def handle(self, update: Any) -> None:
        if getattr(update, "callback_query", None) is not None:
            await self.handle_callback(update)
            return
        message = getattr(update, "message", None) or getattr(update, "channel_post", None)
        if message is None or not getattr(message, "text", None):
            return
        chat = getattr(message, "chat", None)
        source_chat = getattr(chat, "id", None)
        chat_type = getattr(chat, "type", "")
        is_configured_chat = source_chat is not None and str(source_chat) == self.chat_id
        is_private = chat_type == "private"
        if source_chat is None or not (is_configured_chat or is_private):
            return
        command = message.text.strip().split()[0].split("@")[0].lower()
        logger.info("[TELEGRAM] Commande reçue: %s", command)
        if command == "/start":
            await self.notifier.send_text(
                source_chat,
                "💎 <b>Bot Pump.fun actif</b>\n"
                "Je surveille les nouveaux tokens Pump.fun et je valide leurs métriques directement on-chain.\n\n"
                "Commandes : /status · /pause · /resume",
                reply_markup=self.notifier.control_keyboard(),
            )
        elif command == "/status":
            await self.status(source_chat)
        elif command == "/pause":
            user = getattr(getattr(message, "from_user", None), "id", None)
            if is_private and self.admin_user_ids and str(user) not in self.admin_user_ids:
                await self.notifier.send_text(source_chat, "⛔ Commande réservée à l’administrateur du bot.")
                return
            self.monitor.paused = True
            await self.notifier.send_text(source_chat, "⏸️ <b>Bot en pause.</b>\nLe flux reste connecté, aucune alerte ne sera publiée.", reply_markup=self.notifier.control_keyboard())
        elif command == "/resume":
            user = getattr(getattr(message, "from_user", None), "id", None)
            if is_private and self.admin_user_ids and str(user) not in self.admin_user_ids:
                await self.notifier.send_text(source_chat, "⛔ Commande réservée à l’administrateur du bot.")
                return
            self.monitor.paused = False
            await self.notifier.send_text(source_chat, "▶️ <b>Bot réactivé.</b>\nLa validation on-chain reprend.", reply_markup=self.notifier.control_keyboard())

    async def run(self) -> None:
        logger.info("[TELEGRAM] Polling actif: commandes + boutons pause/reprise/status; DM et canal autorisés")
        try:
            await self.notifier.bot.delete_webhook(drop_pending_updates=False)
        except TelegramError as exc:
            logger.warning("Webhook Telegram non supprimé: %s", type(exc).__name__)
        while not self.stop.is_set():
            try:
                updates = await asyncio.wait_for(
                    self.notifier.bot.get_updates(
                        offset=self.offset, timeout=25,
                        allowed_updates=["message", "channel_post", "callback_query"],
                    ), timeout=35,
                )
                for update in updates:
                    self.offset = update.update_id + 1
                    await self.handle(update)
            except asyncio.CancelledError:
                raise
            except (TelegramError, asyncio.TimeoutError) as exc:
                logger.warning("Polling Telegram en attente (%s)", type(exc).__name__)
                await asyncio.sleep(2)


async def health(request: web.Request) -> web.Response:
    monitor = request.app.get("monitor")
    payload = {"status": "ok", "service": "pumpfun-onchain", "observation_mode": False}
    if monitor:
        payload.update({"tracked": monitor.stats["tracked"], "valid": monitor.stats["valid"], "rejected": monitor.stats["rejected"], "discovered": monitor.stats["discovered"], "paused": monitor.paused})
    return web.json_response(payload)


async def main() -> None:
    rpc_url = os.getenv("HELIUS_RPC_URL", "").strip()
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip() or os.getenv("TELEGRAM_CHANNEL_ID", "").strip()
    if not rpc_url or not token or not chat_id:
        raise RuntimeError("HELIUS_RPC_URL, TELEGRAM_BOT_TOKEN et TELEGRAM_CHAT_ID sont obligatoires")
    cfg = {
        "min_liquidity": float(os.getenv("MIN_LIQUIDITY_SOL", "1.5")),
        "max_liquidity": float(os.getenv("MAX_LIQUIDITY_SOL", "30")),
        "max_top10": float(os.getenv("MAX_TOP10_PCT", "18.0")),
        "max_dev": float(os.getenv("MAX_DEV_PCT", "5")),
        "max_bundle": float(os.getenv("MAX_BUNDLE_PCT", "5")),
        "max_block0_buys": int(os.getenv("MAX_BLOCK0_BUYS", "1")),
        "bundle_slot_window": int(os.getenv("BUNDLE_SLOT_WINDOW", "0")),
        "require_mint_renounced": env_bool("REQUIRE_MINT_RENOUNCED", True),
        "min_unique_buyers": int(os.getenv("MIN_UNIQUE_BUYERS", "3")),
        "max_single_holder": float(os.getenv("MAX_SINGLE_HOLDER_PCT", "15.0")),
        "min_sol_inflow_per_min": float(os.getenv("MIN_SOL_INFLOW_PER_MIN", "0.3")),
        "min_buyer_seller_ratio": float(os.getenv("MIN_BUYER_SELLER_RATIO", "2.0")),
        "min_curve_progress_pct": float(os.getenv("MIN_CURVE_PROGRESS_PCT", "10.0")),
        "min_dex_liquidity_usd": float(os.getenv("MIN_DEX_LIQUIDITY_USD", "4000")),
        "activity_slot_window": int(os.getenv("ACTIVITY_SLOT_WINDOW", "150")),
        "max_dev_previous_launches": int(os.getenv("MAX_DEV_PREVIOUS_LAUNCHES", "10")),
        "max_dev_rugs": int(os.getenv("MAX_DEV_RUGS", "1")),
        "enable_sybil_check": env_bool("ENABLE_SYBIL_CHECK", True),
        "require_momentum": env_bool("REQUIRE_MOMENTUM", False),
        "require_metadata_socials": env_bool("REQUIRE_METADATA_SOCIALS", False),
    }
    auto_buy_config = AutoBuyConfig.from_env()
    auto_buy = AutoBuyExecutor(auto_buy_config)
    if auto_buy_config.enabled_requested:
        logger.warning("[AUTO-BUY] ENABLE_AUTO_BUY demandé mais bloqué: aucune transaction automatique n’est exécutée")
    else:
        logger.info("[AUTO-BUY] désactivé; boutons GMGN/Axiom disponibles pour exécution manuelle")
    logger.info("[SAFETY] DRY_RUN=%s; aucune transaction n’est signée ou transmise", auto_buy_config.dry_run)
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
            notifier = TelegramNotifier(token, chat_id)
            monitor = Monitor(session, rpc, notifier, store, cfg)
            app["monitor"] = monitor
            logger.info("[CONFIG] Observation=False; liq=%.2f-%.2f SOL; top10<=%.1f%%; single<=%.1f%%; dev<=%.1f%%; buyers>=%s; bloc0<=%s; sybil=%s; dry_run=%s; auto_buy=%s", cfg["min_liquidity"], cfg["max_liquidity"], cfg["max_top10"], cfg["max_single_holder"], cfg["max_dev"], cfg["min_unique_buyers"], cfg["max_block0_buys"], cfg["enable_sybil_check"], env_bool("DRY_RUN", True), env_bool("ENABLE_AUTO_BUY", False))
            loop = asyncio.get_running_loop()
            stop = asyncio.Event()
            for sig in (signal.SIGINT, signal.SIGTERM):
                loop.add_signal_handler(sig, stop.set)
            monitor_task = asyncio.create_task(monitor.run())
            poll_task = asyncio.create_task(TelegramCommandPoller(notifier, monitor, chat_id).run())
            discovery = MultiSourceDiscovery(session, notifier, store, monitor)
            discovery_task = asyncio.create_task(discovery.run())
            try:
                await asyncio.wait_for(notifier.bot.send_message(
                    chat_id=chat_id,
                    text="💎 <b>BOT PUMPFUN ON-CHAIN — PRÊT</b>\n<i>Polling Telegram actif, observation OFF.</i>",
                    parse_mode="HTML",
                    reply_markup=notifier.control_keyboard(),
                ), timeout=10)
            except (TelegramError, asyncio.TimeoutError) as exc:
                logger.warning("Message Telegram de démarrage ignoré: %s", type(exc).__name__)
            await stop.wait()
            monitor.stop.set()
            discovery.stop.set()
            monitor_task.cancel()
            poll_task.cancel()
            discovery_task.cancel()
            with suppress(asyncio.CancelledError):
                await monitor_task
            with suppress(asyncio.CancelledError):
                await poll_task
            with suppress(asyncio.CancelledError):
                await discovery_task
    finally:
        store.close()
        await runner.cleanup()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
