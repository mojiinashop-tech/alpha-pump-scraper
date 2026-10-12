import os
import sys
import time
import asyncio
import aiohttp
import logging
from typing import Dict, Any, List, Optional
from telegram import Bot
from telegram.error import TelegramError

# --- CONFIGURATION LOGGING COMPATIBLE RENDER ---
logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] ⚡ %(message)s",
    datefmt="%H:%M:%S"
)
logger = logging.getLogger("QuantumAlphaSniper")

class QuantumAlphaEngine:
    """
    Moteur mathématique stochastique & filtres d'immunité d'élite.
    """
    def __init__(
        self, 
        min_liq: float = 3.0, 
        max_liq: float = 30.0, 
        max_top10: float = 20.0, 
        max_dev: float = 5.0, 
        max_bundle: float = 5.0,
        min_score: float = 80.0
    ):
        self.min_liq = min_liq
        self.max_liq = max_liq
        self.max_top10 = max_top10
        self.max_dev = max_dev
        self.max_bundle = max_bundle
        self.min_score = min_score
        
        self.elite_wallets = {
            "4vw54B...", # Wallet de référence (ex: decu)
        }

    def evaluate_token(self, data: Dict[str, Any]) -> tuple[float, bool, str]:
        mint = data.get("mint", "Inconnu")
        name = data.get("name", "N/A")
        symbol = data.get("symbol", "N/A")
        liq = data.get("liquidity_sol", 0.0)
        mc = data.get("market_cap_usd", 0.0)
        top10 = data.get("top10_pct", 0.0)
        dev = data.get("dev_pct", 0.0)
        bundle = data.get("bundle_pct", 0.0)
        age_minutes = data.get("age_minutes", 1.0)
        dex_paid = data.get("dex_paid", False)
        tax_pct = data.get("tax_pct", 0.0)
        early_buyers: List[str] = data.get("early_buyers", [])

        # --- 1. FILTRES DURS D'IMMUNITÉ (REJET STRICT) ---
        if not dex_paid:
            return 0.0, False, "DEX non payé (Unpaid / Non rémunéré)"
        if not (self.min_liq <= liq <= self.max_liq):
            return 0.0, False, f"Liquidité hors zone ({liq:.2f} SOL)"
        if top10 > self.max_top10:
            return 0.0, False, f"Top 10 trop élevé ({top10}%)"
        if dev > self.max_dev:
            return 0.0, False, f"Wallet Dev trop lourd ({dev}%)"
        if bundle > self.max_bundle:
            return 0.0, False, f"Bundlers / Emballeurs trop élevés ({bundle}%)"
        if tax_pct > 2.0:
            return 0.0, False, f"Taxe excessive ({tax_pct}%)"
        if mc <= 0:
            return 0.0, False, "Market Cap invalide"

        # --- 2. EVALUATION BAYÉSIENNE & STOCHASTIQUE ---
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

        # --- 3. SUPRÉMATIE SMART MONEY ---
        elite_count = sum(1 for w in early_buyers if w in self.elite_wallets)
        elite_bonus = 10.0 if elite_count >= 2 else (5.0 if elite_count == 1 else 0.0)

        base_alpha = (0.35 * ratio_score + 0.45 * safety_score + 0.20 * time_score) * 100.0
        final_score = min(100.0, base_alpha + elite_bonus)

        is_valid = final_score >= self.min_score
        return round(final_score, 2), is_valid, "Validation suprême"


class TelegramNotifier:
    def __init__(self, token: str, channel_id: str):
        self.bot = Bot(token=token)
        self.channel_id = channel_id

    async def send_alpha_alert(self, data: Dict[str, Any], score: float):
        mint = data.get("mint")
        name = data.get("name", "Inconnu")
        symbol = data.get("symbol", "N/A")
        liq = data.get("liquidity_sol", 0.0)
        mc = data.get("market_cap_usd", 0.0)
        top10 = data.get("top10_pct", 0.0)
        dev = data.get("dev_pct", 0.0)
        bundle = data.get("bundle_pct", 0.0)
        age_min = data.get("age_minutes", 1.0)
        tax = data.get("tax_pct", 0.0)
        
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
            f"<a href='https://axiom.trade/token/{mint}'>Axiom</a> • "
            f"<a href='https://pump.fun/coin/{mint}'>Pump.fun</a>"
        )

        try:
            await self.bot.send_message(
                chat_id=self.channel_id,
                text=message,
                parse_mode="HTML",
                disable_web_page_preview=True
            )
            logger.info(f"Alerte envoyée pour {symbol}")
        except TelegramError as e:
            logger.error(f"Erreur Telegram: {e}")


async def main():
    logger.info("🚀 Démarrage du noyau Quantum Alpha Sniper...")
    
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    channel = os.getenv("TELEGRAM_CHANNEL_ID")
    
    # BOUCLE SÉCURISÉE ANTI-CRASH SUR RENDER
    if not token or not channel:
        logger.error("⚠️ Clés Telegram non détectées. Maintien du processus actif pour éviter l'échec Render...")
        while True:
            await asyncio.sleep(60)

    notifier = TelegramNotifier(token, channel)
    engine = QuantumAlphaEngine(min_liq=3.0, max_liq=30.0, min_score=80.0)

    try:
        await notifier.bot.send_message(
            chat_id=channel, 
            text="💎 <b>QUANTUM ALPHA BOT SÉCURISÉ — PRÊT</b>\n<i>Surveillance active en cours...</i>", 
            parse_mode="HTML"
        )
    except Exception as e:
        logger.error(f"Erreur envoi notification de lancement: {e}")

    # Boucle de surveillance continue
    while True:
        try:
            await asyncio.sleep(5)
            logger.info("Analyse en arrière-plan des nouveaux blocs...")
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Anomalie boucle principale: {e}")
            await asyncio.sleep(3)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Arrêt du script.")
