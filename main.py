import os
import sys
import time
import asyncio
import aiohttp
import logging
from typing import Dict, Any, List, Optional
from telegram import Bot
from telegram.error import TelegramError

# --- CONFIGURATION DU LOGGING PROFESSIONNEL (RENDER COMPATIBLE) ---
logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] ⚡ %(message)s",
    datefmt="%H:%M:%S"
)
logger = logging.getLogger("QuantumAlphaSniper")

class QuantumAlphaEngine:
    """
    Moteur mathématique quantique et stochastique pour l'évaluation de pépites sur Pump.fun.
    Combine ratio de liquidité, sécurité des détenteurs, vélocité temporelle et suprématie d'élite.
    """
    def __init__(
        self, 
        min_liq: float = 3.0, 
        max_liq: float = 30.0, 
        max_top10: float = 20.0, 
        max_dev: float = 5.0, 
        min_score: float = 80.0
    ):
        self.min_liq = min_liq
        self.max_liq = max_liq
        self.max_top10 = max_top10
        self.max_dev = max_dev
        self.min_score = min_score
        
        # Base de données comportementale des portefeuilles d'élite (Smart Money / Top Traders)
        self.elite_wallets = {
            "decu_wallet_address_placeholder_1", 
            "elite_trader_placeholder_2"
        }

    def evaluate_token(self, data: Dict[str, Any]) -> tuple[float, bool, str]:
        """
        Analyse chirurgicale complète d'un token.
        Renvoie : (Score Alpha [0-100], Booléen de validation suprême, Motif de rejet éventuel)
        """
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

        # --- 1. FILTRES DURS D'IMMUNITÉ (DISQUALIFICATION INSTANTANÉE) ---
        if not dex_paid:
            return 0.0, False, "DEX non payé (Unpaid)"
        if not (self.min_liq <= liq <= self.max_liq):
            return 0.0, False, f"Liquidité hors limites ({liq} SOL)"
        if top10 > self.max_top10:
            return 0.0, False, f"Top 10 holders trop élevé ({top10}%)"
        if dev > self.max_dev:
            return 0.0, False, f"Wallet Dev trop lourd ({dev}%)"
        if tax_pct > 2.0:
            return 0.0, False, f"Taxe excessive ({tax_pct}%)"
        if mc <= 0:
            return 0.0, False, "Market Cap invalide"

        # --- 2. MODÈLE PROBABILISTE STOCHASTIQUE (INDICE ALPHA) ---
        # Ratio Liquidité / Market Cap (Cible mathématique optimale à ~25%)
        lm_ratio = liq / mc
        ratio_score = max(0.0, 1.0 - abs(lm_ratio - 0.25) / 0.25)

        # Indice de sécurité (Pénalités exponentielles de concentration)
        concentration_penalty = (top10 / 100.0) * 1.5 + (dev / 100.0) * 2.0 + (bundle / 100.0) * 3.0
        safety_score = max(0.0, 1.0 - concentration_penalty)

        # Facteur de vélocité temporelle (Fenêtre de tir idéale entre 0.5 et 5 minutes)
        if age_minutes <= 0.5:
            time_score = 0.6  # Trop frais, risque de sandwich bot immédiat
        elif age_minutes <= 5.0:
            time_score = 1.0  # Zone Alpha parfaite
        else:
            time_score = max(0.1, 1.0 - (age_minutes - 5.0) / 25.0)

        # --- 3. FILTRE DE SUPRÉMATIE DES WALLETS D'ÉLITE ---
        elite_count = sum(1 for w in early_buyers if w in self.elite_wallets)
        elite_bonus = 10.0 if elite_count >= 2 else (5.0 if elite_count == 1 else 0.0)

        # Calcul bayésien global pondéré
        base_alpha = (0.35 * ratio_score + 0.45 * safety_score + 0.20 * time_score) * 100.0
        final_score = min(100.0, base_alpha + elite_bonus)

        # Règle de validation suprême : Valide uniquement si le score atteint l'exigence
        is_valid = final_score >= self.min_score
        return round(final_score, 2), is_valid, "Validé par le noyau quantique"


class TelegramNotifier:
    """
    Diffuseur d'alertes Telegram ultra-rapide et structuré chirurgicalement.
    """
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
        
        # Formatage de la durée de vie de façon lisible
        age_str = f"{int(age_min)} minute(s)" if age_min >= 1 else f"{int(age_min * 60)} secondes"

        # Message structuré avec émojis ordonnés selon le cahier des charges
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
            logger.info(f"Alerte Telegram transmise pour le token {symbol} ({mint})")
        except TelegramError as e:
            logger.error(f"Erreur d'envoi Telegram: {e}")


async def main():
    logger.info("🚀 Démarrage du noyau Quantum Alpha Sniper (Mode Ultime)...")
    
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    channel = os.getenv("TELEGRAM_CHANNEL_ID")
    
    if not token or not channel:
        logger.critical("❌ Variables d'environnement Telegram manquantes sur Render !")
        return

    notifier = TelegramNotifier(token, channel)
    engine = QuantumAlphaEngine(min_liq=3.0, max_liq=30.0, min_score=80.0)

    startup_msg = (
        "💎 <b>QUANTUM ALPHA BOT — SYSTÈME OPÉRATIONNEL</b>\n"
        "<i>Filtres de suprématie d'élite & stochastique activés.</i>\n\n"
        "<code>/status • /active_radar • /mode_quant</code>"
    )
    
    try:
        await notifier.bot.send_message(chat_id=channel, text=startup_msg, parse_mode="HTML")
    except Exception as e:
        logger.error(f"Impossible d'envoyer le message de démarrage : {e}")

    # Boucle principale de haute fréquence
    while True:
        try:
            # --- POINT D'INTÉGRATION DU FLUX WEBSOCKET PUMP.FUN ---
            # Ici, ton récepteur WebSocket alimente les objets entrants en temps réel.
            await asyncio.sleep(5)
            
            # Exemple de simulation de test interne de la boucle
            logger.info("Radar quantique en veille active sur les blocs Solana...")

        except asyncio.CancelledError:
            logger.warning("Arrêt du système demandé.")
            break
        except Exception as e:
            logger.error(f"Erreur critique dans la boucle principale : {e}")
            await asyncio.sleep(3)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Arrêt manuel du bot.")
