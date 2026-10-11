import os
import asyncio
import logging
import json
import websockets
import requests

# Configuration des logs pour voir ce qui se passe
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO
)

# Récupération sécurisée des variables d'environnement (configurées sur Render)
PUMP_PROGRAM_ID = os.getenv("PUMP_PROGRAM_ID", "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P")
SOLANA_WS_URL = os.getenv("SOLANA_WS_URL")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# Vérification que toutes les clés sont bien présentes
if not SOLANA_WS_URL or not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
    logging.error("❌ ERREUR : Il manque des variables d'environnement sur Render !")
    exit(1)

def send_telegram_alert(message):
    """Fonction pour envoyer l'alerte sur Telegram"""
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "Markdown",
        "disable_web_page_preview": True
    }
    try:
        response = requests.post(url, json=payload, timeout=10)
        if response.status_code != 200:
            logging.error(f"Erreur d'envoi Telegram : {response.text}")
    except Exception as e:
        logging.error(f"Exception lors de l'envoi Telegram : {e}")

async def listen_pump_fun():
    """Écoute en temps réel les lancements sur Pump.fun via WebSocket Helius"""
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "logsSubscribe",
        "params": [
            {"mentions": [PUMP_PROGRAM_ID]},
            {"commitment": "confirmed"}
        ]
    }

    logging.info("🚀 Démarrage de l'écouteur de flux Pump.fun...")
    send_telegram_alert("🟢 *Bot Pump.fun démarré avec succès !* En attente des nouveaux lancements...")

    while True:
        try:
            async with websockets.connect(SOLANA_WS_URL, ping_interval=20, ping_timeout=20) as websocket:
                await websocket.send(json.dumps(payload))
                logging.info("✅ Connecté au flux WebSocket Helius avec succès.")

                async for raw_message in websocket:
                    try:
                        data = json.loads(raw_message)
                        
                        # Vérification s'il s'agit d'une notification de log
                        if "params" in data and "result" in data["params"]:
                            value = data["params"]["result"]["value"]
                            logs = value.get("logs", [])
                            signature = value.get("signature", "")

                            # On détecte l'initialisation d'un token sur Pump.fun
                            is_creation = any("initialize" in log.lower() or "create" in log.lower() for log in logs)

                            if is_creation:
                                dex_link = f"https://dexscreener.com/solana/{signature}"
                                pump_link = f"https://pump.fun/{signature}"
                                
                                alert_text = (
                                    "🚨 *NOUVEAU TOKEN PUMP.FUN DÉTECTÉ !* 🚨\n\n"
                                    f"🔗 *Signature :* `{signature}`\n\n"
                                    f"📊 [Voir sur Dexscreener]({dex_link})\n"
                                    f"💊 [Voir sur Pump.fun]({pump_link})"
                                )
                                
                                logging.info(f"Nouveau token détecté ! Signature: {signature}")
                                send_telegram_alert(alert_text)

                    except json.JSONDecodeError:
                        continue
                    except Exception as e:
                        logging.error(f"Erreur de traitement d'un message : {e}")

        except websockets.exceptions.ConnectionClosed as e:
            logging.warning(oss := f"⚠️ Connexion WebSocket perdue ({e}). Reconnexion dans 5 secondes...")
            await asyncio.sleep(5)
        except Exception as e:
            logging.error(f"⚠️ Erreur de connexion réseau : {e}. Nouvelle tentative dans 5 secondes...")
            await asyncio.sleep(5)

if __name__ == "__main__":
    try:
        asyncio.run(listen_pump_fun())
    except KeyboardInterrupt:
        logging.info("🛑 Arrêt du bot demandé par l'utilisateur.")
