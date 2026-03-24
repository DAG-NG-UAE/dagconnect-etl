import requests
import os
from dotenv import load_dotenv
import logging

load_dotenv()
logger = logging.getLogger(__name__)

class Alert:
    @staticmethod
    def send_alert(message: str):
        """Send an alert to the user (locally and via Teams if configured)."""
        webhook_url = os.getenv("TEAMS_WEBHOOK_URL")
        logger.warning(f"ETL ALERT: {message}")
        
        if webhook_url:
            try:
                # Adaptive Card payload for "Post message in a chat or channel"
                payload = {
                    "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                    "type": "AdaptiveCard",
                    "version": "1.4",
                    "body": [
                        {
                            "type": "TextBlock",
                            "text": "⚠️ ETL Alert",
                            "weight": "Bolder",
                            "size": "Medium",
                            "color": "Attention"
                        },
                        {
                            "type": "TextBlock",
                            "text": message,
                            "wrap": True
                        }
                    ]
                }
                response = requests.post(webhook_url, json=payload)
                logger.info("Teams alert sent successfully. 🔔")
            except Exception as e:
                logger.error(f"Failed to send Teams alert: {e}")
        else:
            logger.info("Set TEAMS_WEBHOOK_URL in .env to receive these alerts in MS Teams.")
