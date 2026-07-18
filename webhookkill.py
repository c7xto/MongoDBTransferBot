"""
webhookkill.py — one-off utility: check for and delete any webhook set on a
bot token, so Telegram falls back to long-polling (getUpdates), which is
what Pyrogram uses to receive updates.

If a webhook was ever set on this token (even from an old/unrelated
deployment), Telegram will NOT deliver updates via long-polling at all —
the bot process runs fine, connects fine, but every /start and button tap
simply never arrives. That looks identical to a "deaf" bot from the
outside, so this is worth ruling out directly.

NOTE: pyrogram.Client has no delete_webhook() method — deleting a webhook
is a Bot HTTP API call (api.telegram.org/bot<token>/deleteWebhook), not an
MTProto operation, so this talks to Telegram's HTTP API directly instead
of going through Pyrogram. No event loop, no async, nothing to shim.
"""
import json
import os
import sys
import urllib.request

# Token is read from the environment / .env — never hardcode a live bot
# token in a file that gets committed to a repository.
def _load_token() -> str:
    token = os.environ.get("BOT_TOKEN", "").strip()
    if not token and os.path.isfile(".env"):
        with open(".env") as fh:
            for raw in fh:
                raw = raw.strip()
                if raw.startswith("BOT_TOKEN") and "=" in raw:
                    token = raw.partition("=")[2].strip().strip('"').strip("'")
                    break
    if not token:
        sys.exit("BOT_TOKEN not found — set it in your environment or .env file.")
    return token


BOT_TOKEN = _load_token()


def _call(method: str, token: str) -> dict:
    url = f"https://api.telegram.org/bot{token}/{method}"
    with urllib.request.urlopen(url, timeout=10) as resp:
        return json.loads(resp.read().decode())


def get_webhook_info(token: str) -> dict:
    data = _call("getWebhookInfo", token)
    result = data.get("result", {})
    print(f"Current webhook info: {result}")
    return result


def delete_webhook(token: str) -> None:
    data = _call("deleteWebhook?drop_pending_updates=false", token)
    if data.get("ok"):
        print(f"Webhook deleted. result={data.get('result')} description={data.get('description')}")
    else:
        print(f"Failed to delete webhook: {data}")


if __name__ == "__main__":
    info = get_webhook_info(BOT_TOKEN)
    if info.get("url"):
        print(f"⚠️  A webhook IS set ({info['url']!r}) — this blocks long-polling entirely. Deleting it now…")
    else:
        print("No webhook is currently set — long-polling should be unaffected.")
    delete_webhook(BOT_TOKEN)
