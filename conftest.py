import asyncio
import os
import sys

# config.py imports pyrogram.utils at module level, which triggers
# Pyrogram's own sync.py calling asyncio.get_event_loop() at import time —
# Python 3.14 raises RuntimeError instead of silently creating a loop when
# none exists for the thread. Mirrors the jumpstart block at the top of
# mdb.py; must run before anything imports config/db.
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
try:
    asyncio.get_event_loop()
except RuntimeError:
    _jumpstart_loop = asyncio.new_event_loop()
    asyncio.set_event_loop(_jumpstart_loop)

# config.py calls _req(...) at import time for these and sys.exit(1)s if any
# are missing — set dummy values before any test module imports config/db.
os.environ.setdefault("BOT_TOKEN", "123456789:TEST0DUMMY0TOKEN0FOR0PYTEST0ONLY12")
os.environ.setdefault("TELEGRAM_API_ID", "12345")
os.environ.setdefault("TELEGRAM_API_HASH", "0123456789abcdef0123456789abcdef")
os.environ.setdefault("MONGO_URI", "mongodb://testuser:testpass@127.0.0.1:27017/test")
