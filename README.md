# C7 MongoDB Transfer Bot

> Copy Telegram files from a MongoDB collection to your own Telegram channel.

The bot reads saved Telegram file IDs from MongoDB and sends the files directly to a channel you choose. Files are not downloaded or uploaded again, so transfers are fast and do not use server storage.

---

## ✨ What it can do

- Copy movies, series and other Telegram files to your channel
- Continue from the last saved position after a restart
- Skip files that were already transferred
- Pause, resume or stop a transfer
- Show live transfer progress
- Watch the source database for newly added files
- Work with both old and new movie-bot database formats
- Keep each user's settings and progress separate

---

## 🔄 How it works

```text
Source MongoDB
      ↓
Transfer Bot reads the saved file IDs
      ↓
Worker Bot sends the files
      ↓
Your Telegram Channel
      ↓
Your Movie Bot indexes the channel
```

The worker bot must already be able to use the file IDs stored in the source database.

---

## ✅ Before you start

You need:

1. A parent bot token for opening the transfer menu
2. A Telegram API ID and API hash from [my.telegram.org](https://my.telegram.org)
3. A MongoDB database for saving settings and transfer progress
4. The source bot token and source MongoDB details
5. A target Telegram channel

Add both the worker bot and your movie bot as administrators in the target channel.

---

## 🚀 Installation

### 1. Download the bot

```bash
git clone https://github.com/c7xto/MongoDBTransferBot.git
cd MongoDBTransferBot
```

### 2. Install the packages

```bash
pip install -r requirements.txt
```

### 3. Create a `.env` file

```env
BOT_TOKEN=parent_bot_token
TELEGRAM_API_ID=telegram_api_id
TELEGRAM_API_HASH=telegram_api_hash
MONGO_URI=mongodb_uri_for_bot_settings
ADMIN_ID=your_telegram_user_id
```

Optional settings:

```env
STATE_MONGO_URI=mongodb_uri_for_transfer_progress
STATE_DB_PREFIX=c7_runtime
ENCRYPTION_KEY=your_generated_key
PORT=8080
```

`STATE_MONGO_URI` can be the same as `MONGO_URI`. Using a separate database is optional.

`ENCRYPTION_KEY` is needed only for `/prescan`. Create one with:

```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

### 4. Start the bot

```bash
python mdb.py
```

Open the parent bot in Telegram and send `/start`.

---

## 🧙 Telegram setup

Send `/setup` and enter the requested information:

| Setting | What to enter |
|---|---|
| API ID | Telegram API ID used by the worker bot |
| API hash | Telegram API hash used by the worker bot |
| Bot token | Token of the bot that can send the source files |
| Database URI | Source MongoDB connection address |
| Database | Database containing the files |
| Collection | Collection containing the file records |
| Target | Target channel ID, such as `-1001234567890` |

The worker bot must be an administrator in the target channel before starting.

---

## ▶️ Recommended transfer order

1. Finish `/setup`
2. Add the required bots to the target channel
3. Run `/prescan` if the channel already contains files
4. Run `/transfer` for the first full transfer
5. Run `/monitor` after the full transfer finishes

`/prescan` prevents files already present in the channel from being sent again.
It publishes a live three-step Telegram status card with progress, elapsed time,
completion/failure details, and a Stop button.

`/monitor` watches for new source records and copies them as they are added.
Its persistent status card shows startup checks, forwarded/failed counts,
rate-limit waits, reconnects, and the final stopped or failed state.

Transfer, Pre-Scan, and Live Monitor are mutually exclusive so they cannot
race over the same delivery ledger. Emergency Stop stops whichever operation
is active, and destructive wipe/reset actions are blocked while work is running.

---

## 🎮 Commands

| Command | Action |
|---|---|
| `/start` | Open the main menu |
| `/setup` | Add or change transfer details |
| `/transfer` | Start transferring files |
| `/stop` | Stop the current transfer safely |
| `/monitor` | Watch for newly added files |
| `/stopmonitor` | Stop watching for new files |
| `/prescan` | Check files already in the target channel |
| `/stopprescan` | Stop an active pre-scan safely |
| `/stats` | Show transfer totals and progress |
| `/config` | Show saved settings with secrets hidden |
| `/wipe` | Clear saved progress and duplicate records |

Use `/wipe` carefully. It does not delete source files, but it clears the bot's saved transfer history.

---

## ☁️ Hosting

Use this start command on most hosting services:

```bash
pip install --no-cache-dir -r requirements.txt && python mdb.py
```

Add the same environment settings from the `.env` example to your hosting panel. Do not upload your real `.env` file to GitHub.

---

## 🔄 Updating

```bash
git pull origin main
pip install -r requirements.txt
```

Restart the bot after updating. Saved settings and transfer progress will remain in MongoDB.

---

## 🛠️ Common problems

### The bot cannot send to the channel

- Check that the worker bot is a channel administrator
- Check that the target channel ID starts with `-100`
- Make sure the worker bot has permission to post messages

### Files are being skipped

The files may already be saved in the transfer history or found by `/prescan`. Check `/stats` before clearing anything.

### The bot restarted during a transfer

Start the transfer again and choose to continue. The bot will use its saved progress and skip completed files.

### New files are not appearing

Finish the full transfer first, then start `/monitor`. Also confirm that new records are being added to the selected database and collection.

### A file fails to send

The saved Telegram file ID may belong to a different bot or may no longer be usable. Confirm that the configured worker bot can access that file.

---

## 🔐 Keep your credentials safe

- Never post bot tokens, API hashes or MongoDB passwords publicly
- Never commit your `.env` file to GitHub
- Use a read-only MongoDB user for the source database when possible
- Change any credential immediately if it is exposed

---

## 📜 License

This project is available under the [MIT License](LICENSE).

---

Made for simple and reliable Telegram file transfers.
