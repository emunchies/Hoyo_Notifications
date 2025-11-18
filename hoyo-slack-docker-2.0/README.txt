🌙 HoYoLab → Slack Genshin Reporter

A multi-account resin tracker, daily notes collector, and character progress analyzer.

A fully automated Python + Docker application that pulls HoYoLab data for one or more Genshin Impact accounts, stores everything in SQLite, and sends clean status reports directly to Slack.
No app-opening, no refreshing HoYoLab — everything updates itself.

⸻

✅ Features

🔄 Multi-Account Automated Daily Notes

Every loop (default: hourly), the bot pulls the latest Daily Notes for each account:
	•	Resin (current, max, time until full, your timezone aware)
	•	Expeditions (finished / total)
	•	Teapot Realm Currency (current, max, timer)
	•	Commission progress & claim status
	•	Weekly boss discounts remaining
	•	Abyss reset timers
	•	UID never exposed in Slack

All entries are saved into your account-specific SQLite database.

⸻

📊 Character Progress Tracking

The bot also stores full character roster snapshots and automatically detects:
	•	New characters acquired
	•	Level-ups
	•	Friendship gains
	•	Constellation changes
	•	Weapon changes
	•	Weapon level or refinement increases
	•	Total roster changes

You receive Slack messages only when something actually changed.

Includes timeline summaries for:
	•	Last 7 days
	•	Last 30 days
	•	Last 90 days
	•	Last 365 days

All computed from SQLite.

⸻

🔔 Smart Resin Alerts

Optional Slack notifications when resin crosses defined thresholds:
	•	120
	•	160 (full)

No spam — alerts only happen once per threshold.

⸻

🧱 SQLite Data Lake

Every account gets a local SQLite file:

/data/
  genshin_<uid>.sqlite3

Containing:
	•	Daily Notes history
	•	Character snapshots
	•	Change detection
	•	Summary run history

Easy to query, export, or visualize.

⸻

🕒 Timezone-Correct Resin ETA

Correct handling of Mihoyo’s recovery timestamps.
Resin full notification includes your preferred timezone (per-account):

Resin full at: 2025-02-18 14:35 PST


⸻

🔧 Docker-Friendly & Fully Automated

Deployment is simple:

docker build -t hoyo-slack .
docker run -d \
  -v ./data:/data \
  --env-file .env \
  --name hoyo-slack \
  hoyo-slack

Runs on a stable loop (default: every 3600s).
Supports stacks (docker compose) as well.

⸻

🧩 Flexible Configuration (Environment Variables)

.env example:

SLACK_WEBHOOK_URL=https://hooks.slack.com/services/XXXX/YYYY/ZZZZ
DATA_DIR=/data
ACCOUNTS_FILE=/data/accounts.json
LOOP_INTERVAL_SECONDS=3600

accounts.json example:

[
  {
    "name": "ExampleUser",
    "uid": "123456789",
    "ltuid_v2": "your_ltuid",
    "ltoken_v2": "your_ltoken",
    "db_name": "genshin_123456789.sqlite3",
    "slack_mention": "<@U01ABCDEF>",
    "tz": "America/New_York"
  }
]


⸻

🚫 Stability First: What This Version Avoids

To guarantee reliable 24/7 operation:
	•	❌ No parametric transformer calls (unstable)
	•	❌ No deprecated check-in API functions
	•	❌ No “realtime resin estimate hacks”
	•	❌ No exposed UID in logs or Slack
	•	❌ No crash-prone endpoints

Focused entirely on notes, characters, and clean diffs.

⸻

🚀 Future Upgrades (Planned)

⭐ Discord Webhook Support (Planned)

Direct Discord notifications using:
	•	Discord Webhook URL
	•	Embedded messages
	•	Role mentions

Message formatting will mirror Slack output.

⭐ Multi-game Support
	•	Honkai: Star Rail
	•	Zenless Zone Zero

⭐ Custom Resin Thresholds

User-defined alert settings per account.

⭐ Web Dashboard

Minimal read-only dashboard showing:
	•	Resin
	•	Character growth over time
	•	Expedition timers
	•	DB viewer

⭐ Push Notifications

Email & Pushover integration optional.

⸻

🎯 Why This Exists

Because checking HoYoLab manually is annoying.
Because resin caps.
Because we like our data automated, clean, and waiting for us in Slack.

This bot handles it all — completely hands-off.

⸻