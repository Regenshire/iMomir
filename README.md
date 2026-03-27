# iMomir

iMomir is a mobile-first web application for playing a **paper version of the Magic: The Gathering Momir Basic format**.

It generates random cards by mana value and allows instant printing directly from a phone using AirPrint-compatible label printers.

---

## 🎯 Core Concept

Enter a mana value → iMomir selects a **random valid creature card** → print it instantly as a physical card.

Designed for fast, real-time gameplay at a table.

---

## 🚀 Key Features

- 🎲 Random card generation by mana value
- 🎮 Multiple game modes (Momir Basic, Legends, Planeswalker, etc.)
- 🔍 Advanced filtering (type, rarity, sets, Arena, etc.)
- 🖼 Automatic card image caching (Scryfall)
- 🗃 Local SQLite database (MTGJSON)
- 📱 Mobile-first UI (optimized for iPhone Safari)
- 🖨 Direct print support:
  - Standard (2.5" × 3.5")
  - Brother DK-1234 labels (60mm × 86mm)

---

## 🧱 Technology Stack

- Backend: Flask
- Database: SQLite  
- Frontend: HTML, CSS, Vanilla JS  
- Data Sources:
  - MTGJSON (card metadata)
  - Scryfall (card images)

---

## ⚙️ Installation

### 1. Install Python

Download:
https://www.python.org/downloads/

---

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

---

## 🌐 Running as a Network Server (REQUIRED)

⚠️ **This app is NOT meant to be used locally only.**  
You must run it on a computer/server so your mobile device can access it.  You can also run it on a laptop for use on the laptop. This is a web application that you host.

---

### Step 1 — Start the Server

```bash
python app.py
```

The app runs on:

```
http://0.0.0.0:5000
```

---

### Step 2 — Determine Your Local IP

#### Windows
```bash
ipconfig
```

#### Mac / Linux
```bash
ifconfig
```
or
```bash
ip addr
```

Example:
```
192.168.1.50
```

---

### Step 3 — Open on Your Phone

On the same WiFi network:

```
http://192.168.1.50:5000
```

---

### Step 4 — Use Built-in QR Code (Recommended)

Go to:

```
Config → QR Code Scan
```

Scan the QR code to open instantly.

---

## 🧠 How Networking Works (Important)

iMomir automatically determines your LAN-accessible address.

If you see `127.0.0.1` in your QR code:

👉 Your system failed to resolve LAN IP  
👉 Fix by:
- Connecting to WiFi
- Disabling VPN
- Ensuring network allows local routing

---

## 🛠 Initial Setup (Required)

After first launch:

1. Go to **Config → Card Database**
2. Click:
   - ✅ **Refresh Card Database**
   - ✅ **Download Card Images**

---

## 🖨 Printing

### Recommended Hardware

- Printer: Brother QL-820NWBC
- Labels: DK-1234 (60mm × 86mm)

---

### Print Flow

1. Generate card
2. Tap **Print**
3. iPhone Safari → Share → Print (AirPrint)

---

## 🎮 Game Modes

- Momir Basic
- Planeswalker
- Legends
- Aggro / Battleship
- Odd / Even / Prime
- Planechase / Archenemy

---

## 🔄 Refresh Behavior

### Refresh Card Database
- Downloads latest MTGJSON
- Rebuilds database

### Download Card Images
- Matches cards and caches locally

---

## 🧪 Optional: Production Mode

```bash
pip install waitress
```

```python
from waitress import serve
serve(app, host="0.0.0.0", port=5000)
```

---

## 🔐 Firewall Notes

- Allow Python through firewall
- Ensure port 5000 is open
- Devices must be on same network

---

## ⚠️ Known Limitations

- iPhone Safari blocks auto-print, its recommended that you use Firefox or Chrome on apple devices so that you don't have to manually select the print option in the browser.
- Initial card database download takes a little time, be patient
- Requires network access

---

## 📜 License

MIT License

---

## ⚖️ Disclaimer

Not affiliated with Wizards of the Coast.

---

## 🙌 Credits

- MTGJSON — https://mtgjson.com
- Scryfall — https://scryfall.com
