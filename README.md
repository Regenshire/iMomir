# iMomir

iMomir is a web app for playing paper versions of Magic: The Gathering random card formats.

You can use it to generate cards by mana value, open random Chaos Draft packs, print cards and packs, and export Chaos Draft deck lists.

It is designed to work well on a computer, laptop, phone, or tablet when it is hosted on a device on your local network.

## What iMomir Can Do

- Generate random cards by mana value
- Support multiple game modes
- Filter by set
- Open random Chaos Draft packs
- Generate printable PDFs
- Cache card images locally
- Export Chaos Draft lists for Archidekt or Moxfield
- Show a QR code so a phone can open the app quickly

## Game Modes

iMomir currently includes:

- Custom
- Momir Basic
- Momir Select
- Momir Planeswalker
- Momir Legends
- Momir Battleship
- Momir Aggro
- Momir Odds
- Momir Evens
- Momir Prime
- Tower of Power
- Chaos Draft
- PRE-PRINT Chaos Draft
- Planechase
- Archenemy

## What You Need

Install the required Python packages:

```bash
pip install -r requirements.txt
```

## How to Start the App

Run:

```bash
python app.py
```

The app starts on port 5000.

## How to Open the App

### If you are using the same computer

Open:

```text
http://127.0.0.1:5000
```

### If you are using a phone or another device on your network

Find the IP address of the computer or device running the iMomir application, then open:

```text
http://YOUR-IP-ADDRESS:5000
```

Example:

```text
http://192.168.1.50:5000
```

## First Time Setup

When you first open iMomir, you need to load the database.

### Step 1
Open the `Modes` tab.

### Step 2
Open the `Card Database` section.

### Step 3
Click `Download Card Database`.

This loads the card data and set data needed for the app.

### Step 4
If you want, you can also use the image download tools in the same section to pre-download card images.

If you do not do that, iMomir can still download images later as needed.

## Main Tabs

## Draft

This is the main play screen.

What appears here depends on the selected mode.

Examples:

- Momir modes let you enter a mana value and generate a card
- Tower of Power lets you draw random cards from the enabled set pool
- Chaos Draft lets you spin for a random pack, then open or export it
- PRE-PRINT Chaos Draft lets you generate printable packs ahead of time

## Sets

Use this tab to control what sets are available.

You can:

- use all sets
- manually choose sets
- filter by year
- filter by set type
- control which booster types are allowed in Chaos Draft

## Modes

Use this tab to control the app settings.

You can:

- choose the active game mode
- change print settings
- refresh the card database
- download images
- control repeat behavior
- open the QR code screen

## Printing

iMomir supports normal browser printing and PDF printing.

### Browser Printing

Used when PDF printing is turned off.

### PDF Printing

Used when PDF printing is turned on.

Print settings include:

- print template
- print color mode
- PDF width and height
- crop border
- front/back label
- open print in new tab
- Chaos Draft title image options

## Chaos Draft

Chaos Draft lets you spin for a random booster pack from the sets and booster types you have enabled.

### Chaos Draft flow

### Step 1
Click the spin button.

### Step 2
iMomir selects a random eligible booster pack.

### Step 3
The pack contents are generated immediately.

### Step 4
You can then:

- open the pack as a printable PDF
- export the pack list for Archidekt (configured in Config)
- export the pack list for Moxfield (configured in Config)

## PRE-PRINT Chaos Draft

PRE-PRINT Chaos Draft lets you generate multiple Chaos Draft packs at once.

You choose:

- number of players
- number of packs per player

iMomir then generates a combined printable PDF that can be printed.

## QR Code

The QR code screen makes it easy to open iMomir on a phone or tablet.

Open the QR code from the `Modes` tab and scan it with your phone.

## Data Sources

iMomir uses:

- MTGJSON for card and booster data
- Scryfall for image matching and card image data

## Notes

- iMomir is meant to run as a local web app
- it works on the same computer or across your local network
- first-time database setup can take a little time
- image downloads can also take some time
- clipboard copy may not work on every browser or non-secure local network page, but Save export will still work

## License

MIT License

## Disclaimer

iMomir is an unofficial fan project and is not affiliated with Wizards of the Coast.

## Credits
The following online resources are used extensively by the application:

- MTGJSON
- Scryfall