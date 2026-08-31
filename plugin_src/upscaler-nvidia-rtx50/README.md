# iMomir NVIDIA RTX 50 Series Upscaler

Optional AI image upscaling plugin for iMomir.

## Requirements

- Windows
- NVIDIA RTX 50 Series GPU
- Current NVIDIA graphics driver
- Python 3.12 64-bit
- Internet connection during initial installation

## Installation

Install the plugin directly from iMomir:

Settings → Advanced Image Upscaling → Download & Install Upscaler Plugin

iMomir will automatically:

1. Download the plugin.
2. Create an isolated Python environment.
3. Install the required NVIDIA/PyTorch dependencies.
4. Download AI model files when required.

The plugin is optional. iMomir does not require the plugin or Python for normal use.

## Plugin ID

`upscaler-nvidia-rtx50`

## Current Production Model

Magic Card AI v3

## Supported Features

- AI card image upscaling
- Single-faced cards
- Double-faced cards
- Batch upscaling
- Persistent GPU worker for faster repeated processing