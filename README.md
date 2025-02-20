
# Automated FX Market Scanner

## Overview

Identify time windows where FX pairs are most probable to realize their daily low or high based on 
historical analysis.

![Python Version](https://img.shields.io/badge/Python-3.12%2B-green)
<!-- ![License](https://img.shields.io/badge/License-MIT-yellow) -->

## 📦 Prerequisites

- Python 3.12+
- pip (Python Package Manager)
- Virtual Environment (recommended)

## 🔧 Installation

1.Clone the repository in a desired folder (or alternatively download from the same URL):

```bash
git clone https://github.com/rvanhezel/optimal_fx_windows.git
cd optimal_fx_windows
```

2.Create a virtual environment:

```bash
python -m venv venv
source venv/bin/activate  # On Windows, use `venv\Scripts\activate`
```

3.Install dependencies:

```bash
pip install -r requirements.txt
```

4.Set up environment variables for MT5 API:

```bash
# Create a .env file in the project root directory with:
MT5_LOGIN = your_username
MT5_PASSWORD = your_password
MT5_SERVER = server_name
```

## 🎬 Running the Application

```bash
# Run the app
python main.py
```
