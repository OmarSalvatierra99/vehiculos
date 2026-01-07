# 10-inventarios

Sistema de control vehicular para auditoria institucional.

## Setup

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Run

```bash
python3 run.py
```

## Deploy

```bash
sudo python3 scripts/autodeploy_all.py --project 10-10-inventarios
```

## Environment Variables

Create `.env` file:
```
FLASK_ENV=development
PORT=5001
CATALOGOS_DIR=/home/gabo/portfolio/projects/10-inventarios/catalogos
```

🔗 **Live:** [Coming soon](https://example.com)

Created: 2026-01-07
