#!/bin/bash
set -e

python -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
pip install pysocks

curl -fsSL https://claude.ai/install.sh | bash

