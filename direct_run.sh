#!/bin/sh

rm -rf venv

python3 -m venv venv

. ./venv/bin/activate

# Upgrade pip and install the package
python -m pip install --upgrade pip
python -m pip install -e .

# Run the main Python script with arguments
python main.py \
  --setup_file="./setup.py" \
  --runner="DirectRunner" \
  --save_main_session
