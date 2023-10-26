#!/bin/bash

echo "Starting entrypoint..."

echo "Getting requirements..."

pip install --upgrade pip
pip install -r /app/requirements.txt

cd /app

python app.py