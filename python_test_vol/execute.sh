#!/bin/sh
pip install -r requirements.txt
python transfer.py
python read.py
python update.py
python delete.py