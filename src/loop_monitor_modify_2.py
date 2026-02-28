#!/usr/bin/env python3
import sys
import time
import traceback
from monitor_modify_2 import main

INTERVAL = int(sys.argv[1]) if len(sys.argv) > 1 else 10

print(f"=== NFT Modify Monitor Runner (interval: {INTERVAL}s) ===")

while True:
    try:
        main()
    except Exception:
        traceback.print_exc()
    time.sleep(INTERVAL)