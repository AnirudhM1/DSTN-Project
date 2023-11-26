#!/usr/bin/env python3

import json
import os
import sys


NODE_ID = int(sys.argv[1])
NODE_DIR = f"node{NODE_ID}"
SAVE_DIR = f"{NODE_DIR}/datasets"

meta = []
for file in os.listdir(SAVE_DIR):
    filename = os.fsdecode(file)
    meta.append({"img": filename.split(".")[0], "node": NODE_ID})

with open(f"{NODE_DIR}/meta.json", "w") as meta_file:
    json.dump(meta, meta_file)
