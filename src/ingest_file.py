#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Alias chuyển tiếp sang src/ingest_paysim.py để đảm bảo tính tương thích ngược.
"""
import sys
from ingest_paysim import main

if __name__ == "__main__":
    sys.exit(main())
