#!/usr/bin/env python3
"""
Backward-compatible entry point for titiler-cmr compatibilty package.

Usage:
    python run_test.py [--collection CONCEPT_ID] [--granule-id GRANULE_ID] [--page-size N]
"""

from titiler_cmr_compatibility.cli import main

if __name__ == "__main__":
    main()
