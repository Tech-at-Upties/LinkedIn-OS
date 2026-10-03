"""Explicit local declaration after a diagnosed browser startup defect."""
import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / '.local/nos-integration/M1/src'))

from xingestion.linkedin.live import declare_startup_repair_successor


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--previous-manifest', required=True, type=Path)
    parser.add_argument('--next-manifest', required=True, type=Path)
    parser.add_argument('--previous-journal', required=True, type=Path)
    parser.add_argument('--witness', required=True, type=Path)
    result = declare_startup_repair_successor(**vars(parser.parse_args()))
    print(json.dumps(result, indent=2))


if __name__ == '__main__': main()
