import os
from pathlib import Path
import sys

SOURCE_ROOT = Path(os.environ.get(
    "NOS_SOURCE_ROOT", str(Path(__file__).resolve().parents[2].parent / "NOS-V1")
)).resolve()
for module in ("M1", "M2"):
    sys.path.insert(0, str(SOURCE_ROOT / module / "src"))

# Source is inspected read-only. Keep bytecode out of the sibling checkout.
sys.dont_write_bytecode = True
