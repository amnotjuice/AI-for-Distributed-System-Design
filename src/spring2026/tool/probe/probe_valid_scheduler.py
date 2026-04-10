import re
from pathlib import Path

def probe_valid_scheduler(
    scheduler_file: str
) -> dict:

    src = Path(scheduler_file).read_text()
    key_match = re.search(r"""@register_scheduler\((?:key=)?['"]([^'"]+)['"]\)""", src)
    if not key_match:
        return {"functional": False, "failure_mode": "no_scheduler_key"}

    return {
            "functional": True, "failure_mode": "success",
        }