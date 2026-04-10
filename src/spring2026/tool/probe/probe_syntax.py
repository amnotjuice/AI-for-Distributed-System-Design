import re
import ast
from pathlib import Path

def probe_syntax(
    scheduler_file: str
) -> dict:

    src = Path(scheduler_file).read_text()
    try: 
        tree = ast.parse(src)
    except SyntaxError as e:
        return {
                "functional": False, "failure_mode": "syntax_error",
                "reason": "Syntax Error",
                "error_message": f"Failed on syntax verification on line: {str(e)}",
        }
    return {
            "functional": True, "failure_mode": "success",
        }