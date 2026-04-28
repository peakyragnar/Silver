"""Load the src-layout Silver package from a source checkout."""

from pathlib import Path

_SRC_INIT = Path(__file__).resolve().parent.parent / "src" / "silver" / "__init__.py"

__file__ = str(_SRC_INIT)
__path__ = [str(_SRC_INIT.parent)]

exec(compile(_SRC_INIT.read_text(encoding="utf-8"), __file__, "exec"))
