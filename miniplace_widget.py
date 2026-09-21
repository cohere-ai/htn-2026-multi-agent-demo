"""Persistent notebook widget for the MiniPlace dashboard."""

from pathlib import Path

import anywidget
import traitlets


ASSETS = Path(__file__).resolve().parent / "assets"
JAVASCRIPT = (ASSETS / "miniplace.js").read_text()
CSS = (ASSETS / "miniplace.css").read_text()


class MiniPlaceWidget(anywidget.AnyWidget):
    _esm = JAVASCRIPT
    _css = CSS
    scene = traitlets.Dict().tag(sync=True)
    frame = traitlets.Dict().tag(sync=True)
