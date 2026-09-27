"""site/og-card.png, the link preview, drawn from the same data as the site.

    make og-card

LinkedIn, Slack and iMessage show this image when the site's link is shared,
and for most people who see the project that preview is all they see. So it
carries the three numbers the site leads with, read from site/data/ rather
than typed, so it cannot drift from the page it advertises.

A PNG, not a JPEG: lossless at this size, where a JPEG needs chroma
subsampling, and subsampled chroma smears text on a flat dark card.

Rendered by headless Chromium from an HTML template, not drawn with an imaging
library: the card then uses the site's own fonts and palette, and the project
gains no dependency (Pillow is not in the venv; Chromium is on the machine).
"""

from __future__ import annotations

import json
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from string import Template

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "site" / "data"
OUT = ROOT / "site" / "og-card.png"
W, H = 1200, 627

CARD = Template("""<!DOCTYPE html><html><head><meta charset="utf-8"><style>
  * { margin:0; box-sizing:border-box; }
  html, body { width:${W}px; height:${H}px; overflow:hidden; background:#10141a; }
  body { color:#dbe2ec; font-family:system-ui, sans-serif; padding:64px 72px;
         display:flex; flex-direction:column; justify-content:space-between; }
  .eyebrow { color:#8b97a8; font-size:24px; letter-spacing:.04em; text-transform:uppercase; }
  h1 { font-size:66px; line-height:1.05; margin-top:14px; }
  .sub { color:#8b97a8; font-size:28px; margin-top:16px; }
  .stats { display:flex; gap:28px; }
  .stat { flex:1; background:#171d26; border:1px solid #2a3340; border-radius:14px; padding:22px 26px; }
  .stat b { display:block; font-size:46px; font-weight:600; }
  .stat span { color:#8b97a8; font-size:22px; }
  .hot { color:#ff9f43; }
</style></head><body>
  <div>
    <div class="eyebrow">transit-stream &middot; ${first} to ${last}</div>
    <h1>King County Metro, Live</h1>
    <div class="sub">A week of Metro's live bus feeds through Redpanda, Flink and PostGIS</div>
  </div>
  <div class="stats">
    <div class="stat"><b>${positions}</b><span>vehicle positions</span></div>
    <div class="stat"><b class="hot">${pm_peak}</b><span>of alerts in the PM peak</span></div>
    <div class="stat"><b>${replayed}</b><span>of the live rows reproduced by replay</span></div>
  </div>
</body></html>""")


def fields() -> dict[str, str]:
    kpis = json.loads((DATA / "kpis.json").read_text())["data"]
    replay = json.loads((DATA / "replay.json").read_text())["fidelity"]["enriched"]
    replayed = replay["matched"] / replay["live_rows"]
    return {
        "W": str(W), "H": str(H),
        "first": kpis["first"], "last": kpis["last"],
        # Millions, one decimal: the preview renders about 500 px wide, where a
        # seven-digit number is unreadable.
        "positions": f"{kpis['positions'] / 1e6:.1f}M",
        "pm_peak": f"{kpis['pm_peak_share']:.0%}",
        "replayed": "100%" if replay["matched"] == replay["live_rows"] else f"{replayed:.2%}",
    }


def main() -> int:
    """Render the card and write it to site/og-card.png."""
    chromium = shutil.which("chromium") or shutil.which("chromium-browser") or shutil.which("google-chrome")
    if not chromium:
        print("no Chromium on PATH; the card is rendered by a headless browser", file=sys.stderr)
        return 1
    with tempfile.TemporaryDirectory() as tmp:
        page = Path(tmp) / "card.html"
        page.write_text(CARD.substitute(fields()))
        subprocess.run([chromium, "--headless", "--disable-gpu", "--hide-scrollbars",
                        "--force-device-scale-factor=1", f"--window-size={W},{H}",
                        f"--screenshot={OUT}", page.as_uri()],
                       check=True, capture_output=True, timeout=60)
    print(f"  {OUT.relative_to(ROOT)}  {OUT.stat().st_size:,} bytes")
    return 0


if __name__ == "__main__":
    sys.exit(main())
