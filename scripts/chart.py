#!/usr/bin/env python3
"""从 bench-results.json 生成吞吐对比 SVG 图（docs/benchmark-chart.svg）。

用法：cargo run --release -p ohmcp-bench -- --json bench-results.json
      python3 scripts/chart.py
"""
import json
import sys

SRC = sys.argv[1] if len(sys.argv) > 1 else "bench-results.json"
OUT = "docs/benchmark-chart.svg"

ORDER = [
    "latency-echo",
    "bulk-kb-search",
    "bulk-doc-64k",
    "repeat-cached",
    "pipeline-64",
    "concurrent-16",
]
COLORS = {"baseline": "#9aa5b1", "ohmcp": "#2e7de9", "ohmcp-shm": "#12a594"}
LABELS = {"baseline": "JSON-RPC 基线", "ohmcp": "ohmcp（认证+加密）", "ohmcp-shm": "ohmcp + 共享内存通道"}

data = json.load(open(SRC))
rows = {}
for m in data:
    if m["stack"] in COLORS:
        rows.setdefault(m["scenario"], {})[m["stack"]] = m["ops_per_sec"]

W, H, PAD_L, PAD_B, PAD_T = 860, 380, 70, 60, 46
plot_h = H - PAD_B - PAD_T
maxv = max(v for sc in rows.values() for v in sc.values()) * 1.08
group_w = (W - PAD_L - 20) / len(ORDER)

svg = [
    f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" font-family="sans-serif">',
    f'<rect width="{W}" height="{H}" fill="white"/>',
    f'<text x="{W/2}" y="24" text-anchor="middle" font-size="16" font-weight="bold">六场景吞吐对比（ops/s，每场景 3 次取中位数）</text>',
]
for frac in (0.25, 0.5, 0.75, 1.0):
    y = PAD_T + plot_h * (1 - frac)
    svg.append(f'<line x1="{PAD_L}" y1="{y:.0f}" x2="{W-20}" y2="{y:.0f}" stroke="#e5e7eb"/>')
    svg.append(f'<text x="{PAD_L-6}" y="{y+4:.0f}" text-anchor="end" font-size="10" fill="#6b7280">{maxv*frac/1000:.0f}k</text>')

for gi, sc in enumerate(ORDER):
    stacks = [s for s in ("baseline", "ohmcp", "ohmcp-shm") if s in rows.get(sc, {})]
    bw = min(34, (group_w - 24) / max(len(stacks), 1))
    x0 = PAD_L + gi * group_w + (group_w - bw * len(stacks)) / 2
    for si, st in enumerate(stacks):
        v = rows[sc][st]
        bh = plot_h * v / maxv
        x = x0 + si * bw
        y = PAD_T + plot_h - bh
        svg.append(f'<rect x="{x:.1f}" y="{y:.1f}" width="{bw-4:.1f}" height="{bh:.1f}" fill="{COLORS[st]}" rx="2"/>')
        svg.append(f'<text x="{x+(bw-4)/2:.1f}" y="{y-4:.1f}" text-anchor="middle" font-size="9" fill="#374151">{v/1000:.0f}k</text>')
    svg.append(f'<text x="{PAD_L+gi*group_w+group_w/2:.1f}" y="{H-PAD_B+16}" text-anchor="middle" font-size="11">{sc}</text>')

lx = PAD_L
for st in ("baseline", "ohmcp", "ohmcp-shm"):
    svg.append(f'<rect x="{lx}" y="{H-26}" width="12" height="12" fill="{COLORS[st]}" rx="2"/>')
    svg.append(f'<text x="{lx+16}" y="{H-16}" font-size="11">{LABELS[st]}</text>')
    lx += 230
svg.append("</svg>")
open(OUT, "w").write("\n".join(svg))
print(f"wrote {OUT}")
