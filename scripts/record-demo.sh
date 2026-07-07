#!/usr/bin/env bash
# 录制终端演示动图：asciinema 录制 demo 运行，agg 渲染为 GIF。
# 用法：bash scripts/record-demo.sh  （产物：docs/demo.gif）
set -euo pipefail
cd "$(dirname "$0")/.."

cargo build --release -p ohmcp-bench --bin demo >/dev/null 2>&1

cat > /tmp/ohmcp-demo-driver.sh <<'EOF'
type_cmd() { printf '\033[1;32m$\033[0m '; for ((i=0; i<${#1}; i++)); do printf '%s' "${1:i:1}"; sleep 0.03; done; printf '\n'; sleep 0.4; }
type_cmd "cargo run --release -p ohmcp-bench --bin demo"
./target/release/demo
sleep 2
EOF

rm -f /tmp/ohmcp-demo.cast
asciinema rec -q -c "bash /tmp/ohmcp-demo-driver.sh" /tmp/ohmcp-demo.cast
agg --font-size 16 --theme monokai /tmp/ohmcp-demo.cast docs/demo.gif
ls -la docs/demo.gif
