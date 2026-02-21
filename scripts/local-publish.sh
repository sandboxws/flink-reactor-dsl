#!/usr/bin/env bash
set -euo pipefail

REGISTRY="http://localhost:4873"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_ROOT"

# --- Start Verdaccio if not running ---
if ! curl -sf "$REGISTRY" > /dev/null 2>&1; then
  echo "Starting Verdaccio..."
  npx verdaccio --config "$SCRIPT_DIR/verdaccio.yaml" --listen 4873 &

  for i in $(seq 1 30); do
    if curl -sf "$REGISTRY" > /dev/null 2>&1; then
      break
    fi
    if [ "$i" -eq 30 ]; then
      echo "Error: Verdaccio failed to start within 30s" >&2
      exit 1
    fi
    sleep 1
  done
  echo "Verdaccio running at $REGISTRY"
else
  echo "Verdaccio already running at $REGISTRY"
fi

# --- Build ---
echo ""
echo "Building flink-reactor..."
pnpm build

echo ""
echo "Building @flink-reactor/dashboard..."
pnpm --filter @flink-reactor/dashboard build:package

# --- Publish ---
echo ""
echo "Publishing flink-reactor..."
pnpm publish --registry "$REGISTRY" --no-git-checks 2>&1 || echo "  (skipped — version may already exist)"

echo "Publishing @flink-reactor/dashboard..."
pnpm --filter @flink-reactor/dashboard publish --registry "$REGISTRY" --no-git-checks 2>&1 || echo "  (skipped — version may already exist)"

echo ""
echo "Done! Install packages with:"
echo "  npm install flink-reactor --registry $REGISTRY"
echo "  npm install @flink-reactor/dashboard --registry $REGISTRY"
