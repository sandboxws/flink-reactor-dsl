#!/usr/bin/env bash
set -euo pipefail

REGISTRY="http://localhost:4873"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
STORAGE="$PROJECT_ROOT/.verdaccio/storage"
VERDACCIO_CONFIG="$SCRIPT_DIR/verdaccio.yaml"

cd "$PROJECT_ROOT"

start_verdaccio() {
  echo "Starting Verdaccio..."
  npx verdaccio --config "$VERDACCIO_CONFIG" --listen 4873 &

  for i in $(seq 1 30); do
    if curl -sf "$REGISTRY" > /dev/null 2>&1; then
      echo "Verdaccio running at $REGISTRY"
      return 0
    fi
    sleep 1
  done
  echo "Error: Verdaccio failed to start within 30s" >&2
  exit 1
}

# --- Build first (while Verdaccio may or may not be running) ---
echo "Building flink-reactor..."
pnpm build

echo ""
echo "Building @flink-reactor/dashboard..."
pnpm --filter @flink-reactor/dashboard build:package

# --- Stop Verdaccio, clear storage, restart ---
echo ""
echo "Clearing previous versions from local registry..."
pkill -f verdaccio 2>/dev/null || true
sleep 1
rm -rf "$STORAGE/flink-reactor"
rm -rf "$STORAGE/@flink-reactor"
start_verdaccio

# --- Publish ---
echo ""
echo "Publishing flink-reactor..."
npm publish --registry "$REGISTRY" --provenance=false 2>&1

echo ""
echo "Publishing @flink-reactor/dashboard..."
cd "$PROJECT_ROOT/apps/dashboard"
npm publish --registry "$REGISTRY" --provenance=false 2>&1

echo ""
echo "Done! Install packages with:"
echo "  npm install flink-reactor --registry $REGISTRY"
echo "  npm install @flink-reactor/dashboard --registry $REGISTRY"
