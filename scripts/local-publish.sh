#!/usr/bin/env bash
set -euo pipefail

REGISTRY="http://localhost:4873"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
STORAGE="$PROJECT_ROOT/.verdaccio/storage"
VERDACCIO_CONFIG="$SCRIPT_DIR/verdaccio.yaml"

cd "$PROJECT_ROOT"

# --- Detect if this is a prerelease version ---
VERSION=$(node -e "console.log(require('./package.json').version)")
TAG_ARG=""

if [[ "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+-(.+)$ ]]; then
  # Extract tag from version (e.g., "0.1.8-rc.1" → "rc")
  PRERELEASE="${BASH_REMATCH[1]}"
  TAG_NAME="${PRERELEASE%.*}"  # Remove .X suffix (rc.1 → rc)
  TAG_ARG="--tag $TAG_NAME"
  echo "Detected prerelease version $VERSION → using tag: $TAG_NAME"
else
  echo "Detected stable version $VERSION → publishing as latest"
fi

ensure_verdaccio() {
  if curl -sf "$REGISTRY" > /dev/null 2>&1; then
    echo "Verdaccio already running at $REGISTRY"
    return 0
  fi

  echo "Starting Verdaccio..."
  npx verdaccio --config "$VERDACCIO_CONFIG" --listen 4873 &
  VERDACCIO_PID=$!

  for i in $(seq 1 30); do
    if curl -sf "$REGISTRY" > /dev/null 2>&1; then
      echo "Verdaccio started (PID: $VERDACCIO_PID)"
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

# --- Ensure Verdaccio is running ---
echo ""
ensure_verdaccio

# --- Clean previous @flink-reactor packages from storage ---
echo ""
echo "Removing previous @flink-reactor packages from Verdaccio..."
rm -rf "$STORAGE/@flink-reactor" || true

# --- Publish workspace packages first ---
echo ""
echo "Publishing workspace packages..."
pnpm --filter './packages/*' exec npm publish --registry "$REGISTRY" --provenance=false $TAG_ARG 2>&1

# --- Publish root package ---
echo ""
echo "Publishing @flink-reactor/dsl..."
npm publish --registry "$REGISTRY" --provenance=false $TAG_ARG 2>&1

echo ""
echo "✓ Published to Verdaccio!"
echo ""
echo "Install packages with:"
echo "  npm install @flink-reactor/dsl --registry $REGISTRY"
echo "  npm install @flink-reactor/create-fr-app --registry $REGISTRY"
echo "  npm install @flink-reactor/ts-plugin --registry $REGISTRY"
echo ""
echo "Verdaccio is still running. Stop it with:"
echo "  pkill -f verdaccio"
