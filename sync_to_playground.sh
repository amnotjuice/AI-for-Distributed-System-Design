#!/bin/bash
# Sync src/spring2026/ from remote spring2026 branch
# to BauplanLabs/wiscurds-playground spring2026-v2/
#
# Keeps persistent clones under ~/.sync_spring2026/ to avoid re-cloning each time.

set -e

SOURCE_REPO="https://github.com/amnotjuice/AI-for-Distributed-System-Design.git"
SOURCE_BRANCH="spring2026"
SOURCE_PATH="src/spring2026"

TARGET_REPO="https://github.com/BauplanLabs/wiscurds-playground.git"
TARGET_DIR="spring2026-v2"

CACHE_DIR="$HOME/.sync_spring2026"
SOURCE_DIR="$CACHE_DIR/source"
TARGET_CLONE="$CACHE_DIR/target"

mkdir -p "$CACHE_DIR"

# --- Source ---
if [ -d "$SOURCE_DIR/.git" ]; then
  echo "==> Updating source repo..."
  cd "$SOURCE_DIR"
  git fetch origin "$SOURCE_BRANCH"
  git checkout "$SOURCE_BRANCH"
  git reset --hard "origin/$SOURCE_BRANCH"
else
  echo "==> Cloning source repo (first time)..."
  git clone --branch "$SOURCE_BRANCH" "$SOURCE_REPO" "$SOURCE_DIR"
fi

# --- Target ---
if [ -d "$TARGET_CLONE/.git" ]; then
  echo "==> Updating target repo..."
  cd "$TARGET_CLONE"
  git pull
else
  echo "==> Cloning target repo (first time)..."
  git clone "$TARGET_REPO" "$TARGET_CLONE"
fi

# --- Copy ---
echo "==> Syncing files to target..."
TARGET_SUBDIR="$TARGET_CLONE/$TARGET_DIR"
rm -rf "$TARGET_SUBDIR"
mkdir -p "$TARGET_SUBDIR"

# Use git ls-files to enumerate exactly what's tracked — avoids .gitignore interference
cd "$SOURCE_DIR"
git ls-files "$SOURCE_PATH" | while read -r file; do
  rel="${file#$SOURCE_PATH/}"
  src="$SOURCE_DIR/$file"
  dst="$TARGET_SUBDIR/$rel"
  mkdir -p "$(dirname "$dst")"
  cp "$src" "$dst"
done

# --- Commit & Push ---
echo "==> Committing and pushing..."
cd "$TARGET_CLONE"
git add -f "$TARGET_DIR"
if git diff --cached --quiet; then
  echo "No changes to sync."
else
  TIMESTAMP=$(date '+%Y-%m-%d %H:%M')
  git commit -m "sync spring2026 results [$TIMESTAMP]"
  git push
  echo "Done! spring2026-v2 updated in wiscurds-playground."
fi
