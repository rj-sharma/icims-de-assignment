#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REMOVE_VENV=0

usage() {
  echo "Usage: bash scripts/reset_env.sh [--remove-venv]"
  echo
  echo "Removes generated local artifacts so setup can be re-tested."
  echo "Use --remove-venv to also delete the local virtual environment."
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

if [[ "${1:-}" == "--remove-venv" ]]; then
  REMOVE_VENV=1
elif [[ $# -gt 0 ]]; then
  echo "Unknown option: $1"
  usage
  exit 1
fi

TARGETS=(
  "$ROOT_DIR/icims.duckdb"
  "$ROOT_DIR/dbt/icims_project/icims.duckdb"
  "$ROOT_DIR/dbt/icims_project/target"
  "$ROOT_DIR/dbt/icims_project/dbt_packages"
  "$ROOT_DIR/logs"
  "$ROOT_DIR/dbt/logs"
)

if [[ $REMOVE_VENV -eq 1 ]]; then
  TARGETS+=("$ROOT_DIR/venv")
fi

echo "Cleaning generated artifacts from: $ROOT_DIR"

for target in "${TARGETS[@]}"; do
  if [[ -e "$target" ]]; then
    rm -rf "$target"
    echo "Removed: $target"
  else
    echo "Skip (not found): $target"
  fi
done

echo "Cleanup complete."
