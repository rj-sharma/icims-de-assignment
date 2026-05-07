#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REMOVE_VENV=0
DRY_RUN=0

usage() {
  echo "Usage: bash scripts/clean_local_artifacts.sh [--remove-venv] [--dry-run]"
  echo
  echo "Removes generated local artifacts so the pipeline can be tested from a clean state."
  echo
  echo "Options:"
  echo "  --remove-venv   Also remove the local Python virtual environment at ./venv"
  echo "  --dry-run       Print what would be removed without deleting anything"
  echo "  -h, --help      Show this help message"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --remove-venv)
      REMOVE_VENV=1
      ;;
    --dry-run)
      DRY_RUN=1
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1"
      usage
      exit 1
      ;;
  esac
  shift
done

# Generated files/directories only. Source code, README files, and input data are preserved.
TARGETS=(
  "$ROOT_DIR/icims.duckdb"
  "$ROOT_DIR/dbt_packages"
  "$ROOT_DIR/dbt/icims_project/icims.duckdb"
  "$ROOT_DIR/dbt/icims_project/assignment_sql/icims.duckdb"
  "$ROOT_DIR/dbt/icims_project/target"
  "$ROOT_DIR/dbt/icims_project/dbt_packages"
  "$ROOT_DIR/logs"
  "$ROOT_DIR/dbt/logs"
  "$ROOT_DIR/dbt/icims_project/logs"
)

if [[ $REMOVE_VENV -eq 1 ]]; then
  TARGETS+=("$ROOT_DIR/venv")
fi

echo "Project root: $ROOT_DIR"

if [[ $DRY_RUN -eq 1 ]]; then
  echo "Dry run enabled. No files will be removed."
else
  echo "Cleaning generated local artifacts."
fi

for target in "${TARGETS[@]}"; do
  # Guard against accidental deletion outside this repository.
  if [[ "$target" != "$ROOT_DIR"* ]]; then
    echo "Refusing to remove path outside project root: $target"
    exit 1
  fi

  if [[ -e "$target" ]]; then
    if [[ $DRY_RUN -eq 1 ]]; then
      echo "Would remove: $target"
    else
      rm -rf "$target"
      echo "Removed: $target"
    fi
  else
    echo "Skip (not found): $target"
  fi
done

if [[ $DRY_RUN -eq 1 ]]; then
  echo "Dry run complete."
else
  echo "Cleanup complete."
fi
