#!/usr/bin/env bash
set -euo pipefail

# Pin the current Python environment for reproducibility.
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

python -V > env.lock
python -m pip --version >> env.lock
python -m pip freeze > requirements.lock

echo "Wrote env.lock and requirements.lock in $repo_root"
