#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"

cd "$ROOT_DIR"
docker compose -f docker-compose.redis.yml up -d
docker compose -f docker-compose.redis.yml ps