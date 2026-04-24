#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"

cd "$ROOT_DIR"
if [ -f docker-compose.redis.yml ]; then
  docker compose -f docker-compose.redis.yml down --remove-orphans || true
fi
if [ -f docker-compose.redis.cluster.yml ]; then
  docker compose -f docker-compose.redis.cluster.yml down --remove-orphans || true
fi