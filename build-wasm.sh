#!/usr/bin/env bash
set -euo pipefail
wasm-pack build ui/ --out-dir ../web_viewer/pkg --target web
