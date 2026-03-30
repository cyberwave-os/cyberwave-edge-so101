#!/bin/sh
set -e

# If CYBERWAVE_TWIN_JSON_FILE is set and the file exists,
# read the JSON and export each top-level key-value pair as a CYBERWAVE_* env var.
if [ -n "$CYBERWAVE_TWIN_JSON_FILE" ] && [ -f "$CYBERWAVE_TWIN_JSON_FILE" ]; then
    eval "$(python3 -c "
import json, os, re, shlex

with open(os.environ['CYBERWAVE_TWIN_JSON_FILE']) as f:
    data = json.load(f)

_VALID_ENV_NAME = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*$')
_INVALID_ENV_CHARS = re.compile(r'[^A-Za-z0-9_]')

def sanitize_key(key):
    return _INVALID_ENV_CHARS.sub('_', str(key)).upper()

def export_vars(data, prefix='CYBERWAVE'):
    for key, value in data.items():
        if prefix == 'CYBERWAVE' and key == 'uuid':
            env_name = 'CYBERWAVE_TWIN_UUID'
        else:
            sanitized_key = sanitize_key(key)
            env_name = prefix + '_' + sanitized_key
        if not _VALID_ENV_NAME.match(env_name):
            continue
        # Don't override env vars that were explicitly passed to the container
        if env_name in os.environ:
            continue
        if isinstance(value, dict):
            export_vars(value, env_name)
        elif isinstance(value, list):
            print(f'export {env_name}={shlex.quote(json.dumps(value))}')
        else:
            print(f'export {env_name}={shlex.quote(str(value))}')

export_vars(data)
")"
fi

# Ensure CYBERWAVE_EDGE_CONFIG_DIR is set (edge-core passes this)
if [ -z "$CYBERWAVE_EDGE_CONFIG_DIR" ]; then
    export CYBERWAVE_EDGE_CONFIG_DIR="/app/.cyberwave"
fi

exec python main.py "$@"
