#!/usr/bin/env bash
# Creates a short-lived test-only DER certificate for an isolated two-node lab.
# Never use this shared private key in production.
set -euo pipefail
umask 077

usage() {
  cat <<'USAGE'
Usage: create-lab-tls.sh --output DIR

Creates cert.der and key.der. Copy these files only over the lab's trusted
administrative channel; never include them in a GitHub evidence artifact.
USAGE
}

output=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --output) output="${2:?missing value for --output}"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) printf 'unknown argument: %s\n' "$1" >&2; usage >&2; exit 2 ;;
  esac
done
[[ -n "$output" ]] || { printf '%s\n' '--output is required' >&2; exit 2; }
command -v openssl >/dev/null || { printf '%s\n' 'required command not found: openssl' >&2; exit 127; }
if [[ -e "$output" ]] && [[ -n "$(find "$output" -mindepth 1 -maxdepth 1 -print -quit 2>/dev/null)" ]]; then
  printf 'refusing to overwrite non-empty TLS directory: %s\n' "$output" >&2
  exit 2
fi
mkdir -p "$output"

openssl req -x509 -newkey rsa:3072 -nodes -days 2 \
  -subj '/CN=alopex.local' \
  -addext 'subjectAltName=DNS:alopex.local' \
  -addext 'basicConstraints=critical,CA:FALSE' \
  -addext 'keyUsage=critical,digitalSignature,keyEncipherment' \
  -addext 'extendedKeyUsage=serverAuth,clientAuth' \
  -keyout "$output/key.pem" -out "$output/cert.pem" >/dev/null 2>&1
openssl x509 -in "$output/cert.pem" -outform DER -out "$output/cert.der"
openssl pkcs8 -topk8 -nocrypt -in "$output/key.pem" -outform DER -out "$output/key.der"
rm -f "$output/cert.pem" "$output/key.pem"
printf '%s\n' 'Created test-only cert.der and key.der. Delete this directory after the lab run.'
