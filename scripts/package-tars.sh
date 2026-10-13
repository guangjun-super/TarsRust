#!/usr/bin/env bash
set -euo pipefail

# Match TarsGo's release layout: ServerName/ServerName inside ServerName.tgz.
if [[ $# -lt 2 || $# -gt 3 ]]; then
    echo "Usage: $0 BINARY SERVER_NAME [OUTPUT_DIRECTORY]" >&2
    exit 2
fi
binary=$1
server_name=$2
output_dir=${3:-target/tars}
case "$server_name" in
    ''|*[!a-zA-Z0-9_-]*) echo "Invalid server name: $server_name" >&2; exit 2 ;;
esac
if [[ ! -f "$binary" || ! -x "$binary" ]]; then
    echo "Binary must be an executable file: $binary" >&2
    exit 2
fi

mkdir -p "$output_dir"
staging_dir=$(mktemp -d "${TMPDIR:-/tmp}/tars-package.XXXXXX")
trap 'rm -rf "$staging_dir"' EXIT
mkdir "$staging_dir/$server_name"
cp "$binary" "$staging_dir/$server_name/$server_name"
chmod 755 "$staging_dir/$server_name/$server_name"
COPYFILE_DISABLE=1 tar -czf "$output_dir/$server_name.tgz" -C "$staging_dir" "$server_name"
echo "$output_dir/$server_name.tgz"
