#!/usr/bin/env bash
# Usage: ./catdir.sh /path/to/dir

dir=${1:?Usage: $0 DIRECTORY}

[[ -d $dir ]] || { echo "Not a directory: $dir" >&2; exit 1; }

# Loop over regular files only, print each with a small header for clarity
for f in "$dir"/*; do
    [[ -f $f ]] || continue          # skip dirs, symlinks, etc.
    printf '\n===== %s =====\n' "$(basename "$f")"
    cat "$f"
done
