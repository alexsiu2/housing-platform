#!/usr/bin/env sh
# Download the jars in jars.txt from Maven Central into $1 and verify SHA-1.
# Used by both the Spark image and the Airflow image (the Spark driver runs in
# Airflow and needs the same Delta + S3A classes as the executors).
set -eu
dest="$1"
list="$(dirname "$0")/jars.txt"
mkdir -p "$dest"
grep -vE '^\s*(#|$)' "$list" | while read -r path sha1; do
  file="$dest/$(basename "$path")"
  curl -fsSL --retry 3 -o "$file" "https://repo1.maven.org/maven2/$path"
  echo "$sha1  $file" | sha1sum -c -
done
