#!/usr/bin/env python

import hashlib
import json
import os
import shutil

BASE_URL = "https://github.com/TileDB-Inc/tiledb-rs/releases/download/nightlies/{}"

def load_configs():
    return json.loads(os.environ["RELEASE_MATRIX"])

def write_sha256(fname):
    with open(fname, 'rb') as handle:
        digest = hashlib.file_digest(handle, "sha256")

    sha256 = "{}  {}\n".format(
        digest.hexdigest(),
        os.path.basename(fname)
    ).encode("utf-8")

    with open(fname + ".sha256", "wb") as handle:
        handle.write(sha256)

def copy_prebuilt(tarball):
    for fname in [tarball, tarball + ".sha256"]:
        path = os.path.join("prebuilt", fname)
        dest = os.path.join("release", fname)
        shutil.copyfile(path, dest)

def copy_artifact(tarball):
    path = os.path.join("artifacts", tarball)
    dest = os.path.join("release", tarball)
    shutil.copyfile(path, dest)
    write_sha256(dest)

def write_releases(configs):
    lines = []
    for config in configs:
        platform = config["platform"]
        if platform == "linux-aarch64":
            platform = "linux-arm64"
        platform = platform.upper()
        linkage = config["linkage"].lower()
        url = BASE_URL.format(config["tarball"])
        sha256 = os.path.join("release", config["tarball"] + ".sha256")
        with open(sha256, "r") as handle:
            sha256 = handle.read().split()[0].strip()
        lines.append(",".join([platform, config["version"], linkage, url, sha256]))
    lines.sort()
    lines.append("")
    lines = "\n".join(lines)
    dest = os.path.join("release", "releases.csv")
    with open(dest, "wb") as handle:
        handle.write(lines.encode("utf-8"))
    write_sha256(dest)

def main():
    configs = load_configs()
    for config in configs:
        if config["prebuilt"]:
            copy_prebuilt(config["tarball"])
        else:
            copy_artifact(config["tarball"])
    write_releases(configs)

if __name__ == "__main__":
    main()
