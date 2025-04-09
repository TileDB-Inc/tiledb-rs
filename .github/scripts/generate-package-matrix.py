#!/usr/bin/env python

import copy
import json

import requests

API = "https://api.github.com/"
TILEDB_REF_URL = API + "repos/TileDB-Inc/TileDB/git/ref/{ref}"
NIGHTLY_RELEASE_URL = API + "repos/TileDB-Inc/tiledb-rs/releases/tags/nightlies"

BUILD_SHARED_LIBS = ["ON", "OFF"]
VERSIONS = ["main", "2.27.0"]

# This is taken from the TileDB Release workflow found here:
#
# * https://github.com/TileDB-Inc/TileDB/blob/main/.github/workflows/release.yml
#
# The upstream version of this matrix does not include static builds which
# can be useful for projects like TileDB-Tables that want to distribute
# statically linked binaries.
BASE_MATRIX = [
    {
        "platform": "linux-x86_64",
        "os": "ubuntu-20.04",
        "manylinux": "quay.io/pypa/manylinux_2_28_x86_64:2025.03.15-1",
        "triplet": "x64-linux-release"
    },
    {
        "platform": "linux-x86_64-noavx2",
        "os": "ubuntu-20.04",
        "cmake_args": "-DCOMPILER_SUPPORTS_AVX2=OFF",
        "triplet": "x64-linux-release",
        "manylinux": "quay.io/pypa/manylinux_2_28_x86_64:2025.03.15-1"
    },
    {
        "platform": "linux-aarch64",
        "os": "linux-arm64-ubuntu24",
        "triplet": "arm64-linux-release",
        "manylinux": "quay.io/pypa/manylinux_2_28_aarch64:2025.03.15-1"
    },
    {
        "platform": "macos-x86_64",
        "os": "macos-13",
        "cmake_args": "-DCMAKE_OSX_ARCHITECTURES=x86_64",
        "MACOSX_DEPLOYMENT_TARGET": "11",
        "triplet": "x64-osx-release"
    },
    {
        "platform": "macos-arm64",
        "os": "macos-latest",
        "cmake_args": "-DCMAKE_OSX_ARCHITECTURES=arm64",
        "MACOSX_DEPLOYMENT_TARGET": "11",
        "triplet": "arm64-osx-release"
    }
]


def get_existing_assets():
    resp = requests.get(NIGHTLY_RELEASE_URL)
    resp.raise_for_status()
    existing = set()
    for asset in resp.json()["assets"]:
        existing.add(asset["name"])
    return existing


def get_version_sha(version):
    if version == "main":
        version = "heads/main"
    else:
        version = "tags/{}".format(version)
    url = TILEDB_REF_URL.format(ref=version)
    resp = requests.get(url)
    resp.raise_for_status()
    return resp.json()["object"]["sha"][:7]


def gen_combinations():
    assets = get_existing_assets()
    for version in VERSIONS:
        sha = get_version_sha(version)
        for build_shared in BUILD_SHARED_LIBS:
            for config in BASE_MATRIX:
                config = copy.deepcopy(config)
                yield (version, sha, build_shared, config, assets)


def set_package_version(config):
    # ${version}-${hash}${suffix}
    pkg_version = "{}-{}".format(config["version"], config["sha"])
    if config["build_shared_libs"] == "OFF":
        pkg_version += "-static"
    config["pkg_version"] = pkg_version


def set_tarball_name(config, assets):
    platform = config["platform"]

    # For some reason we rename aarch64 in TileDB packaging
    if platform == "linux-aarch64":
        platform = "linux-arm64"

    tarball = "tiledb-{}-{}".format(platform, config["pkg_version"])
    if "windows" in platform:
        tarball += ".zip"
    else:
        tarball += ".tar.gz"

    config["tarball"] = tarball
    config["prebuilt"] = tarball in assets


def main():
    matrix = []
    for (version, sha, build_shared, config, assets) in gen_combinations():
        if build_shared == "ON" and version != "main":
            # For numbered versions with dynamic linkage, we pull the prebuilt
            # package from TileDB-Inc/TileDB.
            continue

        config["version"] = version
        config["sha"] = sha
        config["build_shared_libs"] = build_shared

        if build_shared == "ON":
            config["linkage"] = "Dynamic"
        else:
            config["linkage"] = "Static"

        set_package_version(config)
        set_tarball_name(config, assets)
        matrix.append(config)

    print(json.dumps(matrix))


if __name__ == "__main__":
    main()
