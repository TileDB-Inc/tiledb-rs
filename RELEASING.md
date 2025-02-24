# How to Create a Release

This document describes the process for creating a release of tiledb-rs. There
are two main workflows: major/minor releases and patch releases. The two are
basically identical other than patch releases skip a few extra maintenance
steps for the major/minor release types.

## Release Types

Given the semantic version `x.y.z`, if `x` or `y` have changed, you're working
on a major/minor release. If only `z` has chagned, then its a patch release
which just means you get to skip a few chore steps.

## Required Tooling

- [Git Cliff](https://git-cliff.org/)
- [GitHub CLI](https://cli.github.com/)

## Preparing a Major or Minor Release

1. Create a new `release-x.y` branch
2. Perform any maintenance actions
3. Run `./scripts/make-release.sh`

### 1. Create a new `release-x.y` Branch

```bash
$ git checkout -b release-0.1 origin/main
```

### 2. Perform any maintenance actions

This section is a work in progress. So far the following steps should be
manually verified:

1. Ensure that the `workspace.metadata.libtiledb.version` key is correct in `Cargo.toml`
2. Something something, check MSVR maybe?

### 3. Tag the Release

The following command will perform four major actions:

1. Use `git cliff` to generate the release CHANGELOG
2. Open your editor to view the change log before creating the tag
3. Create and push the tag to Github
4. Create the GitHub release using the newly created tag

```bash
$ ./scripts/make-release.sh x.y.z
```

## Preparing a Patch Release

Preparing a patch release should just be a matter of running `make-release.sh`
on the release branch.

```bash
$ git checkout release-x.y
$ ./scripts/make-release.sh x.y.z
```
