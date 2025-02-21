#!/bin/bash

NOTES=$(git cliff --unreleased --tag $1)
git tag -a --cleanup verbatim -e -m "$NOTES" $1
git push origin $1
gh release create $1 --verify-tag --notes "$NOTES"
