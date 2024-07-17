#!/usr/bin/env bash

FOUND="false"
for arg in "$@"
do
  if [ "$arg" == "link_info" ]; then
    FOUND="true"
  fi
done

if [ "$FOUND" == "true" ]; then
  echo "$@" > link_info.txt
fi

exec $@
