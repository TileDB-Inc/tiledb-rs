#!/bin/bash

ldd target/debug/examples/quickstart_dense-* | grep tiledb
if [ "$?" -eq "0" ]; then
    echo "Detected dynamic linkage to libtiledb.so"
    exit 1
fi
