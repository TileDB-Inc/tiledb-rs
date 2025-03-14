#!/bin/bash

otool -L target/debug/examples/quickstart_dense-* | grep tiledb
if [ "$?" -ne "0" ]; then
    echo "Failed to detect dynamic linkage to libtiledb.dylib"
    exit 1
fi
