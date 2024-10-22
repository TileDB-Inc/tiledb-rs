#!/bin/bash

otool -L target/debug/examples/quickstart_dense-* | grep tiledb
if [ "$?" -eq "0" ]; then
    echo "Detected dynamic linkage to libtiledb.dylib"
    exit 1
fi
