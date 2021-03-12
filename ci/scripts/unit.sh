#!/bin/bash -eux

cwd=$(pwd)

pushd $cwd/dp-graph
  make test
popd