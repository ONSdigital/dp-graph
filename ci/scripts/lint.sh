#!/bin/bash -eux

 cwd=$(pwd)

 pushd $cwd/dp-graph
   make lint
 popd
