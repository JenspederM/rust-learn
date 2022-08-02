#!/usr/bin/bash

source $PWD/scripts/utils.sh

NAME=$(get_token name)
VERSION=$(get_token version)

echo "Building image '$NAME:$VERSION'"

docker build -t $NAME:$VERSION .
