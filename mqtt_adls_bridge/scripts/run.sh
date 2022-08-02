#!/usr/bin/bash

source $PWD/scripts/utils.sh

NAME=$(get_token name)
VERSION=$(get_token version)

docker run -it --rm $NAME:$VERSION