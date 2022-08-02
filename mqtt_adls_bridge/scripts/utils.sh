#!/usr/bin/bash

get_token() {
  token=$1
  echo "$(cat $PWD/Cargo.toml | grep ^$token | cut -d '"' -f 2)"
}