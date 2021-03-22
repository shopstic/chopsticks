#!/usr/bin/env bash

if ! ls /app >/dev/null 2>&1; then
  ln -s "${PWD}" /app
fi

export SAVEHIST=1000
export HISTFILE=/app/.shell-history

