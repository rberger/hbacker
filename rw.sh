#!/usr/bin/env bash

for i in {1..4}; do bundle exec bin/hbacker_worker  > /tmp/worker_${i}.log 2>&1 &  done
