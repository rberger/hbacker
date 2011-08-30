#!/usr/bin/env bash

bundle exec bin/hbacker export \
  --hadoop_home=/apps/hadoop \
  --hbase_home=/apps/hbase \
  -H hbase-master0-production.runa.com -D s3n://runa-production-hbase-backups/ \
  -t furtive_production_frylock_merchant_consumer_summary_e52c36e1-7851-08c1-bbdf-3fc2a84a1cb6 \
  -d
