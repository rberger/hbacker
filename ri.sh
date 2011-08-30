#!/usr/bin/env bash

bundle exec bin/hbacker import \
  --hadoop_home=/home/hadoop \
  --hbase_home=/home/hadoop/hbase \
  --hbase_version=0.90.1 \
  -H localhost -P 8080 -D s3n://runa-production-hbase-backups/ \
  -t furtive_production_frylock_merchant_consumer_summary_e52c36e1-7851-08c1-bbdf-3fc2a84a1cb6 \
  --import-hbase-host=hbase-master0-production.runa.com \
  -d \
  -l emr-master0.runa.com \
  --source-root=s3n://runa-production-hbase-backups/ \
  $1 $2 $3 $4 $5 $6 $7 $8 $9
