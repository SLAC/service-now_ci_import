#!/bin/bash

export ORACLE_HOME=/usr/oracle
export LD_LIBRARY_PATH=/usr/oracle/lib
/opt/service-now_ci_import/bin/run.py pull all  >/opt/service-now_ci_import/var/log/pull.log 2>&1
/opt/service-now_ci_import/bin/run.py merge >/opt/service-now_ci_import/var/log/merge.log 2>&1
/opt/service-now_ci_import/bin/run.py commit >/opt/service-now_ci_import/var/log/commit.log 2>&1
/opt/service-now_ci_import/bin/run.py dump 1>/opt/service-now_ci_import/static/assets.tsv 2>/opt/service-now_ci_import/var/log/dump.log
/opt/service-now_ci_import/bin/run.py upload >/opt/service-now_ci_import/var/log/upload.log 2>&1
