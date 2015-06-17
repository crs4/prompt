#!/usr/bin/env bash

export PYMINE_HOME=/Users/paolo/Documents/CRS4/src/pymine/PyMine
PYDOOP_CMD="pydoop submit"
JOB_CONF="${PYMINE_HOME}/conf/trace_mr.conf"
PYDOOP_ARGS="--job-conf ${JOB_CONF} --upload-file-to-cache"
PYMINE_ARGS="trace_mr"
INPUT=$1
OUTPUT=$2

$PYDOOP_CMD $PYDOOP_ARGS ${PYMINE_HOME}/pymine/mining/process/eventlog/trace_mr.py $PYMINE_ARGS $INPUT $OUTPUT