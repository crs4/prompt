#!/usr/bin/env bash
ROOT_DIR=../
export PYMINE_HOME=$ROOT_DIR
PYDOOP_CMD="pydoop submit"
PYDOOP_ARGS="--upload-file-to-cache"
PYMINE_ARGS="arc_dep_mr"
INPUT=$1
OUTPUT=$2

$PYDOOP_CMD $PYDOOP_ARGS ${PYMINE_HOME}/pymine/mining/process/discovery/heuristics/arc_dep_mr.py $PYMINE_ARGS $INPUT $OUTPUT