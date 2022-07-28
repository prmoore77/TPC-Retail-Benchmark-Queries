#!/bin/bash
# This script assumes you've installed the Google Cloud CLI and that you have authenticated (with gcloud auth or init)

set -e

TPCH_SCALE_FACTOR=${1:?You MUST provide the TPC-H Scale Factor!}

echo "TPCH_SCALE_FACTOR=${TPCH_SCALE_FACTOR}"

gsutil -m cp -r gs://tpch-dataset/${TPCH_SCALE_FACTOR} ./
