#!/usr/bin/env bash
# Run the prototype's checks on Pawsey: Acacia for the object-storage
# questions, the gdal-r-python-extras image for a real GDAL read. Follows
# raad.virtualize hpc/pawsey/singularity-run.sh. From a Setonix login node:
#
#   bash pawsey/run.sh                 # probe, demo on Acacia, GDAL read
#   bash pawsey/run.sh real <flat>     # only: replay a real flat refs store
#                                            # as a history, local. <flat> is a
#                                            # directory or https URL holding
#                                            # arrays.json and refs_<array>.parquet,
#                                            # e.g. a published <dataset>/flat
#
#   BR_S3_ROOT     default s3://aad-derived/raad-scratch/brstore-test
#                  (each run writes under <root>/<run id>/, a few hundred KB)
#   RCLONE_REMOTE  default pawsey1197: keys and endpoint are read from its
#                  rclone config when AWS_ACCESS_KEY_ID is not already set.
#                  They are exported to the container and never printed.
#   SIF            default $MYSOFTWARE/sif_lib/gdal-r-python-extras_latest.sif
#   BR_WORK        default $MYSCRATCH/brstore-work (else ./brstore-work)
#   RV_PYUSER      default $MYSOFTWARE/rv-pyuser (pip --target for what the
#                  image lacks: obstore, duckdb, numcodecs)
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REMOTE=${RCLONE_REMOTE:-pawsey1197}
if [ -z "${AWS_ACCESS_KEY_ID:-}" ]; then
  cfg=$(rclone config show "$REMOTE")
  get() { printf '%s\n' "$cfg" | awk -F' *= *' -v k="$1" '$1==k{print $2; exit}'; }
  AWS_ACCESS_KEY_ID=$(get access_key_id); AWS_SECRET_ACCESS_KEY=$(get secret_access_key)
  AWS_ENDPOINT_URL=${AWS_ENDPOINT_URL:-$(get endpoint)}
  export AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_ENDPOINT_URL
  [ -n "$AWS_ACCESS_KEY_ID" ] || { echo "run.sh: no access_key_id in rclone remote $REMOTE; export AWS_* yourself" >&2; exit 1; }
fi
export AWS_ENDPOINT_URL=${AWS_ENDPOINT_URL:-https://projects.pawsey.org.au}
case $AWS_ENDPOINT_URL in *://*) ;; *) AWS_ENDPOINT_URL=https://$AWS_ENDPOINT_URL ;; esac
export AWS_REGION=${AWS_REGION:-us-east-1}
RUN=$(date -u +%Y%m%dT%H%M%SZ)
export BR_S3=${BR_S3_ROOT:-s3://aad-derived/raad-scratch/brstore-test}/$RUN
export BR_WORK=${BR_WORK:-${MYSCRATCH:-$PWD}/brstore-work}
export BR_MODE=${1:-all} BR_EXPORT=${2:-}
mkdir -p "$BR_WORK"
[ -d "$BR_WORK/fixtures" ] || git clone -q --depth 1 https://github.com/wxylib/fixtures "$BR_WORK/fixtures"
command -v singularity > /dev/null || module load pawseyenv/2025.08 "${SINGULARITY_MODULE:-singularity/4.1.0-slurm}"
SIF=${SIF:-$MYSOFTWARE/sif_lib/gdal-r-python-extras_latest.sif}
PYSITE=${RV_PYUSER:-$MYSOFTWARE/rv-pyuser}/site
export PYTHONPATH=$PYSITE${PYTHONPATH:+:$PYTHONPATH}
export TMPDIR=$BR_WORK/tmp; mkdir -p "$TMPDIR"
BIND="-B $MYSOFTWARE -B $BR_WORK -B $HERE"
case $BR_EXPORT in ""|http://*|https://*) ;; *) BR_EXPORT=$(cd "$BR_EXPORT" && pwd); BIND="$BIND -B $BR_EXPORT" ;; esac
# shellcheck disable=SC2086
exec singularity exec $BIND "$SIF" bash -c '
  set -uo pipefail
  for p in obstore duckdb numcodecs; do
    python -c "import $p" 2>/dev/null || python -m pip install -q --target "'"$PYSITE"'" --no-deps $p
  done
  cd "'"$HERE"'"; F=$BR_WORK/fixtures/mdimrefs; fail=0
  step() { echo; echo "######## $1"; shift; "$@" || { echo "######## FAILED: $1 (exit $?)"; fail=1; }; }
  if [ "$BR_MODE" = real ]; then
    step "a real flat refs store replayed as a history" python real_rows.py "$BR_EXPORT" "$BR_WORK/real"
    [ $fail = 0 ] && echo "PASSED" || echo "FAILED: send the output back"; exit $fail
  fi
  step "1. object storage: conditional PUT, write race, listing ($BR_S3)" python pawsey/probe.py "$BR_S3"
  step "2. the full history with the store on Acacia" env BRSTORE_ROOT="$BR_S3/demo" python demo.py "$F" "$BR_WORK/acacia"
  step "3. the full history with a local store" python demo.py "$F" "$BR_WORK/local"
  step "3b. oracle manifests" python wxy_check.py "$F" "$BR_WORK/local"
  step "4. GDAL reads the committed versions" python pawsey/gdal_check.py "$F" "$BR_WORK/local"
  echo; echo "store written to $BR_S3 (remove with: rclone purge '"$REMOTE"':${BR_S3#s3://})"
  [ $fail = 0 ] && echo "ALL STEPS PASSED" || echo "SOME STEPS FAILED: send the output back"
  exit $fail'
