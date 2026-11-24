#!/usr/bin/env bash
# The same checks on NCI Gadi, with no container: the conda/analysis3
# environment plus duckdb, which it lacks.
#
#   bash nci/run.sh real <flat url>   # login node (it fetches over https):
#                                     # replay a published flat refs store
#   bash nci/run.sh bottom [args]     # after "real": derive the bottom from
#                                     # /g/data in place, commit it to the store
#   bash nci/run.sh pixels [args]     # login or compute node, no network:
#                                     # read chunks through that store with the
#                                     # paths as stored (/g/data/...), so the
#                                     # source files are read in place, and
#                                     # compare with the files' own blocks
#
#   BR_PROJECT  default $PROJECT: where work and the duckdb install go
#   BR_WORK     default /scratch/$BR_PROJECT/$USER/brstore-work
#   BR_PYDIR    default /g/data/$BR_PROJECT/$USER/brstore-py (pip --target)
#
# In a PBS job, ask for the storage the store names and the work is on, for
# Bluelink: -l storage=gdata/gb6+gdata/xp65+gdata/<project>+scratch/<project>
set -eo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
module use /g/data/xp65/public/modules
module load conda/analysis3
set -u
BR_PROJECT=${BR_PROJECT:-$PROJECT}
export BR_WORK=${BR_WORK:-/scratch/$BR_PROJECT/$USER/brstore-work}
PYDIR=${BR_PYDIR:-/g/data/$BR_PROJECT/$USER/brstore-py}
export PYTHONPATH=$PYDIR${PYTHONPATH:+:$PYTHONPATH}
python -c "import duckdb" 2>/dev/null || {
  echo "nci/run.sh: installing duckdb into $PYDIR (once; needs the network, so do this on a login node)"
  mkdir -p "$PYDIR" && python -m pip install -q --target "$PYDIR" --no-deps duckdb
}
mode=${1:-pixels}; [ $# -gt 0 ] && shift
{ [ "$mode" = pixels ] || [ "$mode" = bottom ]; } && set -- --target none "$@"
exec env BR_NATIVE=1 BR_PYTHON=python bash "$HERE/pawsey/run.sh" "$mode" "$@"
