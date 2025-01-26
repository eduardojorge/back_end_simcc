#!/usr/bin/env bash

SIMCC_HOME=$(dirname "$(readlink -f "$0")")

export SHELL=/bin/bash
export PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
export PYTHONPATH="$SIMCC_HOME:$PYTHONPATH"

cd "$SIMCC_HOME" || exit 1
source .venv/bin/activate
source .env

.venv/bin/python routines/soap_lattes.py

rm -rf $JADE_EXTRATOR_FOLTER/audit/*
"${JADE_EXTRATOR_FOLTER}/hop-run.sh" -r local -j Jade-Extrator-Hop -f "${PROJECT_HOME}/metadata/dataset/workflow/Index.hwf"

.venv/bin/python routines/population.py

.venv/bin/python routines/pog.py

.venv/bin/python routines/production.py

.venv/bin/python routines/lattes_bfs.py

.venv/bin/python routines/researcher_indprod.py

.venv/bin/python routines/program_indprod.py

.venv/bin/python routines/powerBI.py