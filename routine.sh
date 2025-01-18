#!/usr/bin/env bash

PROJECT_HOME=$(dirname "$(readlink -f "$0")")

cd "$PROJECT_HOME" || exit 1
source "${PROJECT_HOME}/.env"

rm -rf $JADE_EXTRATOR_FOLTER/audit/*

poetry run python routines/soap_lattes.py

"${JADE_EXTRATOR_FOLTER}/hop-run.sh" -r local -j Jade-Extrator-Hop -f "${PROJECT_HOME}/metadata/dataset/workflow/Index.hwf"

poetry run python routines/population.py

poetry run python routines/pog.py

poetry run python routines/production.py

poetry run python routines/lattes_bfs.py

poetry run python routines/researcher_class.py
