#!/usr/bin/env bash

check_error() {
  if [ $? -ne 0 ]; then
    echo "Erro: Falha ao executar $1 em $DATE_FORMAT"
    exit 1
  fi
}

SIMCC_HOME=$(dirname "$(readlink -f "$0")")

cd "$SIMCC_HOME" || exit 1
source .venv/bin/activate
source .env

"${SIMCC_HOME}/.venv/bin/python" routines/soap_lattes.py
check_error "soap_lattes"

rm -rf $JADE_EXTRATOR_FOLTER/audit/*
"${JADE_EXTRATOR_FOLTER}/hop-run.sh" -r local -j Jade-Extrator-Hop -f "${PROJECT_HOME}/metadata/dataset/workflow/Index.hwf"

"${SIMCC_HOME}/.venv/bin/python" routines/population.py
check_error "population"

"${SIMCC_HOME}/.venv/bin/python" routines/pog.py
check_error "pog"

"${SIMCC_HOME}/.venv/bin/python" routines/production.py
check_error "production"

"${SIMCC_HOME}/.venv/bin/python" routines/lattes_bfs.py
check_error "lattes_bfs"

"${SIMCC_HOME}/.venv/bin/python" routines/researcher_indprod.py
check_error "researcher_indprod"

"${SIMCC_HOME}/.venv/bin/python" routines/program_indprod.py
check_error "program_indprod"

"${SIMCC_HOME}/.venv/bin/python" routines/powerBI.py
check_error "powerBI"

"${SIMCC_HOME}/.venv/bin/python" routines/terms.py
check_error "terms"