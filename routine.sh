#!/usr/bin/env bash

MY_PROJECT_HOME=$(dirname "$(readlink -f "$0")")
LOG_FILE="${MY_PROJECT_HOME}/routine.log"

echo "MY_PROJECT_HOME: $MY_PROJECT_HOME"

# Carregar variáveis de ambiente
source "${MY_PROJECT_HOME}/.env"
source "${MY_PROJECT_HOME}/.venv/bin/activate"

if [ -z "$VIRTUAL_ENV" ]; then
  echo "Ambiente virtual não ativado." | tee -a "$LOG_FILE"
  exit 1
fi

DATE_FORMAT=$(date "+%d-%m-%Y")
LOCK_FILE="/tmp/hop_execution.lock"

if [ -e "$LOCK_FILE" ]; then
  echo "Erro: O script já está em execução." | tee -a "$LOG_FILE"
  exit 1
fi

touch "$LOCK_FILE"

check_error() {
  if [ $? -ne 0 ]; then
    echo "Erro: Falha ao executar $1 em $DATE_FORMAT" | tee -a "$LOG_FILE"
    rm -f "$LOCK_FILE"
    exit 1
  fi
}

trap "rm -f $LOCK_FILE" EXIT

echo "Iniciando execução do script em $DATE_FORMAT" | tee -a "$LOG_FILE"

# Usar o Python do ambiente virtual diretamente
"${MY_PROJECT_HOME}/.venv/bin/python" "${MY_PROJECT_HOME}/soap_lattes_adm.py" | tee -a "$LOG_FILE"
check_error "soap_lattes_adm.py"

rm -rf $JADE_EXTRATOR_FOLTER/audit/*

"${JADE_EXTRATOR_FOLTER}/hop-run.sh" -r local -j Jade-Extrator-Hop -f "${PROJECT_HOME}/metadata/dataset/workflow/Index.hwf" | tee -a "$LOG_FILE"
check_error "hop-run.sh"

"${MY_PROJECT_HOME}/.venv/bin/python" "${MY_PROJECT_HOME}/bd_population.py" | tee -a "$LOG_FILE"
check_error "bd_population.py"

"${MY_PROJECT_HOME}/.venv/bin/python" "${MY_PROJECT_HOME}/bd_production.py" | tee -a "$LOG_FILE"
check_error "bd_production.py"

"${MY_PROJECT_HOME}/.venv/bin/python" "${MY_PROJECT_HOME}/get_lattes10_bfs.py" | tee -a "$LOG_FILE"
check_error "get_lattes10_bfs.py"

"${MY_PROJECT_HOME}/.venv/bin/python" "${MY_PROJECT_HOME}/bd_ind_prod_graduate_program.py" | tee -a "$LOG_FILE"
check_error "bd_ind_prod_graduate_program.py"

"${MY_PROJECT_HOME}/.venv/bin/python" "${MY_PROJECT_HOME}/bd_ind_prod_researcher.py" | tee -a "$LOG_FILE"
check_error "bd_ind_prod_researcher.py"

"${MY_PROJECT_HOME}/.venv/bin/python" "${MY_PROJECT_HOME}/csv_powerBI.py" | tee -a "$LOG_FILE"
check_error "csv_powerBI.py"

echo "Script finalizado com sucesso em $DATE_FORMAT" | tee -a "$LOG_FILE"