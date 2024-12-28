#!/usr/bin/env bash

MY_PROJECT_HOME=$(dirname "$(readlink -f "$0")")

LOG_FILE="${MY_PROJECT_HOME}/routine.log"

# Redirecionar stdout e stderr para o arquivo de log
exec > >(tee -a "${LOG_FILE}") 2>&1

echo "MY_PROJECT_HOME: $MY_PROJECT_HOME"
cd "$MY_PROJECT_HOME" || exit 1

# Carregar variáveis de ambiente
source "${MY_PROJECT_HOME}/.env"
source "${MY_PROJECT_HOME}/.venv/bin/activate"

if [ -z "$VIRTUAL_ENV" ]; then
  echo "Ambiente virtual não ativado."
  exit 1
fi

DATE_FORMAT=$(date "+%d-%m-%Y")

check_error() {
  if [ $? -ne 0 ]; then
    echo "Erro: Falha ao executar $1 em $DATE_FORMAT"
    exit 1
  fi
}

echo "Iniciando execução do script em $DATE_FORMAT"

# Usar o Python do ambiente virtual diretamente
"${MY_PROJECT_HOME}/.venv/bin/python" "${MY_PROJECT_HOME}/soap_lattes.py"
check_error "Soap Lattes"

rm -rf $JADE_EXTRATOR_FOLTER/audit/*

"${JADE_EXTRATOR_FOLTER}/hop-run.sh" -r local -j Jade-Extrator-Hop -f "${PROJECT_HOME}/metadata/dataset/workflow/Index.hwf"
check_error "Hop Run"

"${MY_PROJECT_HOME}/.venv/bin/python" "${MY_PROJECT_HOME}/bd_population.py"
check_error "bd Population"

"${MY_PROJECT_HOME}/.venv/bin/python" "${MY_PROJECT_HOME}/bd_production.py"
check_error "bd Production"

"${MY_PROJECT_HOME}/.venv/bin/python" "${MY_PROJECT_HOME}/get_lattes10_bfs.py"
check_error "get Lattes10 BFS"

"${MY_PROJECT_HOME}/.venv/bin/python" "${MY_PROJECT_HOME}/bd_ind_prod_graduate_program.py"
check_error "bd Ind Prod Graduate Program"

"${MY_PROJECT_HOME}/.venv/bin/python" "${MY_PROJECT_HOME}/bd_ind_prod_researcher.py"
check_error "bd Ind Prod Researcher"

"${MY_PROJECT_HOME}/.venv/bin/python" "${MY_PROJECT_HOME}/csv_powerBI.py"
check_error "csv PowerBI"

echo "Script finalizado com sucesso em $DATE_FORMAT"
