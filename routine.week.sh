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

"${MY_PROJECT_HOME}/.venv/bin/python" "${MY_PROJECT_HOME}/bd_terms.py"
check_error "Inscerção dos termos"

"${MY_PROJECT_HOME}/.venv/bin/python" "${MY_PROJECT_HOME}/get_openAlex.py"
check_error "Consulta dos dados do OpenAlex"

"${MY_PROJECT_HOME}/.venv/bin/python" "${MY_PROJECT_HOME}/bd_openAlex.py"
check_error "Inscerção dos dados do OpenAlex"

echo "Script finalizado com sucesso em $DATE_FORMAT"
