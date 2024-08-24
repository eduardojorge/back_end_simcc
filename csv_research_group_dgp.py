from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd
import time
import os

options = webdriver.ChromeOptions()
options.headless = True
driver = webdriver.Chrome(
    service=Service(ChromeDriverManager().install()), options=options
)

# Navegar até a página inicial
driver.get("https://dgp.cnpq.br/dgp/faces/consulta/consulta_parametrizada.jsf")
time.sleep(20)  # Esperar a página carregar (ajuste conforme necessário)

# Preencher o campo "Termo de Busca" com *
termo_busca = driver.find_element(
    By.ID, "idFormConsultaParametrizada:idTextoFiltro")
termo_busca.send_keys("*")

# Clicar no botão "Pesquisar"
botao_pesquisar = driver.find_element(
    By.ID, "idFormConsultaParametrizada:idPesquisar")
botao_pesquisar.click()
# Esperar a página carregar os resultados (ajuste conforme necessário)
time.sleep(30)

# Nome do arquivo CSV
csv_file = "grupos_pesquisa.csv"


# Função para extrair dados
def extrair_dados():
    items = driver.find_elements(By.CLASS_NAME, "ui-datalist-item")
    dados_pagina = []
    for item in items:
        grupo = (
            item.find_elements(By.CLASS_NAME, "control-group")[0]
            .find_element(By.CLASS_NAME, "controls")
            .text.strip()
        )
        instituicao = (
            item.find_elements(By.CLASS_NAME, "control-group")[1]
            .find_element(By.CLASS_NAME, "controls")
            .text.strip()
        )

        # Coletar o primeiro líder
        lider1 = item.find_element(
            By.XPATH,
            ".//label[contains(text(), 'Líder(es):')]/following-sibling::div/a",
        ).text.strip()

        # Coletar o segundo líder, se existir
        lider2 = ""
        try:
            lider2 = item.find_element(
                By.XPATH,
                ".//label[contains(text(), 'Líder(es):')]/following::div[@class='control-group'][1]//a",
            ).text.strip()
        except:
            pass

        # Corrigir a captura da área
        area_element = item.find_elements(
            By.XPATH, ".//label[contains(text(), 'Área:')]/following-sibling::div"
        )
        area = area_element[0].text.strip() if area_element else ""

        dados_pagina.append([grupo, instituicao, lider1, lider2, area])

    return dados_pagina


# Inicializar CSV
if not os.path.exists(csv_file):
    df = pd.DataFrame(
        columns=["Grupo de Pesquisa", "Instituição",
                 "Líder 1", "Líder 2", "Área"]
    )
    df.to_csv(csv_file, index=False, encoding="utf-8-sig")

# Ler última página processada
ultima_pagina = 1
if os.path.exists("ultima_pagina.txt"):
    with open("ultima_pagina.txt", "r") as f:
        ultima_pagina = int(f.read().strip())

# Navegar até a última página processada
for _ in range(1, ultima_pagina):
    next_button = driver.find_element(By.CLASS_NAME, "ui-paginator-next")
    if "ui-state-disabled" in next_button.get_attribute("class"):
        break
    next_button.click()
    time.sleep(30)  # Esperar a página carregar (ajuste conforme necessário)

# Extrair dados de todas as páginas
pagina_atual = ultima_pagina
while True:
    try:
        dados_pagina = extrair_dados()
        df = pd.DataFrame(
            dados_pagina,
            columns=["Grupo de Pesquisa", "Instituição",
                     "Líder 1", "Líder 2", "Área"],
        )
        df.to_csv(csv_file, mode="a", header=False,
                  index=False, encoding="utf-8-sig")

        # Salvar estado da última página processada
        with open("ultima_pagina.txt", "w") as f:
            f.write(str(pagina_atual))

        # Navegar para a próxima página
        next_button = driver.find_element(By.CLASS_NAME, "ui-paginator-next")
        if "ui-state-disabled" in next_button.get_attribute("class"):
            break
        next_button.click()
        # Esperar a página carregar (ajuste conforme necessário)
        time.sleep(30)
        pagina_atual += 1
    except Exception as e:
        print(f"Erro: {e}")
        break

# Fechar o navegador
driver.quit()

print("Dados extraídos e exportados para grupos_pesquisa.csv")
