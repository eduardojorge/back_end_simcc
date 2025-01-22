import csv
import time

from selenium import webdriver
from selenium.common.exceptions import WebDriverException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select, WebDriverWait

driver = webdriver.Chrome()
url = 'https://sucupira-legado.capes.gov.br/sucupira/public/consultas/coleta/docente/listaDocente.xhtml'
driver.get(url)
time.sleep(2)
path = "//button[contains(@class, 'br-button primary small')]"
driver.find_element(By.XPATH, path).click()
with open(
    'graduate_program_researchers.csv', 'w', newline='', encoding='utf-8'
) as file:
    writer = csv.writer(file)
    instituicoes = [
        '28007018',
        '28008014',
        '28011015',
        '28002016',
        '28023013',
        '25020013',
        '29002001',
        '28010019',
        '28003012',
        '28006011',
        '28001010',
        '28049012',
        '28022017',
        '29007003',
        '28013018',
        '28005015',
    ]
    for instituicao in instituicoes:
        time.sleep(2)
        WebDriverWait(driver, 50000).until(
            EC.presence_of_element_located((By.NAME, 'form:j_idt33:inst:input'))
        ).send_keys(instituicao)
        time.sleep(2)
        select_instituicao = Select(
            driver.find_element(By.NAME, 'form:j_idt33:inst:listbox')
        )
        select_instituicao.select_by_index(0)
        time.sleep(2)
        select_programa = Select(
            driver.find_element(By.NAME, 'form:j_idt33:j_idt113')
        )
        last_index = len(select_programa.options)
        index = 1
        while index < last_index:
            select_programa = Select(
                driver.find_element(By.NAME, 'form:j_idt33:j_idt113')
            )
            select_programa.select_by_index(index)
            time.sleep(2)
            consultar_button = driver.find_element(By.NAME, 'form:consultar')
            driver.execute_script(
                'arguments[0].scrollIntoView();', consultar_button
            )
            consultar_button.click()
            bool = True
            while bool:
                try:
                    table_body_doscentes = WebDriverWait(driver, 10).until(
                        EC.visibility_of_element_located((By.TAG_NAME, 'tbody'))
                    )
                except WebDriverException:
                    break
                table_rows_doscentes = table_body_doscentes.find_elements(
                    By.TAG_NAME, 'tr'
                )
                if (len(table_rows_doscentes)) < 50:
                    bool = False
                for row in table_rows_doscentes:
                    dados_doscentes = row.find_elements(By.TAG_NAME, 'td')
                    row_dados = []
                    for dado in dados_doscentes:
                        row_dados.append(dado.text)
                    writer.writerow(row_dados)
                button_proxima = WebDriverWait(driver, 10).until(
                    EC.visibility_of_element_located((
                        By.ID,
                        'form:j_idt80:botaoProxPagina',
                    ))
                )
                button_proxima.click()
                if (
                    button_proxima.tag_name == 'span'
                ):  # verifica se não há mais paginas
                    bool = False
                time.sleep(2)
            index += 1
        # limpa campo do seletor de instituição:
        WebDriverWait(driver, 50000).until(
            EC.presence_of_element_located((By.NAME, 'form:j_idt33:inst:input'))
        ).clear()
driver.quit()
