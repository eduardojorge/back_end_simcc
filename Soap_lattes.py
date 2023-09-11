from zeep import Client
from datetime import datetime
import xml.etree.ElementTree as ET
import os
import zipfile
import pandas as pd



client = Client( 'http://servicosweb.cnpq.br/srvcurriculo/WSCurriculo?wsdl')
#client.transport.session.proxies = {'http': # Proxy da UNEB ,
#                                    'https':  #Proxy da UNEB}
                                    
def get_DataAtualização(id:str) -> datetime:
    # Retorna a data de atualização do CV
    print("teste1")
    resultado = client.service.getDataAtualizacaoCV(id)
    print("teste2")
    if resultado != None:
        return datetime.strptime(resultado, '%d/%m/%Y %H:%M:%S')

def last_update(xml_filename):
    tree = ET.parse(f'/home/eduardomfjorge/curriculos/{xml_filename}')
    root = tree.getroot()
    lista = [i for i in root.items()  if i[0]=='DATA-ATUALIZACAO' or i[0]=='HORA-ATUALIZACAO']
    return datetime.strptime(lista[0][1] + lista[1][1],'%d%m%Y%H%M%S')


def getIdentificadorCNPQ(nome, data):
    # Retorna o identificador do CNPQ
    resultado = client.service.getIdentificadorCNPq(nomeCompleto=nome, dataNascimento=data,cpf='')
    if resultado != None:
        return resultado

def salvarCV(id, dir):
    data = get_DataAtualização(id)
    print(data)
    try:
        if data <= last_update(id + '.xml'):
            print('Currículo já está atualizado')
            return
    except:
           print('Currículo não  atualizado')  
        
    print(id)
    resultado = client.service.getCurriculoCompactado(id)
    arquivo = open(dir+'/zip/'+id + '.zip','wb')
    arquivo.write(resultado)
    arquivo.close()

    print('teste 4')
    with zipfile.ZipFile(dir+'/zip/'+id + '.zip','r') as zip_ref:
        zip_ref.extractall(dir)
    if os.path.exists(id + '.zip'):
        os.remove(id + '.zip')



df = pd.read_excel(r'files/pesquisadoresCimatec_v1.xlsx')
print(df)
LATTES_ID=0

x=0
for i,infos in df.iterrows():
   

    print("teste x "+ str(infos[LATTES_ID]))
    print(len(str(infos[LATTES_ID])))
    if (len(str(infos[LATTES_ID]))==14):
        lattes_id="00"+str(infos[LATTES_ID])
    elif (len(str(infos[LATTES_ID]))==15):
        lattes_id="0"+str(infos[LATTES_ID])
    else:
        lattes_id=str(infos[LATTES_ID])


    salvarCV( lattes_id,'/home/eduardomfjorge/curriculos')
    x=x+1
print("Fim "+str(x) )    
    #salvarCV('5674134492786099','/home/eduardomfjorge/teste/curriculos')
