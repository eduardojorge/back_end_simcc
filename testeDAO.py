import Dao.sgbdSQL as sgbdSQL
import pandas as pd
import Dao.generalSQL as generalSQL
import math
#import lattes10 as lattes10


#print(generalSQL.lists_magazine_db("","15459993"))

df = pd.read_excel('c:\ic_junior\ic_junior.xlsx')
print(df)


'''

lista_colunas = ['Carimbo de data/hora','Nome Avaliador','Título do Projeto',
                 
                ]
#importo o arquivo
df = pd.read_excel("c:\ic_junior\ic_junior.xlsx"
                 , sheet_name = "Respostas ao formulário 1"
                 #, nrows = 100
                 , usecols = lista_colunas
                )
'''
TITULO=2
RESUMO=3
JUSTIFICATIVA=5
OBJETIVO=7
METODOLOGIA=9
SUBPROJETO=11

for i,infos in df.iterrows():

    print(infos[TITULO])
    print(infos[RESUMO])
    print(infos[JUSTIFICATIVA])
    print(infos[OBJETIVO])
    print(infos[METODOLOGIA])
    #print(infos[SUBPROJETO])
    total=0
    for x in range(7):
        if str(infos[SUBPROJETO+total])=="nan":
           
           break

        print("SUB %s" % str(infos[SUBPROJETO+total]))

        

        for y in range(1,8,2):

            #print(x,y)
            

            print(infos[SUBPROJETO+total+y])
        total=total+9

            


    #break
    


