import pandas as pd

df = pd.read_excel("c:\ic_junior\ic_junior.xlsx")

TITULO = 2
RESUMO = 3
JUSTIFICATIVA = 5
OBJETIVO = 7
METODOLOGIA = 9
SUBPROJETO = 11

for i, infos in df.iterrows():
    print(infos[TITULO])
    print(infos[RESUMO])
    print(infos[JUSTIFICATIVA])
    print(infos[OBJETIVO])
    print(infos[METODOLOGIA])
    # print(infos[SUBPROJETO])
    total = 0
    for x in range(7):
        if str(infos[SUBPROJETO + total]) == "nan":
            break

        print("SUB %s" % str(infos[SUBPROJETO + total]))

        for y in range(1, 8, 2):
            # print(x,y)

            print(infos[SUBPROJETO + total + y])
        total = total + 9

    # break
