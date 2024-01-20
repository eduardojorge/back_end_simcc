from Model.Projeto_IC import Projeto_IC
import pandas as pd

import json

df = pd.read_excel("c:\ic_junior\ic_junior.xlsx")
print(df)


AVALIADOR = 1
TITULO = 2
RESUMO = 3
JUSTIFICATIVA = 5
OBJETIVO = 7
METODOLOGIA = 9
SUBPROJETO = 11

list_projeto = []

for i, infos in df.iterrows():
    print(infos[TITULO])
    print(infos[RESUMO])
    print(infos[JUSTIFICATIVA])
    print(infos[OBJETIVO])
    print(infos[METODOLOGIA])

    p = Projeto_IC()
    p.avaliador = infos[AVALIADOR]
    p.titulo = infos[TITULO][0:40]
    p.resumo = (infos[RESUMO] * 2) / 5
    p.jsutificativa = (infos[JUSTIFICATIVA] * 6) / 5
    p.objetivo = (infos[OBJETIVO] * 16) / 5
    p.metodologia = (infos[METODOLOGIA] * 6) / 5
    p.total_projeto = f"{(p.resumo+p.jsutificativa+p.objetivo+p.metodologia):.2f}"

    # print(p.resumo)

    # print(infos[SUBPROJETO])

    total = 0
    for x in range(7):
        if str(infos[SUBPROJETO + total]) == "nan":
            break

        print("SUB %s" % str(infos[SUBPROJETO + total]))

        total_s = 0
        for y in range(1, 8, 2):
            if y == 1:
                # Justificativa
                total_s = total_s + ((infos[SUBPROJETO + total + y] * 5) / 5)
                print("X")

            if y == 3:
                # Metodologia
                total_s = total_s + ((infos[SUBPROJETO + total + y] * 6) / 5)
                print("X")

            if y == 5:
                # Objetivo
                total_s = total_s + ((infos[SUBPROJETO + total + y] * 6) / 5)
                print("X")

            if y == 7:
                # Cronograma
                total_s = total_s + ((infos[(SUBPROJETO + total + y)] * 3) / 5)
                print("X")

            # print(x,y)

            # print(infos[SUBPROJETO+total+y])

        if total == 0:
            print(total)
            # p.subprojeto1 = infos[(SUBPROJETO+total)][0:10]
            p.subprojeto1 = infos[SUBPROJETO][0:50]
            p.subprojeto1_total = f"{(total_s):.2f}"
        if total == 9:
            print(total)
            p.subprojeto2 = infos[SUBPROJETO + total][0:50]
            p.subprojeto2_total = f"{(total_s):.2f}"
        if total == 18:
            print(total)
            p.subprojeto3 = infos[SUBPROJETO + total][0:50]
            p.subprojeto3_total = f"{(total_s):.2f}"
        if total == 27:
            print(total)
            p.subprojeto4 = infos[SUBPROJETO + total][0:50]
            p.subprojeto4_total = f"{(total_s):.2f}"
        if total == 36:
            print(total)
            p.subprojeto5 = infos[SUBPROJETO + total][0:50]
            p.subprojeto5_total = f"{(total_s):.2f}"
        if total == 45:
            print(total)
            p.subprojeto6 = infos[SUBPROJETO + total][0:50]
            p.subprojeto6_total = f"{(total_s):.2f}"
        if total == 54:
            print(total)
            p.subprojeto7 = infos[SUBPROJETO + total][0:30]
            p.subprojeto7_total = f"{(total_s):.2f}"
        total = total + 9
        # break

    list_projeto.append(p.getJson())


json_string = json.dumps(list_projeto)
df = pd.read_json(json_string)
df.to_csv("file.csv")
