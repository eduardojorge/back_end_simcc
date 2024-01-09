import Dao.sgbdSQL as sgbdSQL
import pandas as pd
import unidecode
import Dao.util as util


import project as project_

project_.project_ = "4"


def lista_researcher_patent_db(tax, term_p, term_i, termos, tax_id):
    term_p = unidecode.unidecode(term_p.lower())
    term_i = unidecode.unidecode(term_i.lower())

    filter_p = util.filterSQLRank2(term_p, ";", "p.title")
    filter_i = util.filterSQLRank2(term_i, ";", "p.title")
    filter_p = filter_p[5 : len(filter_p)]
    filter_i = filter_i[5 : len(filter_i)]

    # filter= util.filterSQL(text,";","or","gae.name")

    sql = """

     SELECT DISTINCT    r.id as researcher_id ,r.institution_id,r.city_id,
               
               
                          '%s' as terms_tax,p.title,p.development_year as year,'%s' as tax,'%s' as tax_id
                          FROM  researcher r  ,
                          patent p
                           WHERE 
                         
                      

                            ( %s or %s )

                         
                      
     
     """ % (
        termos,
        tax,
        tax_id,
        filter_p,
        filter_i,
    )

    print(sql)

    reg = sgbdSQL.consultar_db(sql)

    df_bd = pd.DataFrame(
        reg,
        columns=[
            "researcher_id",
            "institution_id",
            "city_id",
            "terms",
            "title",
            "year",
            "tax",
            "tax_id",
        ],
    )
    print(df_bd)
    return df_bd


# Função para consultar a lista de pesquisadores por palavras existentes na sua frequência
def list_researchers_article_abstract_tax_db(tax_id, term, word, type):
    word = word.replace(" ", "&")

    filter_p = util.filterSQLRank2(term, ";", "title")
    # rank =  filter_p[6:len(filter_p)-9]

    df_bd = pd.DataFrame()
    if type == "ARTICLE":
        sql = """SELECT DISTINCT r.id as id,b.title,
                           r.institution_id,
                           r.city_id,
                           b.year,ba.qualis,jcr, periodical_magazine_name,
                          '%s' as tax_id
                        
                           FROM  researcher r ,
                          
                           bibliographic_production b,
                           bibliographic_production_article ba
                           WHERE 
                            b.id= ba.bibliographic_production_id
                            AND r.id = b.researcher_id
                            
                          
                          
                            %s 
                          
     
                         
                          

                          
                           ORDER BY year desc""" % (
            tax_id,
            filter_p,
        )
        print(sql)
        reg = sgbdSQL.consultar_db(sql)
        df_bd = pd.DataFrame(
            reg,
            columns=[
                "researcher_id",
                "title",
                "institution_id",
                "city_id",
                "year",
                "qualis",
                "jcr",
                "magazine_name",
                "tax_id",
            ],
        )

    return df_bd


# df = pd.read_excel(r'files/pesquisadoresCimatec_v1.xlsx')
# df = pd.read_excel(r'C:\simccv3\tEnergiasRenovaveis.xlsx')
df = pd.read_excel(r"C:\simccv3\Taxonomia.xlsx")


print(df)
CODIGO = 0
HIERARQUIA = 1
TERMOS = 2
COMBINACAO = 3

x = 0

for i, infos in df.iterrows():
    print("teste x " + str(infos[TERMOS]))
    t = []
    t = str(infos[COMBINACAO]).split(";")
    i = 0
    for combinacao_ in t:
        t_ = []
        t_ = str(infos[TERMOS]).split(";")

        for term_ in t_:
            term = (
                unidecode.unidecode(combinacao_.lower())
                + ";"
                + str(unidecode.unidecode(term_.lower()))
            )
            word = unidecode.unidecode(term_.lower()).strip()

            df1_article = list_researchers_article_abstract_tax_db(
                str(infos[CODIGO]), term, word, "ARTICLE"
            )
            # df1_abstract =  list_researchers_article_abstract_tax_db(str(infos[TAX]),str(infos[TERMOS_P]),str(infos[TERMOS_I]),str(infos[TERMOS]),"ABSTRACT",str(infos[TAX_ID]))

            if x != 0:
                # df_patent = pd.concat([df_patent, df1_patent], axis=0, join='inner')
                df_article = pd.concat([df_article, df1_article], axis=0, join="inner")
                #  df_abstract = pd.concat([df_abstract, df1_abstract], axis=0, join='inner')
            else:
                # df_patent=df1_patent
                df_article = df1_article
                # df_abstract=df1_abstract

            x = x + 1
print("Fim " + str(x))
df_article.to_csv("c:\\simccv3\\article_tax.csv")
