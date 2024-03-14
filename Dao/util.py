import unidecode
import nltk
from nltk.tokenize import RegexpTokenizer


# Função para consultar a lista de pesquisadores por palavras existentes na sua frequência
def filterSQLRank(text,split,attribute_2):
 
 text=text.replace("-"," ")
 if (len(text.split(split)))==3:
    text=clean_stopwords(text)
 if (len(text.split("|")))==2:
     t=[]
     t= text.split("|")  
     filter = ""
     i=0;
     filter = """ ts_rank(to_tsvector(unaccent(LOWER(%s))), websearch_to_tsquery( '%s')) > %s    """ % (attribute_2, unidecode.unidecode(t[0]),0.04)   
     filter = "AND ("+filter+ " OR ts_rank(to_tsvector(unaccent(LOWER(%s))), websearch_to_tsquery( '%s')) > %s    """ % (attribute_2, unidecode.unidecode(t[1]),0.04) +")"    
     print("0")
     print(filter) 
     return filter  


 filter=" "
if text!="": 
      t=[]
      t= text.split(split)  
      filter = ""
      i=0;

      if (len(t))==1:
          #filter = " unaccent(LOWER("+attribute+"))='"+t[0].lower()+"' "+booleanOperator+ ""+ filter
          filter = """ ts_rank(to_tsvector(unaccent(LOWER(%s))), websearch_to_tsquery( '%s')) > %s    """ % (attribute_2, unidecode.unidecode(text),0.04)     
          print("Rank"+text)
          x = len(filter)   
          filter = filter[0:x-3]
          filter = " AND ("+filter+")" 
          text =text.strip().replace(" ","&")
          filter = filter + """ AND  (translate(unaccent(LOWER(%s)),'-\.:;''',' ') ::tsvector@@ unaccent(LOWER( '%s'))::tsquery)=TRUE """ % (attribute_2, unidecode.unidecode(text)) 
      
              
      else:     
          filter = """ ts_rank(to_tsvector(translate(unaccent(LOWER(%s)),'-\.:;''',' ')), websearch_to_tsquery( '%s<->%s')) > %s    """ % (attribute_2, unidecode.unidecode(t[0]), unidecode.unidecode(t[1]),0.04) 
          #filter = """ ts_rank(to_tsvector(unaccent(LOWER(%s))), websearch_to_tsquery( '%s<->%s')) > %s    """ % (attribute_2, unidecode.unidecode(t[0]), unidecode.unidecode(t[1]),0.04)    
          x = len(filter)   
          filter = filter[0:x-3]
          filter = " AND ("+filter+")"  
          #t[1] =t[1].strip().replace(" ","&")
          #filter = filter + """ AND  (translate(unaccent(LOWER(%s)),'-\.:;''',' ') ::tsvector@@ unaccent(LOWER( '%s'))::tsquery)=TRUE """ % (attribute_2, unidecode.unidecode(t[1]))      
      #x = len(filter)   
      #filter = filter[0:x-3]
      #filter = " AND ("+filter+")" 
 return filter


def filterSQLRank2(text, split, attribute_2):
    if (len(text.split(split))) == 3:
        text = clean_stopwords(text)

    filter = " "
    if text != "":
        t = []
        t = text.split(split)
        filter = ""
        i = 0

        if (len(t)) == 1:
            # filter = """ (translate(unaccent(LOWER(%s)),\':\',\'\') ::tsvector@@ '%s'::tsquery)=true   """ % (attribute,text)
            filter = (
                """ ts_rank(to_tsvector(unaccent(LOWER(%s))), websearch_to_tsquery( '%s')) > %s    """
                % (attribute_2, unidecode.unidecode(text), 0.02)
            )
            x = len(filter)
            filter = filter[0 : x - 3]
            filter = " AND (" + filter + ")"
            text = text.strip().replace(" ", "&")
            filter = (
                filter
                + """ AND  (translate(unaccent(LOWER(%s)),'\.:;''','') ::tsvector@@ unaccent(LOWER( '%s'))::tsquery)=TRUE """
                % (attribute_2, unidecode.unidecode(text))
            )
            print("Rank2" + text)
        else:
            filter = (
                """ ts_rank(to_tsvector(unaccent(LOWER(%s))), websearch_to_tsquery( '%s<->%s')) > %s    """
                % (
                    attribute_2,
                    unidecode.unidecode(t[0]),
                    unidecode.unidecode(t[1]),
                    0.02,
                )
            )
            x = len(filter)
            filter = filter[0 : x - 3]
            filter = " AND (" + filter + ")"
            t[1] = t[1].strip().replace(" ", "&")
            filter = (
                filter
                + """ AND  (translate(unaccent(LOWER(%s)),'\.:;''','') ::tsvector@@ unaccent(LOWER( '%s'))::tsquery)=TRUE """
                % (attribute_2, unidecode.unidecode(t[1]))
            )

    return filter


# Função para consultar a lista de pesquisadores por palavras existentes na sua frequência
def filterSQL(text, split, booleanOperator, attribute):
    filter = " "
    if text != "":
        t = []
        t = text.split(split)
        filter = ""
        i = 0
        for word in t:
            filter = (
                " unaccent(LOWER("
                + attribute
                + "))='"
                + unidecode.unidecode(word.lower())
                + "' "
                + booleanOperator
                + ""
                + filter
            )
            i = i + 1
        x = len(filter)
        filter = filter[0 : x - 3]
        filter = " AND (" + filter + ")"
    return filter


def filterSQLLike(text, split, booleanOperator, attribute):
    filter = " "
    if text != "":
        t = []
        t = text.split(split)
        filter = ""
        i = 0
        for word in t:
            filter = (
                " unaccent(LOWER("
                + attribute
                + ")) LIKE '%"
                + unidecode.unidecode(word.lower())
                + "%' "
                + booleanOperator
                + ""
                + filter
            )
            i = i + 1
        x = len(filter)
        filter = filter[0 : x - 3]
        filter = " AND (" + filter + ")"
    return filter


def unidecodelower(text1, text2):
    if unidecode.unidecode(text1.lower()) == unidecode.unidecode(text2.lower()):
        return True
    else:
        return False


def clean_stopwords(text):
    stopwords_portuguese = nltk.corpus.stopwords.words("portuguese")
    stopwords_english = nltk.corpus.stopwords.words("english")
    tokenize = RegexpTokenizer(r"\w+")
    tokens = []
    tokens = tokenize.tokenize(text)

    text_new = ""
    for word in tokens:
        if not (
            (word.lower() in stopwords_portuguese)
            or (word.lower() in stopwords_english)
        ):
            text_new = text_new + word.lower() + ";"
    return text_new[0 : len(text_new) - 1]
