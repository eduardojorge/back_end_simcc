 
import unidecode
import nltk
from nltk.tokenize import RegexpTokenizer




# Função para consultar a lista de pesquisadores por palavras existentes na sua frequência
def filterSQLRank(text,split,attribute_2):
 if (len(text.split(split)))==3:
    text=clean_stopwords(text)

 filter=" "
 if text!="": 
      t=[]
      t= text.split(split)  
      filter = ""
      i=0;

      if (len(t))==1:
          #filter = " unaccent(LOWER("+attribute+"))='"+t[0].lower()+"' "+booleanOperator+ ""+ filter
          filter = """ ts_rank(to_tsvector(unaccent(LOWER(%s))), websearch_to_tsquery( '%s')) > %s    """ % (attribute_2,text,0.04)     
          print("Rank"+text)
      
              
      else:     
          filter = """ ts_rank(to_tsvector(unaccent(LOWER(%s))), websearch_to_tsquery( '%s<->%s')) > %s    """ % (attribute_2,t[0],t[1],0.04)    
      x = len(filter)   
      filter = filter[0:x-3]
      filter = " AND ("+filter+")" 
 return filter

def filterSQLRank2(text,split,attribute_2):
 
 if (len(text.split(split)))==3:
      text=clean_stopwords(text)

 filter=" "
 if text!="": 
      t=[]
      t= text.split(split)  
      filter = ""
      i=0;

      if (len(t))==1:
          #filter = """ (translate(unaccent(LOWER(%s)),\':\',\'\') ::tsvector@@ '%s'::tsquery)=true   """ % (attribute,text)
          filter = """ ts_rank(to_tsvector(unaccent(LOWER(%s))), websearch_to_tsquery( '%s')) > %s    """ % (attribute_2,text,0.05)     
          print("Rank2"+text)
      else:     
          filter = """ ts_rank(to_tsvector(unaccent(LOWER(%s))), websearch_to_tsquery( '%s<->%s')) > %s    """ % (attribute_2,t[0],t[1],0.04)    
      x = len(filter)   
      filter = filter[0:x-3]
      filter = " AND ("+filter+")" 
 return filter



# Função para consultar a lista de pesquisadores por palavras existentes na sua frequência
def filterSQL(text,split,booleanOperator,attribute):

 filter=" "
 if text!="": 
      t=[]
      t= text.split(split)  
      filter = ""
      i=0;
      for word in t:
          filter = " unaccent(LOWER("+attribute+"))='"+word.lower()+"' "+booleanOperator+ ""+ filter
          i=i+1
      x = len(filter)   
      filter = filter[0:x-3]
      filter = " AND ("+filter+")" 
 return filter

def filterSQLLike(text,split,booleanOperator,attribute):

 filter=" "
 if text!="": 
      t=[]
      t= text.split(split)  
      filter = ""
      i=0;
      for word in t:
          filter = " unaccent(LOWER("+attribute+")) LIKE '%"+word.lower()+"%' "+booleanOperator+ ""+ filter
          i=i+1
      x = len(filter)   
      filter = filter[0:x-3]
      filter = " AND ("+filter+")" 
 return filter

def unidecodelower(text1,text2):
    if unidecode.unidecode(text1.lower())==unidecode.unidecode(text2.lower()):
       return True
    else:
       return False

def clean_stopwords(text):
    stopwords_portuguese = nltk.corpus.stopwords.words('portuguese')
    stopwords_english = nltk.corpus.stopwords.words('english')
    tokenize = RegexpTokenizer(r'\w+')
    tokens =[]
    tokens = tokenize.tokenize(text)   

    text_new=""
    for word in tokens:
        
        if not((word.lower() in stopwords_portuguese) or (word.lower() in stopwords_english)):
               text_new=text_new+word.lower()+";"
    return text_new[0:len(text_new)-1]


print(clean_stopwords("banco;de;dados"))
               