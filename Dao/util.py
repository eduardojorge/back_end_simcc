 
import unidecode




# Função para consultar a lista de pesquisadores por palavras existentes na sua frequência
def filterSQLRank(text,split,booleanOperator,attribute,attribute_2):

 filter=" "
 if text!="": 
      t=[]
      t= text.split(split)  
      filter = ""
      i=0;

      if (len(t))==1:
          filter = " unaccent(LOWER("+attribute+"))='"+t[0].lower()+"' "+booleanOperator+ ""+ filter
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