class Magazine(object):

        id=""
        qualis=""
        magazine=""
        issn=""
        jif=""
        jcr_link=""

        def __init__(self):
          self.id=""
          
     
        def getJson(self):
         
         Magazine  =  {
             
         'id': self.id,   
        
         'qualis':self.qualis,
         'magazine':self.magazine,
         'issn':self.issn,
        
         'jif':self.jif,
         'jcr_link':self.jcr_link

         }
         return Magazine
          


    