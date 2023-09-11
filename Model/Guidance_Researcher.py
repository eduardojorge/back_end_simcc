class Guidance_Researcher(object):

        id=""
        title=""
        nature=""
        oriented=""
        type=""
        status=""
        year=""
        
        

        def __init__(self):
          self.id=""
          
     
        def getJson(self):
         
         Guidance_Researcher  =  {
             
         'id': self.id,   
         'title': self.title,
         'nature':self.nature,
         'oriented':self.oriented,
         'type':self.type,
         'status':self.status,
         'year': self.year
         

         }
        
         return  Guidance_Researcher
          


    