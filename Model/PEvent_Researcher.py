class PEvent_Researcher(object):

        id=""
        event_name=""
        year=""
        nature=""
        participation=""
       

        def __init__(self):
          self.id=""
          
     
        def getJson(self):
         
         PEvent_Researcher  =  {
             
         'id': self.id,   
         'event_name': self.event_name,
         'year': self.year,
         'participation':self.participation,
         
         'nature':self.nature
         
     
        
         

         }
         return  PEvent_Researcher
          


    