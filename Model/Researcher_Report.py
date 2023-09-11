class Researcher_Report(object):

        id=""
        title=""
        year=""
        project_name=""
        financing=""
        

        def __init__(self):
          self.id=""
          
     
        def getJson(self):
         
         Researcher_Report =  {
             
         'id': self.id,   
         'title': self.title,
         'year': self.year,
         'project_name':self.project_name,
       
          'financing':self.financing
     
       
         

         }
         return  Researcher_Report
          


    