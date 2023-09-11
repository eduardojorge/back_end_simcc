class GraduateProgram(object):


     graduate_program_id=""
     code=""
     name=""
     area=""
     modality=""
     type=""
     rating=""

     
     def __init__(self):
          self.id=""
          
     
     def getJson(self):
         
        
          graduate_program  = {
        'graduate_program_id': str(self.graduate_program_id),
        'code': str(self.code),
        'name': str(self.name),
        'area':str(self.area),
        'modality':str(self.modality),
        'type':str(self.type),
        'rating':str(self.rating)
       

        }
          return  graduate_program
          

r = GraduateProgram()

print(r.getJson())

    