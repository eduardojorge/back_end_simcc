class Projeto_IC(object):

        avaliador=""
        titulo=""
        resumo=""
        jsutificativa=""
        metodologia=""
        objetivo=""
        total_projeto =""
        subprojeto1=""
        subprojeto1_total=""

        subprojeto2=""
        subprojeto2_total=""

        subprojeto3=""
        subprojeto3_total=""

        subprojeto4=""
        subprojeto4_total=""

        subprojeto5=""
        subprojeto5_total=""

        subprojeto6=""
        subprojeto6_total=""

        subprojeto7=""
        subprojeto7_total=""
     
       

        def __init__(self):
          self.id=""
          
     
        def getJson(self):
         
         Projeto_IC  =  {
             
         'avaliador':self.avaliador, 
         'titulo': self.titulo,
         'resumo': self.resumo,
         'justificativa':self.jsutificativa,
         'metodologia':self.metodologia,
         'objetivo':self.objetivo,
         'total_projeto':self.total_projeto,

         'subprojeto1':self.subprojeto1,
         'subprojeto1_total':self.subprojeto1_total,

           'subprojeto2':self.subprojeto2,
         'subprojeto2_total':self.subprojeto2_total,

           'subprojeto3':self.subprojeto3,
         'subprojeto3_total':self.subprojeto3_total,

          'subprojeto4':self.subprojeto4,
         'subprojeto4_total':self.subprojeto4_total,

           'subprojeto5':self.subprojeto5,
         'subprojeto5_total':self.subprojeto5_total,

         'subprojeto6':self.subprojeto6,
         'subprojeto6_total':self.subprojeto6_total,

         'subprojeto7':self.subprojeto7,
         'subprojeto7_total':self.subprojeto7_total

     
        
         

         }
         return  Projeto_IC
          


    