class Resarcher_Production(object):

        id=""
      
        year=""

        patent=""
        software=""
        brand=""
        book=""
        article=""
        book_chapter=""
        work_in_event=""
        researcher=""
        guidance_ic_a=""
        guidance_ic_c=""

        guidance_m_a=""
        guidance_m_c=""

        guidance_d_a=""
        guidance_d_c=""

        guidance_g_a=""
        guidance_g_c=""

        guidance_e_a=""
        guidance_e_c=""

        lattes_10_id=""
        graduation=""
       

        def __init__(self):
          self.id=""
          
     
        def getJson(self):
         
         resarcher_Production  =  {
             
         'id': self.id,   
       
        
         'patent':self.patent,
         'software': self.software,
         'brand':self.brand,
         'book':self.book,
         'article':self.article,
         'book_chapter':self.book_chapter,
         'work_in_event':self.work_in_event,
         'researcher':self.researcher,
         'graduation':self.graduation,
        
          'guidance_ic_a':self.guidance_ic_a,
          'guidance_ic_c':self.guidance_ic_c,

          'guidance_m_a':self.guidance_m_a,
          'guidance_m_c':self.guidance_m_c,

          'guidance_d_a':self.guidance_d_a,
          'guidance_d_c':self.guidance_d_c,

           'guidance_g_a':self.guidance_g_a,
          'guidance_g_c':self.guidance_g_c,


          'guidance_e_a':self.guidance_e_a,
          'guidance_e_c':self.guidance_e_c,

          'lattes_10_id':self.lattes_10_id

     
        
         

         }
         return  resarcher_Production
          


    