
project_="1"

CIMATEC="1"
PROFNIT="2"
IFBA="3"

def setProject(type):
    global project_
    project_ =type
    print("----------"+project_)

def getProject():
    global project_
   # print(project_)
    if (project_==""):
        project_="1"
    #print("----------"+project_)
    return   project_; 