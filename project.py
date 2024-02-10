project_env = "1"


def getProject():
    global project_env
    if project_env == "":
        project_env = "1"
    return project_env
