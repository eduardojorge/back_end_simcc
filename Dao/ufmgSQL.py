import pandas as pd

import Dao.sgbdSQL as sgbdSQL


def get_rt_list():
    script_sql = """
    SELECT matric, rt FROM ufmg_teacher
    """
    registry = sgbdSQL.consultar_db(script_sql)

    data_frame_teacher = pd.DataFrame(registry, columns=["matric", "rt"])

    script_sql = """
    SELECT matric, rt FROM ufmg_technician
    """
    registry = sgbdSQL.consultar_db(script_sql)

    data_frame_technician = pd.DataFrame(registry, columns=["matric", "rt"])

    return {
        "rt_teacher": data_frame_teacher.to_dict(orient="records"),
        "rt_technician": data_frame_technician.to_dict(orient="records"),
    }
