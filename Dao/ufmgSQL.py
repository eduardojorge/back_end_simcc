import pandas as pd

import Dao.sgbdSQL as sgbdSQL


def get_rt_list():
    script_sql = """
    SELECT matric, rt FROM ufmg_teacher
    """

    registry = sgbdSQL.consultar_db(script_sql)

    data_frame = pd.DataFrame(registry, columns=["matric", "rt"])

    return data_frame.to_dict(orient="records")
