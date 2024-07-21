import pandas as pd

import Dao.sgbdSQL as sgbdSQL


def get_rt_list():
    script_sql = """
        SELECT COUNT(*), u.rt
        FROM (
        SELECT rt FROM ufmg_teacher
        UNION
        SELECT rt FROM ufmg_technician) u
        GROUP BY u.rt
        """
    registry = sgbdSQL.consultar_db(script_sql)

    data_frame = pd.DataFrame(registry, columns=["count", "rt"])
    return data_frame.to_dict(orient="records")
