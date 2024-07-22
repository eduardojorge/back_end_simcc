import pandas as pd

import Dao.sgbdSQL as sgbdSQL


def get_rt_list():
    script_sql = """
        SELECT
            rt,
            COUNT(rt)
        FROM
            public.ufmg_teacher
        GROUP BY
            rt
        """

    registry = sgbdSQL.consultar_db(script_sql)

    data_frame_teacher = pd.DataFrame(registry, columns=["rt", "count"])

    script_sql = """
        SELECT
            rt,
            COUNT(rt)
        FROM
            public.ufmg_technician
        GROUP BY
            rt
        """
    registry = sgbdSQL.consultar_db(script_sql)

    data_frame_technician = pd.DataFrame(registry, columns=["rt", "count"])

    return {
        "teachers": data_frame_teacher.to_dict(orient="records"),
        "technician": data_frame_technician.to_dict(orient="records"),
    }
