from simcc.repositories import conn
from simcc.schemas.GraduateProgram import GraduateProgram


def list_graduate_programs() -> list[GraduateProgram]:
    SCRIPT_SQL = """
        SELECT graduate_program_id, code, name, AREA, modality, type, rating,
            institution_id, state, city, region, url_image, acronym, description,
            visible, site
        FROM public.graduate_program;
        """

    result = conn.select(SCRIPT_SQL)
    return result
