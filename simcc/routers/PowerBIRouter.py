import os
from pathlib import Path

from fastapi import APIRouter, Request
from fastapi.responses import FileResponse

from routines import powerBI

router = APIRouter()

STORAGE_PATH = Path('storage/powerBI')
STORAGE_PATH.mkdir(parents=True, exist_ok=True)


@router.get('/dim_titulacao.csv')
def dim_titulacao_xlsx():
    powerBI.dim_titulacao()
    file_path = os.path.join(STORAGE_PATH, 'dim_titulacao.xlsx')
    return FileResponse(file_path, filename='dim_titulacao.xlsx')


@router.get('/fat_area_specialty.csv')
def fat_area_specialty_csv():
    powerBI.fat_area_specialty()
    file_path = os.path.join(STORAGE_PATH, 'fat_area_specialty.csv')
    return FileResponse(file_path, filename='fat_area_specialty.csv')


@router.get('/fat_great_area.csv')
def fat_great_area_csv():
    powerBI.fat_great_area()
    file_path = os.path.join(STORAGE_PATH, 'fat_great_area.csv')
    return FileResponse(file_path, filename='fat_great_area.csv')


@router.get('/dim_area_specialty.csv')
def dim_area_specialty_csv():
    powerBI.dim_area_specialty()
    file_path = os.path.join(STORAGE_PATH, 'dim_area_specialty.csv')
    return FileResponse(file_path, filename='dim_area_specialty.csv')


@router.get('/dim_great_area.csv')
def dim_great_area_csv():
    powerBI.dim_great_area()
    file_path = os.path.join(STORAGE_PATH, 'dim_great_area.csv')
    return FileResponse(file_path, filename='dim_great_area.csv')


@router.get('/fat_openalex_researcher.csv')
def fat_openalex_researcher_csv():
    powerBI.fat_openalex_researcher()
    file_path = os.path.join(STORAGE_PATH, 'fat_openalex_researcher.csv')
    return FileResponse(file_path, filename='fat_openalex_researcher.csv')


@router.get('/researcher_area_leader.csv')
def researcher_area_leader_csv():
    powerBI.researcher_area_leader()
    file_path = os.path.join(STORAGE_PATH, 'researcher_area_leader.csv')
    return FileResponse(file_path, filename='researcher_area_leader.csv')


@router.get('/fat_openalex_article.csv')
def fat_openalex_article_csv():
    powerBI.fat_openalex_article()
    file_path = os.path.join(STORAGE_PATH, 'fat_openalex_article.csv')
    return FileResponse(file_path, filename='fat_openalex_article.csv')


@router.get('/dim_area_leader.csv')
def dim_area_leader_csv():
    powerBI.dim_area_leader()
    file_path = os.path.join(STORAGE_PATH, 'dim_area_leader.csv')
    return FileResponse(file_path, filename='dim_area_leader.csv')


@router.get('/npai.png')
def npai_png():
    powerBI.npai()
    file_path = os.path.join(STORAGE_PATH, 'npai.png')
    return FileResponse(file_path, filename='npai.png')


@router.get('/iapos.png')
def iapos_png():
    powerBI.iapos()
    file_path = os.path.join(STORAGE_PATH, 'iapos.png')
    return FileResponse(file_path, filename='iapos.png')


@router.get('/dim_city.csv')
def dim_city_csv():
    powerBI.dim_city()
    file_path = os.path.join(STORAGE_PATH, 'dim_city.csv')
    return FileResponse(file_path, filename='dim_city.csv')


@router.get('/ufmg_researcher.csv')
def ufmg_researcher_csv():
    powerBI.ufmg_researcher()
    file_path = os.path.join(STORAGE_PATH, 'ufmg_researcher.csv')
    return FileResponse(file_path, filename='ufmg_researcher.csv')


@router.get('/DimensaoAno.xlsx')
def DimensaoAno_xlsx():
    powerBI.DimensaoAno()
    file_path = os.path.join(STORAGE_PATH, 'DimensaoAno.xlsx')
    return FileResponse(file_path, filename='DimensaoAno.xlsx')


@router.get('/DimensaoTipoProducao.xlsx')
def DimensaoTipoProducao_xlsx():
    powerBI.DimensaoTipoProducao()
    file_path = os.path.join(STORAGE_PATH, 'DimensaoTipoProducao.xlsx')
    return FileResponse(file_path, filename='DimensaoTipoProducao.xlsx')


@router.get('/platform_image.xlsx')
def platform_image_xlsx():
    powerBI.platform_image()
    file_path = os.path.join(STORAGE_PATH, 'platform_image.xlsx')
    return FileResponse(file_path, filename='platform_image.xlsx')


@router.get('/Qualis.xlsx')
def Qualis_xlsx():
    powerBI.Qualis()
    file_path = os.path.join(STORAGE_PATH, 'Qualis.xlsx')
    return FileResponse(file_path, filename='Qualis.xlsx')


@router.get('/data.csv')
def data_csv():
    powerBI.data()
    file_path = os.path.join(STORAGE_PATH, 'data.csv')
    return FileResponse(file_path, filename='data.csv')


@router.get('/cimatec_graduate_program_student.csv')
def cimatec_graduate_program_student_csv():
    powerBI.cimatec_graduate_program_student()
    file_name = 'cimatec_graduate_program_student.csv'
    file_path = os.path.join(STORAGE_PATH, file_name)
    return FileResponse(file_path, filename=file_name)


@router.get('/dim_graduate_program_acronym.csv')
def dim_graduate_program_acronym_csv():
    powerBI.dim_graduate_program_acronym()
    file_name = 'dim_graduate_program_acronym.csv'
    file_path = os.path.join(STORAGE_PATH, file_name)
    return FileResponse(file_path, filename=file_name)


@router.get('/graduate_program_researcher_year_unnest.csv')
def graduate_program_researcher_year_unnest_csv():
    powerBI.graduate_program_researcher_year_unnest()
    file_name = 'graduate_program_researcher_year_unnest.csv'
    file_path = os.path.join(STORAGE_PATH, file_name)
    return FileResponse(file_path, filename=file_name)


@router.get('/dim_departament_technician.csv')
def dim_departament_technician_csv():
    powerBI.dim_departament_technician()
    file_name = 'dim_departament_technician.csv'
    file_path = os.path.join(STORAGE_PATH, file_name)
    return FileResponse(file_path, filename=file_name)


@router.get('/dim_departament_researcher.csv')
def dim_departament_researcher_csv():
    powerBI.dim_departament_researcher()
    file_name = 'dim_departament_researcher.csv'
    file_path = os.path.join(STORAGE_PATH, file_name)
    return FileResponse(file_path, filename=file_name)


@router.get('/fat_group_leaders.csv')
def fat_group_leaders_csv():
    powerBI.fat_group_leaders()
    file_name = 'fat_group_leaders.csv'
    file_path = os.path.join(STORAGE_PATH, file_name)
    return FileResponse(file_path, filename=file_name)


@router.get('/dim_research_group.csv')
def dim_research_group_csv():
    powerBI.dim_research_group()
    file_name = 'dim_research_group.csv'
    file_path = os.path.join(STORAGE_PATH, file_name)
    return FileResponse(file_path, filename=file_name)


@router.get('/dim_category_level_code.csv')
def dim_category_level_code_csv():
    powerBI.dim_category_level_code()
    file_name = 'dim_category_level_code.csv'
    file_path = os.path.join(STORAGE_PATH, file_name)
    return FileResponse(file_path, filename=file_name)


@router.get('/fat_foment.csv')
def fat_foment_csv():
    powerBI.fat_foment()
    file_name = 'fat_foment.csv'
    file_path = os.path.join(STORAGE_PATH, file_name)
    return FileResponse(file_path, filename=file_name)


@router.get('/fat_production_tecnical_year_novo_csv_db.csv')
def fat_production_tecnical_year_novo_csv_db_csv():
    powerBI.fat_production_tecnical_year_novo_csv_db()
    file_name = 'fat_production_tecnical_year_novo_csv_db.csv'
    file_path = os.path.join(STORAGE_PATH, file_name)
    return FileResponse(file_path, filename=file_name)


@router.get('/dim_institution.csv')
def dim_institution_csv():
    powerBI.dim_institution()
    file_name = 'dim_institution.csv'
    file_path = os.path.join(STORAGE_PATH, file_name)
    return FileResponse(file_path, filename=file_name)


@router.get('/researcher_city.csv')
def researcher_city_csv():
    powerBI.researcher_city()
    file_name = 'researcher_city.csv'
    file_path = os.path.join(STORAGE_PATH, file_name)
    return FileResponse(file_path, filename=file_name)


@router.get('/dim_researcher.csv')
def dim_researcher_csv(request: Request):
    origin = request.base_url
    powerBI.dim_researcher(origin)
    file_name = 'dim_researcher.csv'
    file_path = os.path.join(STORAGE_PATH, file_name)
    return FileResponse(file_path, filename=file_name)


@router.get('/fat_simcc_bibliographic_production.csv')
def fat_simcc_bibliographic_production_csv():
    powerBI.fat_simcc_bibliographic_production()
    file_name = 'fat_simcc_bibliographic_production.csv'
    file_path = os.path.join(STORAGE_PATH, file_name)
    return FileResponse(file_path, filename=file_name)


@router.get('/production_tecnical_year.csv')
def production_tecnical_year_csv():
    powerBI.production_tecnical_year()
    file_name = 'production_tecnical_year.csv'
    file_path = os.path.join(STORAGE_PATH, file_name)
    return FileResponse(file_path, filename=file_name)


@router.get('/researcher.csv')
def researcher_csv():
    powerBI.researcher()
    file_name = 'researcher.csv'
    file_path = os.path.join(STORAGE_PATH, file_name)
    return FileResponse(file_path, filename=file_name)


@router.get('/article_qualis_year_institution.csv')
def article_qualis_year_institution_csv():
    powerBI.article_qualis_year_institution()
    file_name = 'article_qualis_year_institution.csv'
    file_path = os.path.join(STORAGE_PATH, file_name)
    return FileResponse(file_path, filename=file_name)


@router.get('/production_researcher.csv')
def production_researcher_csv():
    powerBI.production_researcher()
    file_name = 'production_researcher.csv'
    file_path = os.path.join(STORAGE_PATH, file_name)
    return FileResponse(file_path, filename=file_name)


@router.get('/article_qualis_year.csv')
def article_qualis_year_csv():
    powerBI.article_qualis_year()
    file_name = 'article_qualis_year.csv'
    file_path = os.path.join(STORAGE_PATH, file_name)
    return FileResponse(file_path, filename=file_name)


@router.get('/production_year_distinct.csv')
def production_year_distinct_csv():
    powerBI.production_year_distinct()
    file_name = 'production_year_distinct.csv'
    file_path = os.path.join(STORAGE_PATH, file_name)
    return FileResponse(file_path, filename=file_name)


@router.get('/production_year.csv')
def production_year_csv():
    powerBI.production_year()
    file_name = 'production_year.csv'
    file_path = os.path.join(STORAGE_PATH, file_name)
    return FileResponse(file_path, filename=file_name)


@router.get('/production_coauthors_csv_db.csv')
def production_coauthors_csv_db_csv():
    powerBI.production_coauthors_csv_db()
    file_name = 'production_coauthors_csv_db.csv'
    file_path = os.path.join(STORAGE_PATH, file_name)
    return FileResponse(file_path, filename=file_name)


@router.get('/fat_researcher_ind_prod.csv')
def fat_researcher_ind_prod_csv():
    powerBI.fat_researcher_ind_prod()
    file_name = 'fat_researcher_ind_prod.csv'
    file_path = os.path.join(STORAGE_PATH, file_name)
    return FileResponse(file_path, filename=file_name)


@router.get('/graduate_program_ind_prod.csv')
def graduate_program_ind_prod_csv():
    powerBI.graduate_program_ind_prod()
    file_name = 'graduate_program_ind_prod.csv'
    file_path = os.path.join(STORAGE_PATH, file_name)
    return FileResponse(file_path, filename=file_name)


@router.get('/researcher_production_novo_csv_db.csv')
def researcher_production_novo_csv_db_csv():
    powerBI.researcher_production_novo_csv_db()
    file_name = 'researcher_production_novo_csv_db.csv'
    file_path = os.path.join(STORAGE_PATH, file_name)
    return FileResponse(file_path, filename=file_name)


@router.get('/article_distinct_novo_csv_db.csv')
def article_distinct_novo_csv_db_csv():
    powerBI.article_distinct_novo_csv_db()
    file_name = 'article_distinct_novo_csv_db.csv'
    file_path = os.path.join(STORAGE_PATH, file_name)
    return FileResponse(file_path, filename=file_name)


@router.get('/production_distinct_novo_csv_db.csv')
def production_distinct_novo_csv_db_csv():
    powerBI.production_distinct_novo_csv_db()
    file_name = 'production_distinct_novo_csv_db.csv'
    file_path = os.path.join(STORAGE_PATH, file_name)
    return FileResponse(file_path, filename=file_name)


@router.get('/cimatec_graduate_program_researcher.csv')
def cimatec_graduate_program_researcher_csv():
    powerBI.cimatec_graduate_program_researcher()
    file_name = 'cimatec_graduate_program_researcher.csv'
    file_path = os.path.join(STORAGE_PATH, file_name)
    return FileResponse(file_path, filename=file_name)


@router.get('/cimatec_graduate_program.csv')
def cimatec_graduate_program_csv():
    powerBI.cimatec_graduate_program()
    file_name = 'cimatec_graduate_program.csv'
    file_path = os.path.join(STORAGE_PATH, file_name)
    return FileResponse(file_path, filename=file_name)


@router.get('/dim_departament.csv')
def dim_departament_csv():
    powerBI.dim_departament()
    file_name = 'dim_departament.csv'
    file_path = os.path.join(STORAGE_PATH, file_name)
    return FileResponse(file_path, filename=file_name)
