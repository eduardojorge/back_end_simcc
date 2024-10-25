CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
create EXTENSION fuzzystrmatch;
create EXTENSION pg_trgm;
CREATE EXTENSION unaccent;
CREATE TYPE relationship AS ENUM ('COLABORADOR', 'PERMANENTE');
CREATE TABLE IF NOT EXISTS public.country (
    id uuid NOT NULL DEFAULT uuid_generate_v4(),
    name character varying NOT NULL,
    name_pt character varying NOT NULL,
    alpha_2_code character(2),
    alpha_3_code character(3),
    created_at timestamp without time zone NOT NULL DEFAULT now(),
    updated_at timestamp without time zone,
    deleted_at timestamp without time zone,
    CONSTRAINT "PK_bf6e37c231c4f4ea56dcd887269" PRIMARY KEY (id),
    CONSTRAINT "UQ_2c5aa339240c0c3ae97fcc9dc4c" UNIQUE (name),
    CONSTRAINT "UQ_69c6da9574151020d186279419f" UNIQUE (alpha_2_code),
    CONSTRAINT "UQ_9f88595b715818e292be3472256" UNIQUE (alpha_3_code),
    CONSTRAINT "UQ_f7c67d6e048708bb13b14a0bc1a" UNIQUE (name_pt)
);
CREATE TABLE IF NOT EXISTS public.state (
    id uuid NOT NULL DEFAULT uuid_generate_v4(),
    name character varying NOT NULL,
    abbreviation character(2),
    country_id uuid NOT NULL,
    created_at timestamp without time zone NOT NULL DEFAULT now(),
    updated_at timestamp without time zone,
    deleted_at timestamp without time zone,
    CONSTRAINT "PK_549ffd046ebab1336c3a8030a12" PRIMARY KEY (id),
    CONSTRAINT "UQ_a4925b2350673eb963998d27ec3" UNIQUE (abbreviation),
    CONSTRAINT "UQ_b2c4aef5929860729007ac32f6f" UNIQUE (name),
    CONSTRAINT "FKCountryState" FOREIGN KEY (country_id) REFERENCES public.country (id) MATCH SIMPLE ON UPDATE CASCADE ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS public.city (
    id uuid NOT NULL DEFAULT uuid_generate_v4(),
    name character varying NOT NULL,
    country_id uuid NOT NULL,
    state_id uuid,
    created_at timestamp without time zone NOT NULL DEFAULT now(),
    updated_at timestamp without time zone,
    deleted_at timestamp without time zone,
    CONSTRAINT "PK_b222f51ce26f7e5ca86944a6739" PRIMARY KEY (id),
    CONSTRAINT "FKCountryCity" FOREIGN KEY (country_id) REFERENCES public.country (id) MATCH SIMPLE ON UPDATE CASCADE ON DELETE CASCADE,
    CONSTRAINT "FKStateCity" FOREIGN KEY (state_id) REFERENCES public.state (id) MATCH SIMPLE ON UPDATE CASCADE ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS public.institution (
    id uuid NOT NULL DEFAULT uuid_generate_v4(),
    name character varying NOT NULL,
    acronym character varying(50),
    description character varying(5000),
    lattes_id character(12),
    cnpj character(14),
    image character varying,
    created_at timestamp without time zone NOT NULL DEFAULT now(),
    updated_at timestamp without time zone,
    deleted_at timestamp without time zone,
    latitude double precision,
    longitude double precision,
    CONSTRAINT "PK_f60ee4ff0719b7df54830b39087" PRIMARY KEY (id),
    CONSTRAINT "UQ_c50c675ba2bedbaff7192b0a30e" UNIQUE (acronym),
    CONSTRAINT "UQ_c9af99711dccbeb22b20b24cca8" UNIQUE (cnpj),
    CONSTRAINT "UQ_d218ad3566afa9e396f184fd7d5" UNIQUE (name)
);
CREATE TABLE IF NOT EXISTS public.periodical_magazine (
    id uuid NOT NULL DEFAULT uuid_generate_v4(),
    name character varying(600),
    issn character varying(20),
    qualis character varying(8),
    created_at timestamp without time zone NOT NULL DEFAULT now(),
    updated_at timestamp without time zone,
    deleted_at timestamp without time zone,
    jcr character varying(100),
    jcr_link character varying(200),
    CONSTRAINT "PK_35bb0df687d8879d763c1f3ae68" PRIMARY KEY (id)
);
CREATE TABLE IF NOT EXISTS public.great_area_expertise (
    id uuid NOT NULL DEFAULT uuid_generate_v4(),
    name character varying NOT NULL,
    created_at timestamp without time zone NOT NULL DEFAULT now(),
    updated_at timestamp without time zone,
    deleted_at timestamp without time zone,
    CONSTRAINT pk_id_great_area_expertise PRIMARY KEY (id)
);
CREATE TABLE IF NOT EXISTS public.area_expertise (
    id uuid NOT NULL DEFAULT uuid_generate_v4(),
    name character varying NOT NULL,
    created_at timestamp without time zone NOT NULL DEFAULT now(),
    updated_at timestamp without time zone,
    deleted_at timestamp without time zone,
    great_area_expertise_id uuid,
    CONSTRAINT "PK_44d189c8477ad880b9ec101d453" PRIMARY KEY (id),
    CONSTRAINT "FK_great_area_expertise" FOREIGN KEY (great_area_expertise_id) REFERENCES public.great_area_expertise (id) MATCH SIMPLE ON UPDATE CASCADE ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS public.sub_area_expertise (
    id uuid NOT NULL DEFAULT uuid_generate_v4(),
    name character varying NOT NULL,
    area_expertise_id uuid NOT NULL,
    created_at timestamp without time zone NOT NULL DEFAULT now(),
    updated_at timestamp without time zone,
    deleted_at timestamp without time zone,
    CONSTRAINT pk_id_sub_area_expertise PRIMARY KEY (id),
    CONSTRAINT sub_area_expertise_area_expertise_id_fkey FOREIGN KEY (area_expertise_id) REFERENCES public.area_expertise (id) MATCH SIMPLE ON UPDATE NO ACTION ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS public.area_specialty (
    id uuid NOT NULL DEFAULT uuid_generate_v4(),
    name character varying NOT NULL,
    sub_area_expertise_id uuid NOT NULL,
    created_at timestamp without time zone NOT NULL DEFAULT now(),
    updated_at timestamp without time zone,
    deleted_at timestamp without time zone,
    CONSTRAINT pk_id_area_specialty PRIMARY KEY (id),
    CONSTRAINT area_specialty_sub_area_expertise_id_fkey FOREIGN KEY (sub_area_expertise_id) REFERENCES public.sub_area_expertise (id) MATCH SIMPLE ON UPDATE NO ACTION ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS public.researcher (
    id uuid NOT NULL DEFAULT uuid_generate_v4(),
    name character varying NOT NULL,
    lattes_id character(16),
    lattes_10_id character(10),
    last_update timestamp without time zone NOT NULL DEFAULT now(),
    citations character varying,
    orcid character(31),
    abstract character varying(5000),
    abstract_en character varying(5000),
    other_information character varying(5000),
    city_id uuid,
    country_id uuid,
    has_image boolean NOT NULL DEFAULT false,
    docente boolean NOT NULL DEFAULT false,
    created_at timestamp without time zone NOT NULL DEFAULT now(),
    updated_at timestamp without time zone,
    deleted_at timestamp without time zone,
    qtt_publications integer,
    institution_id uuid,
    graduate_program character varying(255),
    graduation character varying(30),
    update_abstract boolean DEFAULT true,
    student boolean DEFAULT false,
    CONSTRAINT "PK_7b53850398061862ebe70d4ce44" PRIMARY KEY (id),
    CONSTRAINT "UQ_cd7166a27f090d19d4e985592db" UNIQUE (lattes_10_id),
    CONSTRAINT "UQ_fdf2bde0f46501e3e84ec154c32" UNIQUE (lattes_id),
    CONSTRAINT "FKCityResearcher" FOREIGN KEY (city_id) REFERENCES public.city (id) MATCH SIMPLE ON UPDATE CASCADE ON DELETE CASCADE,
    CONSTRAINT "FKCountryResearcher" FOREIGN KEY (country_id) REFERENCES public.country (id) MATCH SIMPLE ON UPDATE CASCADE ON DELETE CASCADE,
    CONSTRAINT "FKInstitutionResearcher" FOREIGN KEY (institution_id) REFERENCES public.institution (id) MATCH SIMPLE ON UPDATE CASCADE ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS public.researcher_address (
    id uuid NOT NULL DEFAULT uuid_generate_v4(),
    researcher_id uuid NOT NULL,
    created_at timestamp without time zone NOT NULL DEFAULT now(),
    updated_at timestamp without time zone,
    deleted_at timestamp without time zone,
    city character varying(50),
    organ character varying(255),
    unity character varying(255),
    institution character varying(255),
    public_place character varying(255),
    district character varying(255),
    cep character varying(255),
    mailbox character varying(255),
    fax character varying(20),
    url_homepage character varying(300),
    telephone character varying(20),
    country character varying(100),
    uf character varying(5),
    CONSTRAINT "PK_180e58d987170694c2c11424916" PRIMARY KEY (id),
    CONSTRAINT "FKAddressResearcher" FOREIGN KEY (researcher_id) REFERENCES public.researcher (id) MATCH SIMPLE ON UPDATE CASCADE ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS public.researcher_area_expertise (
    id uuid NOT NULL DEFAULT uuid_generate_v4(),
    researcher_id uuid NOT NULL,
    sub_area_expertise_id uuid NOT NULL,
    "order" integer,
    created_at timestamp without time zone NOT NULL DEFAULT now(),
    updated_at timestamp without time zone,
    deleted_at timestamp without time zone,
    area_expertise_id uuid,
    great_area_expertise_id uuid,
    area_specialty_id uuid,
    CONSTRAINT "PK_35338c2e178fa10e7b30966a4fc" PRIMARY KEY (id),
    CONSTRAINT "FKResearcherAreaExpertise" FOREIGN KEY (researcher_id) REFERENCES public.researcher (id) MATCH SIMPLE ON UPDATE CASCADE ON DELETE CASCADE,
    CONSTRAINT "FKSubAreaExpertise" FOREIGN KEY (sub_area_expertise_id) REFERENCES public.sub_area_expertise (id) MATCH SIMPLE ON UPDATE CASCADE ON DELETE CASCADE,
    CONSTRAINT "FkAreaExpertise" FOREIGN KEY (area_expertise_id) REFERENCES public.area_expertise (id) MATCH SIMPLE ON UPDATE CASCADE ON DELETE CASCADE,
    CONSTRAINT "FkAreaSpecialty" FOREIGN KEY (area_specialty_id) REFERENCES public.area_specialty (id) MATCH SIMPLE ON UPDATE CASCADE ON DELETE CASCADE,
    CONSTRAINT "FkGreatAreaExpertise" FOREIGN KEY (great_area_expertise_id) REFERENCES public.great_area_expertise (id) MATCH SIMPLE ON UPDATE CASCADE ON DELETE CASCADE
);
CREATE TYPE public.bibliographic_production_type_enum AS ENUM (
    'BOOK',
    'BOOK_CHAPTER',
    'ARTICLE',
    'WORK_IN_EVENT',
    'TEXT_IN_NEWSPAPER_MAGAZINE'
);
CREATE TABLE IF NOT EXISTS public.bibliographic_production (
    id uuid NOT NULL DEFAULT uuid_generate_v4(),
    title character varying(500) NOT NULL,
    title_en character varying(500),
    type bibliographic_production_type_enum NOT NULL,
    doi character varying,
    nature character varying(50),
    year character(4),
    country_id uuid,
    language character(2),
    means_divulgation character varying(20),
    homepage character varying,
    relevance boolean NOT NULL DEFAULT false,
    scientific_divulgation boolean DEFAULT false,
    created_at timestamp without time zone NOT NULL DEFAULT now(),
    updated_at timestamp without time zone,
    deleted_at timestamp without time zone,
    researcher_id uuid,
    authors character varying(1000),
    year_ integer,
    is_new boolean DEFAULT true,
    CONSTRAINT "PK_9c61219aee0513e9a1cf707a41a" PRIMARY KEY (id),
    CONSTRAINT "FKCountryResearcher" FOREIGN KEY (country_id) REFERENCES public.country (id) MATCH SIMPLE ON UPDATE CASCADE ON DELETE CASCADE,
    CONSTRAINT fk_researcher_id FOREIGN KEY (researcher_id) REFERENCES public.researcher (id) MATCH SIMPLE ON UPDATE CASCADE ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS public.software (
    id uuid NOT NULL DEFAULT uuid_generate_v4(),
    created_at timestamp without time zone NOT NULL DEFAULT now(),
    title character varying,
    platform character varying,
    goal character varying,
    environment character varying,
    availability character varying,
    financing_institutionc character varying,
    researcher_id uuid,
    year smallint,
    is_new boolean DEFAULT true,
    CONSTRAINT software_pkey PRIMARY KEY (id)
);
CREATE TABLE IF NOT EXISTS public.patent (
    id uuid NOT NULL DEFAULT uuid_generate_v4(),
    created_at timestamp without time zone NOT NULL DEFAULT now(),
    title character varying(400),
    category character varying(200),
    development_year character varying(10),
    details character varying(2500),
    researcher_id uuid,
    grant_date timestamp without time zone,
    deposit_date character varying(255),
    is_new boolean DEFAULT true,
    CONSTRAINT patent_pkey PRIMARY KEY (id)
);
CREATE TABLE IF NOT EXISTS public.research_report (
    id uuid NOT NULL DEFAULT uuid_generate_v4(),
    created_at timestamp without time zone NOT NULL DEFAULT now(),
    researcher_id uuid,
    title character varying(400),
    project_name character varying(255),
    financing_institutionc character varying(255),
    year smallint,
    is_new boolean DEFAULT true,
    CONSTRAINT research_report_pkey PRIMARY KEY (id),
    CONSTRAINT research_report_relation_1 FOREIGN KEY (researcher_id) REFERENCES public.researcher (id) MATCH SIMPLE ON UPDATE CASCADE ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS public.guidance (
    id uuid NOT NULL DEFAULT uuid_generate_v4(),
    created_at timestamp without time zone NOT NULL DEFAULT now(),
    researcher_id uuid,
    title character varying(400),
    nature character varying(255),
    oriented character varying(255),
    type character varying(255),
    status character varying(100),
    year smallint,
    is_new boolean DEFAULT true,
    CONSTRAINT guidance_pkey PRIMARY KEY (id)
);
CREATE TABLE IF NOT EXISTS public.brand (
    id uuid NOT NULL DEFAULT uuid_generate_v4(),
    created_at timestamp without time zone NOT NULL DEFAULT now(),
    title character varying(400),
    goal character varying(255),
    nature character varying(100),
    researcher_id uuid,
    year smallint,
    is_new boolean DEFAULT true,
    CONSTRAINT brand_pkey PRIMARY KEY (id)
);
CREATE TABLE IF NOT EXISTS public.participation_events (
    id uuid NOT NULL DEFAULT uuid_generate_v4(),
    created_at timestamp without time zone NOT NULL DEFAULT now(),
    title character varying(500),
    event_name character varying(500),
    nature character varying(30),
    form_participation character varying(30),
    type_participation character varying(30),
    researcher_id uuid,
    year smallint,
    is_new boolean DEFAULT true,
    CONSTRAINT participation_events_pkey PRIMARY KEY (id)
);
CREATE TABLE IF NOT EXISTS public.event_organization (
    id uuid NOT NULL DEFAULT uuid_generate_v4(),
    created_at timestamp without time zone NOT NULL DEFAULT now(),
    title character varying(500),
    promoter_institution character varying(500),
    nature character varying(30),
    researcher_id uuid,
    local character varying(500),
    duration_in_weeks smallint,
    year smallint,
    is_new boolean DEFAULT true,
    CONSTRAINT event_organization_pkey PRIMARY KEY (id)
);
CREATE TABLE IF NOT EXISTS public.bibliographic_production_article (
    id uuid NOT NULL DEFAULT uuid_generate_v4(),
    bibliographic_production_id uuid NOT NULL,
    periodical_magazine_id uuid NOT NULL,
    volume character varying(30),
    fascicle character varying(30),
    series character varying(30),
    start_page character varying(30),
    end_page character varying(30),
    place_publication character varying,
    created_at timestamp without time zone NOT NULL DEFAULT now(),
    updated_at timestamp without time zone,
    deleted_at timestamp without time zone,
    periodical_magazine_name character varying(600),
    issn character varying(20),
    qualis character varying(8) DEFAULT 'SQ'::character varying,
    jcr character varying(100),
    jcr_link character varying(200),
    CONSTRAINT "PK_3a53ca9c0bd82c629e7a14ef0f4" PRIMARY KEY (id),
    CONSTRAINT "FKPeriodicalMagazineArticle" FOREIGN KEY (periodical_magazine_id) REFERENCES public.periodical_magazine (id) MATCH SIMPLE ON UPDATE CASCADE ON DELETE CASCADE,
    CONSTRAINT "FKPublicationArticle" FOREIGN KEY (bibliographic_production_id) REFERENCES public.bibliographic_production (id) MATCH SIMPLE ON UPDATE CASCADE ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS public.bibliographic_production_book (
    id uuid NOT NULL DEFAULT uuid_generate_v4(),
    bibliographic_production_id uuid NOT NULL,
    isbn character(13),
    qtt_volume character varying(25),
    qtt_pages character varying(25),
    num_edition_revision character varying(25),
    num_series character varying(25),
    publishing_company character varying,
    publishing_company_city character varying,
    created_at timestamp without time zone NOT NULL DEFAULT now(),
    updated_at timestamp without time zone,
    deleted_at timestamp without time zone,
    CONSTRAINT "PK_818a520edae9528a6d586485d18" PRIMARY KEY (id),
    CONSTRAINT "FKPublicationBook" FOREIGN KEY (bibliographic_production_id) REFERENCES public.bibliographic_production (id) MATCH SIMPLE ON UPDATE CASCADE ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS public.bibliographic_production_book_chapter (
    id uuid NOT NULL DEFAULT uuid_generate_v4(),
    bibliographic_production_id uuid NOT NULL,
    book_title character varying,
    isbn character(13),
    start_page character varying(25),
    end_page character varying(25),
    qtt_volume character varying(25),
    organizers character varying(500),
    num_edition_revision character varying(25),
    num_series character varying(25),
    publishing_company character varying,
    publishing_company_city character varying,
    created_at timestamp without time zone NOT NULL DEFAULT now(),
    updated_at timestamp without time zone,
    deleted_at timestamp without time zone,
    CONSTRAINT "PK_ccc5964c28ffa1e316b8c0c821e" PRIMARY KEY (id),
    CONSTRAINT "FKPublicationBookChapter" FOREIGN KEY (bibliographic_production_id) REFERENCES public.bibliographic_production (id) MATCH SIMPLE ON UPDATE CASCADE ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS public.research_dictionary (
    research_dictionary_id uuid NOT NULL DEFAULT uuid_generate_v4(),
    term character varying(255),
    frequency integer DEFAULT 1,
    type_ character varying(30),
    CONSTRAINT research_dictionary_pkey PRIMARY KEY (research_dictionary_id),
    CONSTRAINT research_dictionary_term_type__key UNIQUE (term, type_)
);
CREATE TABLE IF NOT EXISTS public.graduate_program(
    graduate_program_id uuid NOT NULL DEFAULT uuid_generate_v4(),
    code VARCHAR(100),
    name VARCHAR(100) NOT NULL,
    area VARCHAR(100) NOT NULL,
    modality VARCHAR(100) NOT NULL,
    TYPE VARCHAR(100) NULL,
    rating VARCHAR(5),
    institution_id uuid NOT NULL,
    state character varying(4) DEFAULT 'BA'::character varying,
    city character varying(100) DEFAULT 'Salvador'::character varying,
    region character varying(100) DEFAULT 'Nordeste'::character varying,
    url_image VARCHAR(200) NULL,
    acronym character varying(100),
    description TEXT,
    visible bool DEFAULT FALSE,
    site TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (graduate_program_id),
    FOREIGN KEY (institution_id) REFERENCES institution (id)
);
CREATE TABLE IF NOT EXISTS public.graduate_program_researcher(
    graduate_program_id uuid NOT NULL,
    researcher_id uuid NOT NULL,
    year INT [],
    type_ relationship,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (graduate_program_id, researcher_id),
    FOREIGN KEY (researcher_id) REFERENCES researcher (id),
    FOREIGN KEY (graduate_program_id) REFERENCES graduate_program (graduate_program_id)
);
CREATE TABLE IF NOT EXISTS public.graduate_program_student(
    graduate_program_id uuid NOT NULL DEFAULT uuid_generate_v4(),
    researcher_id uuid NOT NULL DEFAULT uuid_generate_v4(),
    year INT [],
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (graduate_program_id, researcher_id, year),
    FOREIGN KEY (researcher_id) REFERENCES researcher (id),
    FOREIGN KEY (graduate_program_id) REFERENCES graduate_program (graduate_program_id)
);
CREATE TABLE IF NOT EXISTS public.JCR (
    rank character varying,
    journalName character varying,
    jcrYear character varying,
    abbrJournal character varying,
    issn character varying,
    eissn character varying,
    totalCites character varying,
    totalArticles character varying,
    citableItems character varying,
    citedHalfLife character varying,
    citingHalfLife character varying,
    jif2019 character varying,
    url_revista character varying
);
CREATE TABLE IF NOT EXISTS public.researcher_production (
    researcher_production_id uuid NOT NULL DEFAULT uuid_generate_v4(),
    researcher_id uuid NOT NULL,
    articles integer,
    book_chapters integer,
    book integer,
    work_in_event integer,
    patent integer,
    software integer,
    brand integer,
    great_area text,
    area_specialty text,
    city character varying(100),
    organ character varying(100),
    CONSTRAINT researcher_production_pkey PRIMARY KEY (researcher_production_id),
    CONSTRAINT researcher_production_researcher_id_fkey FOREIGN KEY (researcher_id) REFERENCES public.researcher (id) MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION
);
CREATE TABLE IF NOT EXISTS public.foment (
    id uuid NOT NULL DEFAULT uuid_generate_v4(),
    researcher_id uuid REFERENCES researcher(id) ON DELETE CASCADE,
    modality_code character varying(50),
    modality_name character varying(255),
    call_title character varying(255),
    category_level_code character varying(50),
    funding_program_name character varying(255),
    institute_name character varying(255),
    aid_quantity integer,
    scholarship_quantity integer
);
CREATE TABLE education (
    id UUID NOT NULL DEFAULT uuid_generate_v4(),
    researcher_id UUID NOT NULL,
    degree VARCHAR(255) NOT NULL,
    education_name VARCHAR(255),
    education_start INTEGER,
    education_end INTEGER,
    institution_id UUID,
    key_words VARCHAR(255),
    CONSTRAINT pk_education PRIMARY KEY (id),
    CONSTRAINT fk_researcher_education FOREIGN KEY (researcher_id) REFERENCES public.researcher (id),
    CONSTRAINT fk_institution_education FOREIGN KEY (institution_id) REFERENCES public.institution (id)
);
CREATE TABLE IF NOT EXISTS public.openalex_article (
    id uuid NOT NULL DEFAULT uuid_generate_v4(),
    article_id uuid NOT NULL,
    article_institution VARCHAR,
    issn VARCHAR,
    authors_institution VARCHAR,
    abstract TEXT,
    authors VARCHAR,
    language VARCHAR,
    citations_count SMALLINT,
    pdf VARCHAR,
    landing_page_url VARCHAR,
    keywords VARCHAR,
    CONSTRAINT "PK_FIXMEHELP" PRIMARY KEY (article_id)
);
CREATE TABLE IF NOT EXISTS public.openalex_researcher (
    researcher_id uuid,
    h_index integer,
    relevance_score double precision,
    works_count integer,
    cited_by_count integer,
    i10_index integer,
    scopus character varying(255),
    orcid character varying(255),
    openalex character varying(255),
    CONSTRAINT fk_researcher_op FOREIGN KEY (researcher_id) REFERENCES public.researcher (id) MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION
);
CREATE TABLE IF NOT EXISTS public.researcher_ind_prod (
    id uuid NOT NULL DEFAULT uuid_generate_v4(),
    researcher_id uuid NOT NULL,
    year integer NOT NULL,
    ind_prod_article numeric(10, 3),
    ind_prod_book numeric(10, 3),
    ind_prod_book_chapter numeric(10, 3),
    ind_prod_software numeric(10, 3),
    ind_prod_report numeric(10, 3),
    ind_prod_granted_patent numeric(10, 3),
    ind_prod_not_granted_patent numeric(10, 3),
    ind_prod_guidance numeric(10, 3),
    CONSTRAINT "PKRIndProd" PRIMARY KEY (researcher_id, year),
    CONSTRAINT "FKRIndProd" FOREIGN KEY (researcher_id) REFERENCES public.researcher (id) MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION
);
CREATE TABLE public.graduate_program_ind_prod (
    id uuid DEFAULT public.uuid_generate_v4() NOT NULL,
    graduate_program_id uuid NOT NULL,
    year integer NOT NULL,
    ind_prod_article numeric(10, 3),
    ind_prod_book numeric(10, 3),
    ind_prod_book_chapter numeric(10, 3),
    ind_prod_software numeric(10, 3),
    ind_prod_report numeric(10, 3),
    ind_prod_granted_patent numeric(10, 3),
    ind_prod_not_granted_patent numeric(10, 3),
    ind_prod_guidance numeric(10, 3)
);
CREATE TABLE IF NOT EXISTS research_group(
    id uuid DEFAULT public.uuid_generate_v4() NOT NULL,
    name character varying(200),
    institution character varying(200),
    first_leader character varying(200),
    first_leader_id uuid,
    second_leader character varying(200),
    second_leader_id uuid,
    area character varying(200),
    census int,
    start_of_collection character varying(200),
    end_of_collection character varying(200),
    group_identifier character varying(200),
    year int,
    institution_name character varying(200),
    category character varying(200)
);
CREATE TABLE IF NOT EXISTS ufmg_teacher (
    researcher_id uuid,
    matric character varying(40),
    inscUFMG character varying(40),
    nome character varying(200),
    genero character varying(40),
    situacao character varying(40),
    rt character varying(40),
    clas character varying(40),
    cargo character varying(40),
    classe character varying(40),
    ref character varying(40),
    titulacao character varying(40),
    entradaNaUFMG DATE,
    progressao DATE,
    semester character varying(6)
);
CREATE TABLE IF NOT EXISTS ufmg_technician (
    technician_id uuid PRIMARY KEY,
    matric INT UNIQUE,
    ins_ufmg VARCHAR(255),
    nome VARCHAR(255),
    genero VARCHAR(50),
    deno_sit VARCHAR(255),
    rt VARCHAR(255),
    classe VARCHAR(255),
    cargo VARCHAR(255),
    nivel VARCHAR(255),
    ref VARCHAR(255),
    titulacao VARCHAR(255),
    setor VARCHAR(255),
    detalhe_setor VARCHAR(255),
    dting_org DATE,
    data_prog DATE,
    semester character varying(6)
);
CREATE TABLE IF NOT EXISTS ufmg_departament (
    dep_id VARCHAR(10),
    org_cod VARCHAR(3),
    dep_nom VARCHAR(100),
    dep_des VARCHAR(500),
    dep_email VARCHAR(100),
    dep_site VARCHAR(100),
    dep_sigla VARCHAR(30),
    dep_tel VARCHAR(20),
    img_data BYTEA,
    PRIMARY KEY (dep_id)
);
CREATE TABLE IF NOT EXISTS ufmg_departament_technician (
    dep_id character varying(10),
    technician_id uuid,
    PRIMARY KEY (dep_id, technician_id),
    FOREIGN KEY (dep_id) REFERENCES ufmg_departament (dep_id),
    FOREIGN KEY (technician_id) REFERENCES ufmg_technician (technician_id)
);
CREATE TABLE IF NOT EXISTS departament_researcher (
    dep_id VARCHAR(20),
    researcher_id uuid NOT NULL,
    PRIMARY KEY (dep_id, researcher_id),
    FOREIGN KEY (dep_id) REFERENCES ufmg_departament (dep_id),
    FOREIGN KEY (researcher_id) REFERENCES researcher (id)
);
CREATE TABLE incite_graduate_program(
    incite_graduate_program_id uuid NOT NULL DEFAULT uuid_generate_v4(),
    name VARCHAR(255) NOT NULL,
    description VARCHAR(500) NULL,
    link VARCHAR(500) NULL,
    institution_id uuid NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    visible bool DEFAULT FALSE,
    PRIMARY KEY (incite_graduate_program_id),
    FOREIGN KEY (institution_id) REFERENCES institution (id)
);
CREATE TABLE incite_graduate_program_researcher(
    incite_graduate_program_id uuid NOT NULL DEFAULT uuid_generate_v4(),
    researcher_id uuid NOT NULL DEFAULT uuid_generate_v4(),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (incite_graduate_program_id, researcher_id),
    FOREIGN KEY (researcher_id) REFERENCES researcher (id),
    FOREIGN KEY (incite_graduate_program_id) REFERENCES incite_graduate_program (incite_graduate_program_id)
);
CREATE TABLE research_lines(
    id uuid NOT NULL DEFAULT uuid_generate_v4(),
    research_group_id uuid,
    title TEXT,
    objective TEXT,
    keyword VARCHAR(510),
    group_identifier VARCHAR(510),
    year INT,
    predominant_major_area VARCHAR(510),
    predominant_area VARCHAR(510)
);
CREATE TABLE research_project (
    id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
    researcher_id uuid NOT NULL REFERENCES public.researcher(id),
    start_year INT,
    end_year INT,
    agency_code VARCHAR(255),
    agency_name VARCHAR(255),
    project_name TEXT,
    status VARCHAR(255),
    nature VARCHAR(255),
    number_undergraduates INT DEFAULT 0,
    number_specialists INT DEFAULT 0,
    number_academic_masters INT DEFAULT 0,
    number_phd INT DEFAULT 0,
    description TEXT
);
CREATE TABLE research_project_components (
    project_id uuid NOT NULL REFERENCES public.research_project(id),
    name VARCHAR(255),
    lattes_id VARCHAR(255),
    citations VARCHAR
);
CREATE TABLE research_project_foment (
    project_id uuid NOT NULL REFERENCES public.research_project(id),
    agency_name VARCHAR(255),
    agency_code VARCHAR(255),
    nature VARCHAR(255)
);
CREATE TABLE research_project_production (
    project_id uuid NOT NULL REFERENCES public.research_project(id),
    title TEXT,
    type VARCHAR(255)
);
CREATE TABLE IF NOT EXISTS public.technical_work (
    id uuid NOT NULL DEFAULT uuid_generate_v4() PRIMARY KEY,
    researcher_id uuid NOT NULL,
    country VARCHAR,
    title TEXT NOT NULL,
    nature VARCHAR,
    funding_institution VARCHAR,
    duration INT,
    year INT,
    CONSTRAINT fk_researcher
        FOREIGN KEY (researcher_id) REFERENCES public.researcher(id)
);
CREATE TABLE IF NOT EXISTS public.technical_work_presentation (
    id uuid NOT NULL DEFAULT uuid_generate_v4() PRIMARY KEY,
    researcher_id uuid NOT NULL,
    country VARCHAR,
    title TEXT NOT NULL,
    nature VARCHAR,
    year INT,
	event_name VARCHAR,
	promoting_institution VARCHAR,
    CONSTRAINT fk_researcher
        FOREIGN KEY (researcher_id) REFERENCES public.researcher(id)
);
CREATE TABLE IF NOT EXISTS public.technical_work_program (
    id uuid NOT NULL DEFAULT uuid_generate_v4() PRIMARY KEY,
    researcher_id uuid NOT NULL,
    country VARCHAR,
    title TEXT NOT NULL,
    nature VARCHAR,
    year INT,
	theme VARCHAR,
    CONSTRAINT fk_researcher
        FOREIGN KEY (researcher_id) REFERENCES public.researcher(id)
);
CREATE TABLE IF NOT EXISTS public.technological_product (
    id uuid NOT NULL DEFAULT uuid_generate_v4() PRIMARY KEY,
    researcher_id uuid NOT NULL,
    country VARCHAR,
    title TEXT NOT NULL,
    nature VARCHAR,
	type VARCHAR,
    year INT,
    CONSTRAINT fk_researcher
        FOREIGN KEY (researcher_id) REFERENCES public.researcher(id)
);
CREATE TABLE IF NOT EXISTS public.didactic_material (
    id uuid NOT NULL DEFAULT uuid_generate_v4() PRIMARY KEY,
    researcher_id uuid NOT NULL,
    title TEXT NOT NULL,
    country VARCHAR,
    nature VARCHAR,
	description TEXT,
    year INT,
    CONSTRAINT fk_researcher
        FOREIGN KEY (researcher_id) REFERENCES public.researcher(id)
);
CREATE TABLE IF NOT EXISTS public.artistic_production (
    id uuid NOT NULL DEFAULT uuid_generate_v4() PRIMARY KEY,
    researcher_id uuid NOT NULL,
    title TEXT NOT NULL,
    year INT,
    CONSTRAINT fk_researcher
        FOREIGN KEY (researcher_id) REFERENCES public.researcher(id)
);
CREATE SCHEMA IF NOT EXISTS embeddings;
CREATE EXTENSION vector;
CREATE TABLE IF NOT EXISTS embeddings.abstract (
    id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
    reference_id uuid REFERENCES public.researcher(id),
    embeddings vector,
    price numeric(20, 18)
);
CREATE TABLE IF NOT EXISTS embeddings.article (
    id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
    reference_id uuid REFERENCES public.bibliographic_production(id),
    embeddings vector,
    price numeric(20, 18)
);
CREATE TABLE IF NOT EXISTS embeddings.article_abstract (
    id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
    reference_id uuid REFERENCES public.openalex_article(article_id),
    embeddings vector,
    price numeric(20, 18)
);
CREATE TABLE IF NOT EXISTS embeddings.book (
    id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
    reference_id uuid REFERENCES public.bibliographic_production(id),
    embeddings vector,
    price numeric(20, 18)
);
CREATE TABLE IF NOT EXISTS embeddings.event (
    id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
    reference_id uuid REFERENCES public.bibliographic_production(id),
    embeddings vector,
    price numeric(20, 18)
);
CREATE TABLE IF NOT EXISTS embeddings.patent (
    id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
    reference_id uuid REFERENCES public.patent(id),
    embeddings vector,
    price numeric(20, 18)
);
CREATE INDEX ON researcher USING gin (name gin_trgm_ops);
CREATE INDEX ON researcher USING gin (abstract gin_trgm_ops);
CREATE INDEX ON researcher USING gin (abstract_en gin_trgm_ops);
CREATE INDEX ON great_area_expertise USING gin (name gin_trgm_ops);
CREATE INDEX ON area_expertise USING gin (name gin_trgm_ops);
CREATE INDEX ON periodical_magazine USING gin (name gin_trgm_ops);