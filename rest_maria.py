import os
from Dao import sgbdSQL as bd
from langchain_openai import ChatOpenAI
from langchain.prompts import PromptTemplate
from langchain.memory import ChatMessageHistory
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
from langchain_core.output_parsers import JsonOutputParser
from langchain_core.pydantic_v1 import BaseModel, Field


maria = ChatOpenAI(model="gpt-3.5-turbo", api_key=os.environ["OPENAI_API_KEY"])


class TermTaxonomy(BaseModel):
    id: int = Field(description="Layer of taxonomy")
    term: str = Field(description="Term related to the upper layer of taxonomy.")
    branch: list = Field(
        description="A list wich other ther terms. 3 or 5 terms is the ideal."
    )


class BaseTaxonomy(BaseModel):
    id: int = Field(description="Layer of taxonomy")
    term: str = Field(description="Term related to the upper layer of taxonomy.")
    branch: list[TermTaxonomy] = Field(
        description="A list wich other ther terms. 3 or 5 terms is the ideal, the id from branches = actual layer of taxonomy + 1",
        min_items=3,
        max_items=5,
    )


def create_taxonomy(
    question: str = "Me retorne uma taxonomia sobre energias renovaveis.",
    term: str = None,
):

    if term:
        question = f"Me retorne uma taxonomia com 3 branchs de relacionados a {term}."
    parser = JsonOutputParser(pydantic_object=BaseTaxonomy)
    prompt = PromptTemplate(
        template="""Você é um chatbot chamado Maria e ajuda pesquisadores que 
                estão acessando o SIMCC, o Sistema de mapeamento de competencias da Bahia, 
                com duvidas sobre produções academicas. Responda à consulta do usuário:\n{format_instructions}\n{question}\n""",
        input_variables=["question"],
        partial_variables={"format_instructions": parser.get_format_instructions()},
    )

    chain = prompt | maria | parser

    # for json_fragment in chain.stream({"question": question}):
    #     yield json_fragment
    return chain.invoke({"question": question})


def chating_maria():

    demo_ephemeral_chat_history = ChatMessageHistory()

    prompt = ChatPromptTemplate.from_messages(
        [
            (
                "system",
                """
            Você é um chatbot chamado Maria e ajuda pesquisadores que 
            estão acessando o SIMCC, o Sistema de mapeamento de competencias da Bahia, 
            com duvidas sobre produções academicas.
            """,
            ),
            MessagesPlaceholder(variable_name="messages"),
        ]
    )
    chain = prompt | maria

    while True:
        message = str(input("User: "))
        demo_ephemeral_chat_history.add_user_message(message)
        response = chain.invoke({"messages": demo_ephemeral_chat_history.messages})
        demo_ephemeral_chat_history.add_ai_message(response)
        print(response)


def read_json(json: dict, terms_list: list = list()) -> list:
    terms_list.append(json["term"].split(" "))
    for json_part in json["branch"]:
        read_json(json_part, terms_list)

    if json["id"] == 1:
        return terms_list


json_Biblioteca = {
    "id": 1,
    "term": "Criação de Jogos",
    "branch": [
        {
            "id": 2,
            "term": "Game Design",
            "branch": [
                {"id": 3, "term": "Mecânicas de Jogo", "branch": []},
                {"id": 4, "term": "Narrativa Interativa", "branch": []},
                {"id": 5, "term": "Prototipagem", "branch": []},
            ],
        },
        {
            "id": 6,
            "term": "Desenvolvimento de Jogos",
            "branch": [
                {"id": 7, "term": "Programação", "branch": []},
                {"id": 8, "term": "Gráficos e Animação", "branch": []},
                {"id": 9, "term": "Áudio", "branch": []},
            ],
        },
        {
            "id": 10,
            "term": "Testes e Qualidade",
            "branch": [
                {"id": 11, "term": "Testes Funcionais", "branch": []},
                {"id": 12, "term": "Testes de Usabilidade", "branch": []},
                {"id": 13, "term": "Testes de Performance", "branch": []},
            ],
        },
        {
            "id": 14,
            "term": "Marketing e Distribuição",
            "branch": [
                {"id": 15, "term": "Estratégias de Marketing", "branch": []},
                {"id": 16, "term": "Plataformas de Distribuição", "branch": []},
            ],
        },
        {
            "id": 17,
            "term": "Comunidade e Feedback",
            "branch": [
                {"id": 18, "term": "Engajamento da Comunidade", "branch": []},
                {"id": 19, "term": "Análise de Feedback", "branch": []},
            ],
        },
    ],
}
