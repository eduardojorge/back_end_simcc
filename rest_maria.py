import os

from langchain.memory import ChatMessageHistory
from langchain.prompts import PromptTemplate
from langchain_core.output_parsers import JsonOutputParser
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
from langchain_core.pydantic_v1 import BaseModel, Field
from langchain_openai import ChatOpenAI

from Dao import sgbdSQL as bd

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
