from http import HTTPStatus

import httpx
from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware

from simcc.config import settings
from simcc.routers import (
    ConecteeRouter,
    GraduateProgramRouter,
    PowerBIRouter,
    ResearcherRouter,
)

app = FastAPI(root_path=settings.ROOT_PATH)

app.include_router(
    GraduateProgramRouter.router,
    prefix='/v2/graduate_program',
    tags=['Graduate Program'],
)

app.include_router(
    ResearcherRouter.router,
    tags=['Researcher'],
)
app.include_router(
    PowerBIRouter.router,
    tags=['PowerBI Data'],
)

app.include_router(
    ConecteeRouter.router,
    prefix='/ufmg',
    tags=['Conectee'],
)


PROXY_URL = settings.PROXY_URL


@app.get('/')
def read_root():
    return {'message': 'Olá Mundo!'}


app.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_methods=['*'],
    allow_headers=['*'],
    allow_credentials=True,
)


@app.middleware('http')
async def reverse_proxy(request: Request, call_next):
    response = await call_next(request)

    if response.status_code == HTTPStatus.NOT_FOUND:
        async with httpx.AsyncClient() as client:
            proxy_response = await client.request(
                method=request.method,
                url=f'{PROXY_URL}{request.url.path}',
                params=request.query_params,
                headers=dict(request.headers),
                content=await request.body(),
            )
            return Response(
                content=proxy_response.content,
                status_code=proxy_response.status_code,
                headers=dict(proxy_response.headers),
            )
    return response