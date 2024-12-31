from http import HTTPStatus

import httpx
from fastapi import FastAPI, Request, Response

from .config import settings

app = FastAPI()


@app.get('/')
def read_root():
    return {'message': 'Olá Mundo!'}


@app.middleware('http')
async def reverse_proxy(request: Request, call_next):
    response = await call_next(request)

    if response.status_code == HTTPStatus.NOT_FOUND:
        async with httpx.AsyncClient() as client:
            proxy_response = await client.request(
                method=request.method,
                url=f'{settings.PROXY_URL}{request.url.path}',
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
