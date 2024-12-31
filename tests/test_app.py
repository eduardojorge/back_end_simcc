from http import HTTPStatus

import httpx
import respx

from simcc import PROXY_URL


def test_root_route(client):
    response = client.get('/')
    assert response.status_code == HTTPStatus.OK
    assert response.json() == {'message': 'Olá Mundo!'}


@respx.mock
def test_reverse_proxy(client):
    URL = PROXY_URL + '/test'
    target_route = respx.get(URL).mock(
        return_value=httpx.Response(200, json={'message': 'Proxy working!'})
    )

    response = client.get('/test')

    assert target_route.called
    assert response.status_code == HTTPStatus.OK
    assert response.json() == {'message': 'Proxy working!'}


@respx.mock
def test_reverse_proxy_not_found(client):
    URL = PROXY_URL + '/not-found'
    target_route = respx.get(URL).mock(
        return_value=httpx.Response(404, json={'detail': 'Not Found'})
    )

    response = client.get('/not-found')

    assert target_route.called
    assert response.status_code == HTTPStatus.NOT_FOUND
    assert response.json() == {'detail': 'Not Found'}
