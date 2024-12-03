from fastapi import FastAPI

from simcc.routes import researcher_search

app = FastAPI()


@app.get("/")
def root():
    return {"message": "Api em funcionamento!"}


app.include_router(researcher_search.router)
