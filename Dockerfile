FROM python:3.11-slim

WORKDIR app/
COPY . .

RUN pip install -r requirements.txt

RUN python -m nltk.downloader stopwords

EXPOSE 8080
CMD python -m gunicorn --certfile=$CERT_FILE --keyfile=$KEY_FILE -b 0.0.0.0:8080 server:app --reload