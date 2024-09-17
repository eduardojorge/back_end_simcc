FROM python:3.11-slim

WORKDIR app/
COPY . .

RUN pip install -r requirements.txt

RUN python -m nltk.downloader stopwords

EXPOSE 5001

CMD ["python" "-m" "gunicorn", "-b", "0.0.0.0:5001", "server:app", "--reload", "--log-level", "error", "--access-logfile", "-", "--error-logfile", "-", "--workers", "4"]