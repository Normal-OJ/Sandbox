FROM python:alpine

WORKDIR /app

COPY . .
RUN pip install -r requirements.txt
CMD gunicorn -c gunicorn.conf.py app:app
