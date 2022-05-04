FROM python:3.8-slim

WORKDIR /app

# install dependencies
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

CMD ["gunicorn", "-c", "gunicorn.conf.py", "app:create_app()"]
