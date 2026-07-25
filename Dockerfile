FROM python:3.13-alpine

WORKDIR /app

# install dependencies
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

CMD ["python", "main.py"]
