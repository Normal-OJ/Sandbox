FROM python:3.13-alpine

WORKDIR /app

# install dependencies
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

# Make logs directory exist
RUN mkdir -p logs

CMD ["python", "main.py"]
