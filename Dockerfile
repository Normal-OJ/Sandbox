FROM python:alpine

WORKDIR /app

COPY . .
RUN pip install -r requirements.txt

# RUN addgroup -S --gid 1000 normal-oj && \
#     adduser -S --uid 1000 normal-oj normal-oj && \
#     addgroup docker && \ 
#     addgroup normal-oj docker
# USER normal-oj

CMD gunicorn -c gunicorn.conf.py app:app
