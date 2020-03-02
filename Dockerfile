FROM python:alpine
ARG GID=1000
ARG UID=1000

WORKDIR /app

# setup normal user
RUN addgroup -S -g $GID normal-oj && \
    addgroup docker && \ 
    adduser -S -u $UID -G normal-oj normal-oj && \
    addgroup normal-oj docker
# copy and set ownership
COPY --chown=normal-oj:normal-oj . .
RUN chown normal-oj:normal-oj /app 
# install dependencies
RUN pip install -r requirements.txt
# switch to normal user
USER normal-oj

CMD gunicorn -c gunicorn.conf.py app:app
