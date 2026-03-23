FROM python:3.13-alpine

WORKDIR /app

# install dependencies
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

# SANDBOX_MODE: "push" (default, legacy Flask server) or "pull" (runner client)
ENV SANDBOX_MODE=push

CMD if [ "$SANDBOX_MODE" = "pull" ]; then \
      python runner_client.py; \
    else \
      gunicorn -c gunicorn.conf.py app:app; \
    fi
