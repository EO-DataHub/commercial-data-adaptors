FROM python:3.12-slim@sha256:5e2bdb1b0d542d5039cffb8181c65930093949194a65c365dfb1b0fe13a4fa5b
ENV PYTHONUNBUFFERED 1
ENV PYTHONPATH "/app"

RUN apt-get update
RUN python3 -m pip install --upgrade pip

WORKDIR /app

COPY ../requirements.txt /app
RUN python -m pip install -r requirements.txt 

COPY ../common /app/common
COPY ../airbus_optical_adaptor /app/airbus_optical_adaptor
ENTRYPOINT ["python", "-m", "airbus_optical_adaptor"]
