FROM python:3.12-slim
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
