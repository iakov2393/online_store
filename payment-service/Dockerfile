FROM python:3.12

WORKDIR /app

COPY requirements.txt ./

RUN python -m pip install --upgrade pip && \
    pip install --default-timeout=1000 --retries 5 -r requirements.txt

COPY . .
#COPY bin/ /order/bin/

EXPOSE 8001

ENV PYTHONPATH /app
