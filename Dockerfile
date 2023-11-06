FROM python:3.9.8-slim-buster

WORKDIR /usr/src/app

RUN pip install --upgrade pip
COPY requirements.txt /usr/src/app/requirements.txt
RUN pip install -r requirements.txt

COPY . /usr/src/app/

CMD ["python", "main.py"]
