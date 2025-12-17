FROM python:3.12-bullseye

WORKDIR /app

COPY requirements.txt requirements.txt

RUN pip install -r requirements.txt

COPY sifec_base /usr/local/lib/python3.12/site-packages/sifec_base

COPY . .

EXPOSE 8000

ENTRYPOINT [ "fastapi", "run", "main.py" ]
