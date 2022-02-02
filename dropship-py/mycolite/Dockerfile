FROM tiangolo/uvicorn-gunicorn-fastapi:python3.7

COPY ./app /app

COPY ./requirements.txt /app/requirements.txt

RUN pip install -r /app/requirements.txt

CMD ["uvicorn", "main:app", "--reload"]
