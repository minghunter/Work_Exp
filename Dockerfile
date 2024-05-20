FROM python:3.9

WORKDIR /my-dp

COPY ./requirements.txt /my-dp/requirements.txt

RUN pip install --no-cache-dir --upgrade -r /my-dp/requirements.txt

COPY ./app /my-dp/app

COPY ./main.py /my-dp/main.py

COPY ./mongodb_cre.txt /my-dp/mongodb_cre.txt

CMD ["uvicorn", "main:app", "--host=0.0.0.0", "--port=80"]