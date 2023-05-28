FROM python:3.11-slim-buster

WORKDIR /DB_Lab_3/flask

RUN pip install --upgrade pip
COPY requirements.txt /DB_Lab_3/flask/requirements.txt
RUN pip install -r requirements.txt

COPY . /DB_Lab_3/flask

CMD ["python", "app.py"]
CMD ["flask", "run", "--host", "0.0.0.0"]