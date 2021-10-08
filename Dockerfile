FROM python:3.8

WORKDIR /opt/app/

COPY ./requirements.txt /opt/requirements.txt

RUN pip install --no-cache-dir --upgrade -r /opt/requirements.txt

COPY ./src /opt/app/src

CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "80"]