FROM python:3.9-slim

WORKDIR /app

COPY . .

RUN pip install --no-cache-dir flask redis kafka-python

EXPOSE 5000

CMD ["python", "valute_converter.py"]
