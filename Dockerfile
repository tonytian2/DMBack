FROM python:alpine

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 4999

CMD flask --app main run --host=0.0.0.0 -p 4999 --debug