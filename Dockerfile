FROM python:3.6-alpine

WORKDIR /opt/orchestrator

RUN apk --no-cache add git

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY src/orchestrator.py .

CMD ["python", "orchestrator.py"]
