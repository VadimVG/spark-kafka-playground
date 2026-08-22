FROM python:3.12-slim

WORKDIR /event-simulator

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY event-simulator/ .

CMD ["python", "-u", "main.py"]