FROM python:3.4-alpine
ADD ./code /code
WORKDIR /code
RUN pip install -r requirements
CMD ["python", "kafka-produce.py"]
