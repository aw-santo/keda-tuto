FROM python:3.13-slim
ENV PYTHONUNBUFFERED=1
RUN pip install --no-cache-dir --only-binary=:all: confluent-kafka==2.15.0
WORKDIR /app
COPY consumer.py producer.py /app/
USER 65532:65532
STOPSIGNAL SIGTERM
ENTRYPOINT ["python"]
CMD ["/app/consumer.py"]
