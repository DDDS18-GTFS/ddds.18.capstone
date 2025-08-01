FROM python:3.9-slim
WORKDIR /app
RUN pip install pandas psycopg2-binary requests pyarrow
COPY collect_and_load.py .
EXPOSE 8080
CMD ["python", "collect_and_load.py"]
