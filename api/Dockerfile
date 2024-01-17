FROM python:3.11-slim
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt
EXPOSE 8000
CMD ["uvicorn","main:app","--host", "0.0.0.0", "--reload"]
