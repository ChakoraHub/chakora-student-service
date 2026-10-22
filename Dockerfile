FROM python:3.9-slim
LABEL org.opencontainers.image.source=https://github.com/ChakoraHub/chakora-student-service
ENV PYTHONUNBUFFERED=1
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
EXPOSE 8001
CMD ["python", "-m", "uvicorn", "student_service:app", "--host", "0.0.0.0", "--port", "8001"]
