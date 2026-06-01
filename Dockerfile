FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app
ENV PYTHONPATH=/app

CMD ["uvicorn", "app.responder:app", "--host", "0.0.0.0", "--port", "8000"]
# Dockerfile para el servicio responder, que define la imagen base de Python 3.11 slim
# instala las dependencias necesarias desde requirements.txt, copia el código de la aplicación al contenedor
# y establece el comando para iniciar el servidor FastAPI usando Uvicorn en el puerto 8000.