FROM python:3.11-slim

# Evita preguntas en apt
ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Copiar requirements
COPY requirements.txt /app/

# Instalar dependencias
RUN pip install --no-cache-dir -r requirements.txt

# Copiar código del servidor
COPY server/ /app/

# Exponer puertos para API y WebSocket
EXPOSE 8000 8765

# Asegura carpeta para la DB
RUN mkdir -p /data
ENV DB_PATH=/data/agenda.db

# Comando para ejecutar el servidor
CMD ["python", "services/main.py"]