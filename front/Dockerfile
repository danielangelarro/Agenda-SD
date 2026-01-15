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

# Copiar código del cliente
COPY client/ /app/

# Exponer puerto para Streamlit
EXPOSE 8501

# Ajustes recomendados para contenedor
ENV STREAMLIT_SERVER_ENABLECORS=false \
    STREAMLIT_SERVER_ENABLEXsSRFProtection=false \
    STREAMLIT_BROWSER_GATHER_USAGE_STATS=false \
    STREAMLIT_SERVER_ADDRESS=0.0.0.0

# Comando para ejecutar el cliente
CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]