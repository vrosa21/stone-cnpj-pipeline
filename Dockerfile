FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY etl/ ./etl/

# Cria as pastas de dados em um RUN separado
RUN mkdir -p data/bronze data/silver data/gold

# Cria a pasta de logs em outro RUN
RUN mkdir -p logs

COPY dashboard/ ./dashboard/

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

CMD ["streamlit", "run", "dashboard/app.py", "--server.address", "0.0.0.0"]
