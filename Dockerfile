FROM python:3.11-slim

# Definir diretorio de trabalho
WORKDIR /app

# Copiar arquivo de dependencias
COPY requirements.txt .

# Instalar dependencias
RUN pip install --no-cache-dir -r requirements.txt

# Copiar codigo fonte
COPY etl/ ./etl/

# Criar diretorios necessarios
RUN mkdir -p data/bronze data/silver data/gold logs

# Definir comando padrao
CMD ["python", "etl/main.py"]