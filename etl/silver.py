import os
import logging
import zipfile
from pathlib import Path
from typing import Dict
import pandas as pd
import numpy as np

# Garantir que a pasta 'logs' existe na raiz do projeto
log_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'logs'))
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(log_dir, 'silver.log')),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class SilverLayer:
    def __init__(self, bronze_path: str = None, silver_path: str = None):
        root_dir = Path(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
        if bronze_path is None:
            bronze_path = root_dir / "data" / "bronze"
        if silver_path is None:
            silver_path = root_dir / "data" / "silver"

        self.bronze_path = Path(bronze_path)
        self.silver_path = Path(silver_path)
        self.silver_path.mkdir(parents=True, exist_ok=True)
        logger.info(f"SilverLayer initialized. Path: {self.silver_path}")

    def validar_cnpj(self, cnpj_series):
        cnpj_series = cnpj_series.fillna('')
        cnpj_series = cnpj_series.astype(str).str.extract(r'(\d{8})')[0]
        cnpj_validos = cnpj_series.notnull().sum()
        total = len(cnpj_series)
        percentual = (cnpj_validos / total * 100) if total > 0 else 0
        logger.info(f"ValidaÃ§Ã£o CNPJ: {cnpj_validos}/{total} ({percentual:.1f}%) vÃ¡lidos")
        return percentual > 80

    def validar_tipo_socio(self, tipo_socio_series):
        tipo_socio_series = tipo_socio_series.fillna('')
        tipos_validos = tipo_socio_series.isin(['1', '2', '3']).sum()
        total = len(tipo_socio_series)
        percentual = (tipos_validos / total * 100) if total > 0 else 0
        logger.info(f"ValidaÃ§Ã£o TIPO_SOCIO: {tipos_validos}/{total} ({percentual:.1f}%) vÃ¡lidos")
        return percentual > 50

    def aplicar_tipos_empresas(self, df):
        logger.info("Aplicando tipos Ã s empresas")
        try:
            df['cnpj'] = df['cnpj'].astype(str)
            df['razao_social'] = df['razao_social'].astype(str)
            df['natureza_juridica'] = pd.to_numeric(df['natureza_juridica'], errors='coerce').astype('Int64')
            df['qualificacao_responsavel'] = pd.to_numeric(df['qualificacao_responsavel'], errors='coerce').astype('Int64')
            df['capital_social'] = df['capital_social'].str.replace(',', '.', regex=False)
            df['capital_social'] = pd.to_numeric(df['capital_social'], errors='coerce').astype(float)
            df['cod_porte'] = df['cod_porte'].astype(str)
            logger.info("Tipos Ã s empresas aplicados com sucesso")
        except Exception as e:
            logger.error(f"Erro ao aplicar tipos Ã s empresas: {e}")
        return df

    def aplicar_tipos_socios(self, df):
        logger.info("Aplicando tipos aos sÃ³cios")
        try:
            df['cnpj'] = df['cnpj'].astype(str)
            df['tipo_socio'] = pd.to_numeric(df['tipo_socio'], errors='coerce').astype('Int64')
            df['nome_socio'] = df['nome_socio'].astype(str)
            df['documento_socio'] = df['documento_socio'].astype(str)
            df['codigo_qualificacao'] = df['codigo_qualificacao'].astype(str)
            logger.info("Tipos aos sÃ³cios aplicados com sucesso")
        except Exception as e:
            logger.error(f"Erro ao aplicar tipos aos sÃ³cios: {e}")
        return df

    def processar_arquivo_empresas(self, zip_path):
        cols = ["cnpj", "razao_social", "natureza_juridica", "qualificacao_responsavel", "capital_social", "cod_porte"]
        try:
            with zipfile.ZipFile(zip_path, 'r') as z:
                for filename in z.namelist():
                    # Pula diretÃ³rios e arquivos muito pequenos
                    if filename.endswith('/') or z.getinfo(filename).file_size < 100:
                        continue
                    
                    logger.info(f"Tentando processar arquivo empresas: {filename}")
                    with z.open(filename) as f:
                        # Tenta ler as primeiras linhas para detectar se Ã© CSV vÃ¡lido
                        try:
                            df_test = pd.read_csv(f, sep=';', header=None, names=cols, dtype=str, encoding='latin1', nrows=10)
                            if len(df_test.columns) >= 6:  # Se tem as colunas esperadas de empresas
                                f.seek(0)  # volta ao inÃ­cio do arquivo
                                df = pd.read_csv(f, sep=';', header=None, names=cols, dtype=str, encoding='latin1')
                                df = self.aplicar_tipos_empresas(df)
                                if self.validar_cnpj(df["cnpj"]):
                                    logger.info("ValidaÃ§Ã£o CNPJ passou para empresas")
                                else:
                                    logger.warning("Problemas de validaÃ§Ã£o CNPJ para empresas")
                                df.to_parquet(self.silver_path / "empresas_silver.parquet", index=False)
                                logger.info(f"Empresas salvas em {self.silver_path / 'empresas_silver.parquet'} com {len(df)} registros")
                                return df
                        except Exception as e:
                            logger.warning(f"Arquivo {filename} nÃ£o Ã© CSV vÃ¡lido para empresas: {e}")
                            continue
            
            logger.warning("Nenhum arquivo de empresas processado")
            return pd.DataFrame()
        except Exception as e:
            logger.error(f"Erro processando arquivo empresas: {e}")
            return pd.DataFrame()

    def processar_arquivo_socios(self, zip_path):
        try:
            with zipfile.ZipFile(zip_path, 'r') as z:
                for filename in z.namelist():
                    # Pula diretÃ³rios e arquivos muito pequenos
                    if filename.endswith('/') or z.getinfo(filename).file_size < 100:
                        continue
                    
                    logger.info(f"Tentando processar arquivo socios: {filename}")
                    with z.open(filename) as f:
                        # Tenta ler as primeiras linhas para detectar se Ã© CSV vÃ¡lido
                        try:
                            df_test = pd.read_csv(f, sep=';', header=None, dtype=str, encoding='latin1', nrows=10)
                            if df_test.shape[1] >= 5:  # Se tem pelo menos 5 colunas
                                f.seek(0)  # volta ao inÃ­cio do arquivo
                                df = pd.read_csv(f, sep=';', header=None, dtype=str, encoding='latin1')
                                df = df.iloc[:, :5]  # pegar as cinco primeiras colunas
                                df.columns = ["cnpj", "tipo_socio", "nome_socio", "documento_socio", "codigo_qualificacao"]
                                df = self.aplicar_tipos_socios(df)
                                if self.validar_cnpj(df["cnpj"]) and self.validar_tipo_socio(df["tipo_socio"]):
                                    logger.info("ValidaÃ§Ã£o passou para socios")
                                else:
                                    logger.warning("Problema na validacao para socios")
                                df.to_parquet(self.silver_path / "socios_silver.parquet", index=False)
                                logger.info(f"Socios salvos em {self.silver_path / 'socios_silver.parquet'} com {len(df)} registros")
                                return df
                            else:
                                logger.warning(f"Arquivo {filename} tem menos de 5 colunas")
                        except Exception as e:
                            logger.warning(f"Arquivo {filename} nÃ£o Ã© CSV vÃ¡lido para socios: {e}")
                            continue
            
            logger.warning("Nenhum arquivo de socios processado")
            return pd.DataFrame()
        except Exception as e:
            logger.error(f"Erro processando arquivo socios: {e}")
            return pd.DataFrame()

    def process(self, arquivos: Dict[str, Path]):
        resultados = {}
        if "empresas" in arquivos:
            resultados["empresas"] = self.processar_arquivo_empresas(arquivos["empresas"])
        else:
            resultados["empresas"] = pd.DataFrame()
        if "socios" in arquivos:
            resultados["socios"] = self.processar_arquivo_socios(arquivos["socios"])
        else:
            resultados["socios"] = pd.DataFrame()
        return resultados

if __name__ == "__main__":
    from pathlib import Path
    import os
    root_dir = Path(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
    exemplos = {
        "empresas": root_dir / "data" / "bronze" / "empresas.zip",
        "socios": root_dir / "data" / "bronze" / "socios.zip"
    }
    silver = SilverLayer()
    result = silver.process(exemplos)
    if not result["empresas"].empty:
        print("Empresas - tipos finais:")
        print(result["empresas"].dtypes)
        print(f"Empresas - registros: {len(result['empresas'])}")
    if not result["socios"].empty:
        print("Socios - tipos finais:")
        print(result["socios"].dtypes)
        print(f"Socios - registros: {len(result['socios'])}")