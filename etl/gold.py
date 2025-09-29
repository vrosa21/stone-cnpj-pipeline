import os
import logging
from pathlib import Path
import pandas as pd
import pyarrow.parquet as pq

# Garantir que a pasta 'logs' existe na raiz do projeto (uma pasta acima da pasta atual)
log_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'logs'))
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(log_dir, 'gold.log')),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class GoldLayer:
    def __init__(self, bronze_path=None, silver_path=None, gold_path=None, batch_size=100000):
        root_dir = Path(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
        self.bronze_path = Path(bronze_path) if bronze_path else root_dir / "data" / "bronze"
        self.silver_path = Path(silver_path) if silver_path else root_dir / "data" / "silver"
        self.gold_path = Path(gold_path) if gold_path else root_dir / "data" / "gold"
        self.gold_path.mkdir(parents=True, exist_ok=True)
        self.batch_size = batch_size
        logger.info(f"GoldLayer initialized. Path: {self.gold_path}")

    def carregar_parquet_em_batches(self, parquet_path):
        logger.info(f"Carregando parquet em batches: {parquet_path}")
        table = pq.ParquetFile(str(parquet_path))
        dfs = []
        for batch in table.iter_batches(batch_size=self.batch_size):
            df_batch = batch.to_pandas()
            dfs.append(df_batch)
            logger.info(f"Batch carregado com {len(df_batch)} registros")
        df = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
        return df

    def carregar_dados(self):
        empresas_file = self.silver_path / "empresas_silver.parquet"
        socios_file = self.silver_path / "socios_silver.parquet"

        if not empresas_file.exists() or not socios_file.exists():
            logger.warning("Arquivos Parquet da camada Silver não foram encontrados")
            return pd.DataFrame(), pd.DataFrame()

        df_empresas = self.carregar_parquet_em_batches(empresas_file)
        df_socios = self.carregar_parquet_em_batches(socios_file)

        logger.info(f"Empresas carregadas: {len(df_empresas)} registros")
        logger.info(f"Socios carregados: {len(df_socios)} registros")

        return df_empresas, df_socios

    def process(self):
        logger.info("Processando camada Gold")

        df_empresas, df_socios = self.carregar_dados()
        if df_empresas.empty or df_socios.empty:
            logger.warning("Dados insuficientes para criar dataset final.")
            return pd.DataFrame()

        qtde_socios = df_socios.groupby('cnpj')['documento_socio'].nunique().reset_index()
        qtde_socios.rename(columns={'documento_socio': 'qtde_socios'}, inplace=True)

        df_socios['is_estrangeiro'] = df_socios['documento_socio'] == '***999999**'
        flag_estrangeiros = df_socios.groupby('cnpj')['is_estrangeiro'].any().reset_index()
        flag_estrangeiros.rename(columns={'is_estrangeiro': 'flag_socio_estrangeiro'}, inplace=True)

        df_final = df_empresas[['cnpj', 'cod_porte']] \
            .merge(qtde_socios, on='cnpj', how='left') \
            .merge(flag_estrangeiros, on='cnpj', how='left')

        df_final['qtde_socios'] = df_final['qtde_socios'].fillna(0).astype(int)

        df_final['flag_socio_estrangeiro'] = df_final['flag_socio_estrangeiro'].astype(pd.BooleanDtype())
        df_final['flag_socio_estrangeiro'] = df_final['flag_socio_estrangeiro'].fillna(False)

        df_final['doc_alvo'] = (df_final['cod_porte'] == '03') & (df_final['qtde_socios'] > 1)

        df_final = df_final.astype({
            'cnpj': str,
            'qtde_socios': 'int64',
            'doc_alvo': 'bool'
        })

        df_final.drop(columns=['cod_porte'], inplace=True)

        file_gold = self.gold_path / "cnpj_gold.parquet"
        df_final.to_parquet(file_gold)
        logger.info(f"Gold salvo: {file_gold} ({len(df_final)} registros)")

        return df_final

if __name__ == "__main__":
    gold = GoldLayer()
    gold.process()