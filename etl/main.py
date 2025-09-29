import os
import logging
from webscraping import WebscrapingStone
from bronze import BronzeLayer
from silver import SilverLayer
from gold import GoldLayer

# Criar pasta 'logs' na raiz do projeto para armazenar arquivos de log
root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
log_dir = os.path.join(root_dir, 'logs')
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

log_file = os.path.join(log_dir, 'pipeline.log')

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file, encoding="utf-8"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

def main():
    logger.info("===== INÍCIO DO PIPELINE =====")
    
    # Fase 1: Webscraping
    logger.info("Iniciando Fase 1: Descobrindo URLs dos menores arquivos")
    scraper = WebscrapingStone()
    menores = scraper.executar_webscraping_completo()
    
    if not menores:
        logger.error("Nenhum arquivo válido encontrado para download. Encerrando pipeline.")
        return False
    
    # Fase 2: Bronze Layer - Download
    logger.info("Iniciando Fase 2: Download dos arquivos (Bronze Layer)")
    bronze = BronzeLayer()
    sucesso_bronze = bronze.executar_downloads(menores)
    
    if not sucesso_bronze:
        logger.error("Falha na fase Bronze. Encerrando pipeline.")
        return False
    
    # Fase 3: Silver Layer - Processamento e limpeza
    logger.info("Iniciando Fase 3: Processamento e limpeza (Silver Layer)")
    silver = SilverLayer()
    sucesso_silver = silver.processar_todos_arquivos()
    
    if not sucesso_silver:
        logger.error("Falha na fase Silver. Encerrando pipeline.")
        return False
    
    # Fase 4: Gold Layer - Agregações finais
    logger.info("Iniciando Fase 4: Agregações e métricas finais (Gold Layer)")
    gold = GoldLayer()
    sucesso_gold = gold.processar_camada_gold()
    
    if not sucesso_gold:
        logger.error("Falha na fase Gold. Encerrando pipeline.")
        return False
    
    logger.info("===== PIPELINE EXECUTADO COM SUCESSO =====")
    return True

if __name__ == "__main__":
    try:
        sucesso = main()
        if sucesso:
            logger.info("Pipeline finalizado com sucesso!")
        else:
            logger.error("Pipeline finalizado com falhas.")
    except Exception as e:
        logger.exception(f"Erro não tratado no pipeline: {e}")
