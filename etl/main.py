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
    logger.info("===== INÃCIO DO PIPELINE =====")

    # Fase 1: Webscraping
    logger.info("Iniciando Fase 1: Descobrindo URLs dos menores arquivos")
    scraper = WebscrapingStone()
    menores = scraper.executar_webscraping_completo()
    if not menores:
        logger.error("Nenhum arquivo vÃ¡lido encontrado para download. Encerrando pipeline.")
        return
    logger.info(f"Fase 1 concluÃ­da. Arquivos para baixar: {list(menores.keys())}")

    arquivos_para_baixar = {k: v.url_completa for k, v in menores.items()}

    # Fase 2: Bronze
    logger.info("Iniciando Fase 2: Baixando arquivos selecionados")
    bronze = BronzeLayer()
    arquivos_baixados = bronze.process(arquivos_para_baixar)
    if not arquivos_baixados:
        logger.error("Falha ao baixar arquivos. Encerrando pipeline.")
        return
    logger.info(f"Fase 2 concluÃ­da. Arquivos baixados: {list(arquivos_baixados.keys())}")

    # Fase 3: Silver
    logger.info("Iniciando Fase 3: Processando e limpando dados")
    silver = SilverLayer()
    resultado_silver = silver.process(arquivos_baixados)
    logger.info("Fase 3 concluÃ­da. Resumo dos dados processados:")

    if 'empresas' in resultado_silver and not resultado_silver['empresas'].empty:
        logger.info(f"Empresas processadas: {len(resultado_silver['empresas'])}")
    else:
        logger.warning("Nenhum dado de empresas processado.")

    if 'socios' in resultado_silver and not resultado_silver['socios'].empty:
        logger.info(f"SÃ³cios processados: {len(resultado_silver['socios'])}")
    else:
        logger.warning("Nenhum dado de sÃ³cios processado.")

    # Fase 4: Gold
    logger.info("Iniciando Fase 4: ConsolidaÃ§Ã£o dos dados finais")
    gold = GoldLayer()
    resultado_gold = gold.process()
    if resultado_gold is not None and not resultado_gold.empty:
        logger.info(f"Gold layer criada com {len(resultado_gold)} registros.")
    else:
        logger.warning("Falha ao criar Gold layer.")

    logger.info("===== FIM DO PIPELINE =====")

if __name__ == "__main__":
    main()