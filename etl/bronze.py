import os
import logging
import requests
from pathlib import Path
from typing import Dict, Optional

# Garante que pasta logs existe, considerando execuÃ§Ã£o dentro da pasta etl
log_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'logs'))
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(log_dir, 'bronze.log')),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class BronzeLayer:
    def __init__(self, bronze_path: Optional[str] = None):
        if bronze_path is None:
            # Pega a raiz do projeto (uma pasta acima de onde este script estÃ¡)
            root_dir = Path(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
            bronze_path = root_dir / "data" / "bronze"
        self.bronze_path = Path(bronze_path)
        self.bronze_path.mkdir(parents=True, exist_ok=True)
        logger.info(f"BronzeLayer initialized. Path: {self.bronze_path}")

    def download_file(self, url: str, filename: str) -> Optional[Path]:
        filepath = self.bronze_path / filename
        logger.info(f"Starting download of {filename} from {url}")
        try:
            resp = requests.get(url, stream=True)
            resp.raise_for_status()
            with open(filepath, 'wb') as f:
                for chunk in resp.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            size_mb = filepath.stat().st_size / (1024 * 1024)
            logger.info(f"Download completed: {filepath} ({size_mb:.2f} MB)")
            return filepath
        except Exception as e:
            logger.error(f"Failed to download {filename}: {e}")
            return None

    def process(self, urls_to_download: Dict[str, str]) -> Dict[str, Path]:
        resultados = {}
        for tipo, url in urls_to_download.items():
            filename = f"{tipo}.zip"
            caminho = self.download_file(url, filename)
            if caminho:
                resultados[tipo] = caminho
            else:
                logger.error(f"Download failed for file {tipo} at {url}")
        return resultados


if __name__ == "__main__":
    exemplo_urls = {
        "empresas": "https://arquivos.receitafederal.gov.br/cnpj/dados/Empresas0.zip",
        "socios": "https://arquivos.receitafederal.gov.br/cnpj/dados/Socios0.zip"
    }
    bronze_layer = BronzeLayer()
    arquivos_baixados = bronze_layer.process(exemplo_urls)
    for tipo, caminho in arquivos_baixados.items():
        print(f"{tipo} baixado em {caminho}")