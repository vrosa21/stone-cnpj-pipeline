import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import logging
import os
import json
from datetime import datetime
from typing import List, Dict, Optional

# Garantir que a pasta de logs existe (considerando que o codigo pode rodar dentro da pasta etl)
log_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'logs'))
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

# Configura logs para arquivo e console, com o caminho correto para o arquivo de log
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(log_dir, 'webscraping.log')),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class ArquivoEncontrado:
    """
    Representa um arquivo ZIP encontrado: nome, URL, tipo,
    tamanho em bytes e numero extrai­do.
    """
    def __init__(self, nome: str, url_completa: str,
                 tipo: str, tamanho_bytes: float, numero_arquivo: int):
        self.nome = nome
        self.url_completa = url_completa
        self.tipo = tipo
        self.tamanho_bytes = tamanho_bytes
        self.numero_arquivo = numero_arquivo

class WebscrapingStone:
    def __init__(self):
        # Endpoint base
        self.endpoint = "https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/2025-09/"
        self.headers = {'User-Agent': 'Mozilla/5.0', 'Accept': '*/*'}
        logger.info("WebscrapingStone inicializado")
        logger.info(f"Endpoint configurado: {self.endpoint}")

    def acessar_endpoint(self) -> Optional[requests.Response]:
        logger.info("Acessando endpoint...")
        try:
            resp = requests.get(self.endpoint, headers=self.headers, timeout=30)
            resp.raise_for_status()
            logger.info(f"Sucesso: status {resp.status_code}, resposta {len(resp.content)} bytes")
            return resp
        except Exception as e:
            logger.error(f"Erro ao acessar endpoint: {e}")
            return None

    def get_remote_size(self, url: str, timeout: int = 10) -> float:
        try:
            head = requests.head(url, headers=self.headers, timeout=timeout)
            head.raise_for_status()
            length = head.headers.get('Content-Length')
            return float(length) if length else float('inf')
        except Exception as e:
            logger.warning(f"NÃ£o foi possi­vel obter tamanho HEAD de {url}: {e}")
            return float('inf')

    def identificar_tipo_arquivo(self, nome_arquivo: str) -> str:
        lname = nome_arquivo.lower()
        if 'empresas' in lname:
            return 'empresas'
        if 'socios' in lname:
            return 'socios'
        return 'outros'

    def extrair_numero_arquivo(self, nome_arquivo: str) -> int:
        import re
        m = re.search(r'(\d+)\.zip$', nome_arquivo, re.IGNORECASE)
        return int(m.group(1)) if m else 9999

    def extrair_arquivos_da_pagina(self, response: requests.Response) -> List[ArquivoEncontrado]:
        logger.info("Extraindo arquivos .zip da pagina")
        soup = BeautifulSoup(response.content, 'html.parser')
        encontrados: List[ArquivoEncontrado] = []

        for link in soup.find_all('a', href=True):
            href = link['href']
            if not href.lower().endswith('.zip'):
                continue

            tipo = self.identificar_tipo_arquivo(href)
            if tipo not in ('empresas', 'socios'):
                continue

            url_completa = urljoin(self.endpoint, href)
            tamanho = self.get_remote_size(url_completa)
            numero = self.extrair_numero_arquivo(href)

            arq = ArquivoEncontrado(href, url_completa, tipo, tamanho, numero)
            encontrados.append(arq)

            mb = tamanho / 1024 / 1024 if tamanho != float('inf') else float('inf')
            logger.info(f"  {tipo.capitalize()}: {href} â€“ {mb:.2f} MB")

        logger.info(f"Total de arquivos relevantes: {len(encontrados)}")
        return encontrados

    def encontrar_menores_arquivos(self, arquivos: List[ArquivoEncontrado]) -> Dict[str, ArquivoEncontrado]:
        logger.info("Identificando menores arquivos por tipo")
        menores: Dict[str, ArquivoEncontrado] = {}
        for arq in arquivos:
            if arq.tipo not in menores or arq.tamanho_bytes < menores[arq.tipo].tamanho_bytes:
                menores[arq.tipo] = arq

        for tipo, arq in menores.items():
            mb = arq.tamanho_bytes / 1024 / 1024 if arq.tamanho_bytes != float('inf') else float('inf')
            logger.info(f"Menor {tipo}: {arq.nome} â€“ {mb:.2f} MB")
        return menores

    def salvar_resultado_webscraping(self, encontrados: List[ArquivoEncontrado],
                                     menores: Dict[str, ArquivoEncontrado]):
        resultado = {
            'timestamp': datetime.now().isoformat(),
            'endpoint': self.endpoint,
            'total_arquivos_encontrados': len(encontrados),
            'menores_arquivos': {
                tipo: {
                    'nome': arq.nome,
                    'url_completa': arq.url_completa,
                    'tamanho_bytes': arq.tamanho_bytes,
                    'numero_arquivo': arq.numero_arquivo
                } for tipo, arq in menores.items()
            },
            'todos_arquivos': [
                {
                    'nome': a.nome,
                    'url_completa': a.url_completa,
                    'tipo': a.tipo,
                    'tamanho_bytes': a.tamanho_bytes,
                    'numero_arquivo': a.numero_arquivo
                } for a in encontrados
            ]
        }
        with open('webscraping_resultado.json', 'w', encoding='utf-8') as f:
            json.dump(resultado, f, indent=2, ensure_ascii=False)
        logger.info("Resultado salvo em webscraping_resultado.json")

    def executar_webscraping_completo(self) -> Optional[Dict[str, ArquivoEncontrado]]:
        logger.info("Iniciando webscraping completo")
        resp = self.acessar_endpoint()
        if not resp:
            return None

        encontrados = self.extrair_arquivos_da_pagina(resp)
        if not encontrados:
            logger.error("Nenhum arquivo relevante encontrado")
            return None

        menores = self.encontrar_menores_arquivos(encontrados)
        self.salvar_resultado_webscraping(encontrados, menores)
        logger.info("Webscraping conclui­do com sucesso")
        return menores

if __name__ == "__main__":
    scraper = WebscrapingStone()
    menores = scraper.executar_webscraping_completo()
    if menores:
        for tipo, arq in menores.items():
            print(f"Menor arquivo de {tipo}: {arq.nome} -> {arq.url_completa}")
    else:
        print("Nenhum arquivo relevante encontrado.")