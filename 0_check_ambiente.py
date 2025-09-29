#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
SCRIPT DE VERIFICACAO DO AMBIENTE STONE
======================================

Este script verifica se todas as ferramentas estao instaladas
e funcionando corretamente antes de executar o pipeline.

Atencao, meu povo: Execute este script ANTES de tentar rodar o projeto principal, hein!
"""

import subprocess
import sys
import os
import importlib
from pathlib import Path

def verificar_python():
    """Verifica instalacao do Python"""
    print("Verificando Python...")
    try:
        version = sys.version_info
        if version.major >= 3 and version.minor >= 8:
            print(f"   Python {version.major}.{version.minor}.{version.micro} - OK")
            return True
        else:
            print(f"   Python {version.major}.{version.minor}.{version.micro} - Versao muito antiga")
            print("   Instale Python 3.8 ou superior")
            return False
    except Exception as e:
        print(f"   Erro ao verificar Python: {e}")
        return False

def verificar_pip():
    """Verifica se pip esta funcionando"""
    print("Verificando pip...")
    try:
        result = subprocess.run([sys.executable, "-m", "pip", "--version"], 
                              capture_output=True, text=True)
        if result.returncode == 0:
            print(f"   {result.stdout.strip()} - OK")
            return True
        else:
            print("   pip nao esta funcionando")
            return False
    except Exception as e:
        print(f"   Erro ao verificar pip: {e}")
        return False

def verificar_git():
    """Verifica instalacao do Git"""
    print("Verificando Git...")
    try:
        result = subprocess.run(["git", "--version"], capture_output=True, text=True)
        if result.returncode == 0:
            print(f"   {result.stdout.strip()} - OK")
            return True
        else:
            print("   Git nao esta instalado")
            print("   Baixe em: https://git-scm.com/download/windows")
            return False
    except FileNotFoundError:
        print("   Git nao esta instalado ou nao esta no PATH")
        print("   Baixe em: https://git-scm.com/download/windows")
        return False

def verificar_docker():
    """Verifica instalacao do Docker"""
    print("Verificando Docker...")
    try:
        result = subprocess.run(["docker", "--version"], capture_output=True, text=True)
        if result.returncode == 0:
            print(f"   {result.stdout.strip()} - OK")

            # Verificar se Docker esta rodando
            result2 = subprocess.run(["docker", "info"], capture_output=True, text=True)
            if result2.returncode == 0:
                print("   Docker esta rodando - OK")
                return True
            else:
                print("   Docker instalado mas nao esta rodando")
                print("   Abra Docker Desktop e aguarde inicializar")
                return False
        else:
            print("   Docker nao esta funcionando")
            return False
    except FileNotFoundError:
        print("   Docker nao esta instalado")
        print("   Baixe em: https://www.docker.com/products/docker-desktop/")
        return False

def verificar_bibliotecas():
    """Verifica bibliotecas Python essenciais"""
    print("Verificando bibliotecas Python...")

    bibliotecas_essenciais = [
        'requests', 'beautifulsoup4', 'pandas', 
        'numpy', 'pyarrow', 'sqlalchemy'
    ]

    problemas = []

    for lib in bibliotecas_essenciais:
        try:
            # Para beautifulsoup4, o import e diferente
            nome_import = 'bs4' if lib == 'beautifulsoup4' else lib
            importlib.import_module(nome_import)
            print(f"   {lib} - OK")
        except ImportError:
            print(f"   {lib} - NAO INSTALADO")
            problemas.append(lib)

    if problemas:
        print(f"   Execute: pip install {' '.join(problemas)}")
        return False
    else:
        print("   Todas as bibliotecas essenciais estao instaladas")
        return True

def verificar_estrutura_projeto():
    """Verifica estrutura de pastas do projeto"""
    print("Verificando estrutura do projeto...")

    pastas_necessarias = [
        'data', 'data/bronze', 'data/silver', 'data/gold',
        'logs', 'config', 'etl'
    ]

    arquivos_necessarios = [
        'etl/main.py',
        'requirements.txt'
    ]

    problemas = []

    # Verificar pastas
    for pasta in pastas_necessarias:
        caminho = Path(pasta)
        if caminho.exists():
            print(f"   Pasta {pasta} - OK")
        else:
            print(f"   Pasta {pasta} - NAO EXISTE")
            caminho.mkdir(parents=True, exist_ok=True)
            print(f"   Pasta {pasta} - CRIADA")

    # Verificar arquivos
    for arquivo in arquivos_necessarios:
        caminho = Path(arquivo)
        if caminho.exists():
            print(f"   Arquivo {arquivo} - OK")
        else:
            print(f"   Arquivo {arquivo} - NAO EXISTE")
            problemas.append(f"criar {arquivo}")

    if problemas:
        print(f"   Problemas encontrados: {len(problemas)}")
        return False
    else:
        print("   Estrutura do projeto esta correta")
        return True

def main():
    """Funcao principal"""
    print("VERIFICACAO DO AMBIENTE - DESAFIO STONE")
    print("=" * 50)
    print("Verificando se tudo esta pronto para executar o projeto...")
    print()

    # Lista de verificacoes
    verificacoes = [
        ("Python", verificar_python),
        ("pip", verificar_pip),
        ("Git", verificar_git),
        ("Docker", verificar_docker),
        ("Bibliotecas", verificar_bibliotecas),
        ("Estrutura", verificar_estrutura_projeto)
    ]

    resultados = {}

    # Executar todas as verificacoes
    for nome, funcao in verificacoes:
        try:
            resultados[nome] = funcao()
            print()
        except Exception as e:
            print(f"   Erro inesperado em {nome}: {e}")
            resultados[nome] = False
            print()

    # Resumo final
    print("RESUMO DA VERIFICACAO")
    print("=" * 30)

    sucessos = 0
    for nome, resultado in resultados.items():
        status = "OK" if resultado else "PROBLEMA"
        print(f"{nome:12} : {status}")
        if resultado:
            sucessos += 1

    print()
    print(f"Resultado: {sucessos}/{len(verificacoes)} verificacoes passaram")

    if sucessos == len(verificacoes):
        print("AMBIENTE COMPLETAMENTE CONFIGURADO!")
        print("Voce pode executar o projeto Stone com confianca!")
        print()
        print("Proximos passos:")
        print("   1. Execute: python stone_pipeline_main.py")
        print("   2. OU: docker-compose up --build")
    else:
        print("AMBIENTE PRECISA DE AJUSTES")
        print("Corrija os problemas apontados acima antes de continuar")
        print()
        print("Consulte o tutorial completo para instrucoes detalhadas")

if __name__ == "__main__":
    main()