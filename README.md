# Arquitetura Detalhada do Pipeline de ETL - Desafio Stone - 
## Visão Geral
Este projeto implementa um pipeline ETL completo seguindo a arquitetura medalhão (Bronze, Silver, Gold) para processamento de dados da Receita Federal do Brasil. A solução demonstra competências em engenharia de dados através de uma abordagem enterprise-ready que combina web scraping inteligente, processamento em lotes, containerização Docker e observabilidade operacional.

## Fluxo Completo de Dados
## Detalhamento Técnico de Cada Etapa

### **Etapa 1: Web Scraping Inteligente**
**Arquivo**: `webscraping.py`  
**Tecnologias**: Python + BeautifulSoup4 + Requests HTTP  
**Localização**: Receita Federal API (`https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/`)

#### **Processo Detalhado**:
```python
class WebscrapingStone:
    def descobrir_menores_arquivos(self):
        # 1. Faz request HTTP para página de listagem
        response = requests.get(self.url_base)
        
        # 2. Parseia HTML com BeautifulSoup
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # 3. Extrai links e metadados dos arquivos ZIP
        links = soup.find_all('a', href=re.compile(r'\.zip$'))
        
        # 4. Para cada arquivo, faz HEAD request para obter tamanho
        size = self.get_remote_size(url)  # Sem download completo
        
        # 5. Classifica por tipo (empresas/sócios) via regex
        tipo = self.classificar_tipo_arquivo(nome)
        
        # 6. Seleciona os menores arquivos de cada categoria
        return arquivos_selecionados
```

#### **Critérios de Seleção**:
- **Otimização de Recursos**: Seleção automática dos menores arquivos (< 50MB)
- **Classificação Inteligente**: Regex para identificar padrões "Empresas" vs "Sócios"
- **Eficiência de Network**: HEAD requests economizam 99% do bandwidth
- **Logging Estruturado**: Rastreamento completo das decisões tomadas

#### **Output**: Lista de objetos `ArquivoEncontrado` com metadados completos

---

### **Etapa 2: Bronze Layer - Extração de Dados Brutos**
**Arquivo**: `bronze.py`  
**Tecnologias**: Python + Requests + Pathlib + Streaming Download  
**Armazenamento**: Sistema de arquivos local (`data/bronze/`)

#### **Características Técnicas**:
- **Streaming Download**: Suporte a arquivos de grande volume sem saturar memória
- **Integridade de Dados**: Verificação de tamanho e estrutura pós-download
- **Gestão de Recursos**: Context managers para limpeza automática
- **Rastreabilidade**: Preservação de metadados originais e timestamps

#### **Output**: Arquivos ZIP preservados em estado original na camada Bronze

---

### **Etapa 3: Silver Layer - Limpeza e Validação**
**Arquivo**: `silver.py`  
**Tecnologias**: Python + Pandas + PyArrow + NumPy + Zipfile  
**Armazenamento**: Formato Parquet (`data/silver/`)

#### **Validações de Qualidade Implementadas**:
```python
def validar_cnpj(self, cnpj_series):
    # Extração de padrão: 8 dígitos base do CNPJ
    cnpj_clean = cnpj_series.str.extract(r'(\d{8})')[0]
    cnpjs_validos = cnpj_clean.notna().sum()
    percentual = (cnpjs_validos / len(cnpj_series)) * 100
    
    # Threshold: 80% CNPJs válidos (critério Stone)
    return percentual > 80

def validar_tipos_socio(self, tipo_series):
    # Códigos válidos: 1, 2, 3 (conforme especificação RFB)
    tipos_validos = tipo_series.isin(['1', '2', '3']).sum()
    percentual = (tipos_validos / len(tipo_series)) * 100
    
    # Threshold: 50% tipos válidos (tolerância dados públicos)
    return percentual > 50
```
#### **Transformações Aplicadas**:
- **Normalização de Tipos**: Conversão string → numérico onde aplicável
- **Limpeza de Dados**: Remoção de caracteres especiais e espaços
- **Padronização**: Aplicação de schema consistente entre arquivos
- **Deduplicação**: Remoção de registros duplicados por CNPJ

#### **Output**: Dados limpos e validados em formato Parquet otimizado

---

### **Etapa 4: Gold Layer - Agregações de Negócio**
**Arquivo**: `gold.py`  
**Tecnologias**: Python + Pandas + PyArrow + Batch Processing  
**Armazenamento**: Parquet + PostgreSQL (`data/gold/`)

#### **Métricas de Negócio Implementadas**:
- **Quantidade de Sócios**: Agregação por CNPJ
- **Flag Sócio Estrangeiro**: Detecção por análise de documentos
- **Documento Alvo**: Empresas porte 03 com mais de 1 sócio
- **Validações Cruzadas**: Integridade referencial empresas ↔ sócios

#### **Output**: Dataset analítico pronto para consumo (`gold_cnpj`)

---

### **Etapa 5: Armazenamento Persistente**
**Tecnologia**: PostgreSQL 15 + Docker Container  
**Configuração**: `docker-compose.yml`

#### **Schema Implementado**:
```sql
-- Tabela principal de resultados (conforme especificação Stone)
CREATE TABLE gold_cnpj (
    cnpj VARCHAR PRIMARY KEY,
    razao_social VARCHAR,
    natureza_juridica INTEGER,
    qualificacao_responsavel INTEGER,
    capital_social DECIMAL,
    cod_porte VARCHAR,
    qtde_socios INTEGER,
    flag_socio_estrangeiro BOOLEAN,
    doc_alvo BOOLEAN
);

-- Índices para performance
CREATE INDEX idx_gold_cnpj_porte ON gold_cnpj(cod_porte);
CREATE INDEX idx_gold_cnpj_doc_alvo ON gold_cnpj(doc_alvo);
```

#### **Características de Produção**:
- **ACID Compliance**: Transações atômicas e consistência
- **Health Checks**: Monitoramento automático de disponibilidade
- **Backup Strategy**: Volumes persistentes Docker
- **Connection Pooling**: Gestão eficiente de conexões

---

### **Etapa 6: Dashboard de Monitoramento**
**Arquivo**: `app.py`  
**Tecnologias**: Streamlit + Plotly + Pandas  
**Acesso**: `http://localhost:8501`

#### **Métricas Monitoradas**:
```python
def calcular_kpis():
    return {
        'total_empresas': len(df_gold),
        'empresas_doc_alvo': len(df_gold[df_gold['doc_alvo'] == True]),
        'taxa_qualidade_cnpj': percentual_cnpjs_validos,
        'empresas_com_socios_estrangeiros': len(df_gold[df_gold['flag_socio_estrangeiro'] == True]),
        'distribuicao_por_porte': df_gold['cod_porte'].value_counts(),
        'tempo_ultima_atualizacao': ultima_execucao
    }
```

#### **Visualizações Implementadas**:
- **KPIs Operacionais**: Cards com métricas principais
- **Distribuições**: Gráficos de barras por porte empresarial
- **Tendências Temporais**: Evolução das métricas ao longo do tempo
- **Alertas de Qualidade**: Indicadores de saúde dos dados
- **Logs em Tempo Real**: Stream de eventos do pipeline

---

### **Etapa 7: Orquestração e Automação**
**Arquivo**: `main.py` + `docker-compose.yml` + `crontab`  
**Tecnologias**: Docker + Cron + Python Logging

#### **Fluxo de Orquestração**:
```python
def main():
    logger.info("===== INÍCIO DO PIPELINE =====")
    
    # Circuit Breaker Pattern: falha rápida em caso de problemas
    if not executar_webscraping():
        return False
    
    if not executar_bronze():
        return False
        
    if not executar_silver():
        return False
        
    if not executar_gold():
        return False
    
    logger.info("===== PIPELINE CONCLUÍDO COM SUCESSO =====")
    return True
```

#### **Agendamento Automatizado**:
```bash
# Crontab - Execução diária às 06:00 (horário Brasília)
0 6 * * * root cd /app && python etl/main.py >> logs/cron.log 2>&1

# Health check a cada 6 horas
0 */6 * * * root cd /app && python 0_check_ambiente.py >> logs/health.log 2>&1
```

---

## Fluxo de Dados Completo

### **Input → Processing → Output**

1. **Web Scraping** → Lista de arquivos otimizada
2. **Bronze Layer** → Arquivos ZIP preservados
3. **Silver Layer** → Dados limpos em Parquet
4. **Gold Layer** → Métricas de negócio agregadas
5. **PostgreSQL** → Persistência transacional
6. **Dashboard** → Visualização em tempo real

### **Tecnologias por Camada**

| Camada | Tecnologia Principal | Formato Output | Justificativa |
|--------|---------------------|----------------|---------------|
| **Web Scraping** | BeautifulSoup4 + Requests | JSON metadata | Eficiência de discovery |
| **Bronze** | Streaming HTTP + Pathlib | ZIP files | Preservação original |
| **Silver** | Pandas + PyArrow | Parquet | Performance analítica |
| **Gold** | Pandas + Batch Processing | Parquet + SQL | Business intelligence |
| **Storage** | PostgreSQL + Docker | Relational tables | ACID + Escalabilidade |
| **Monitor** | Streamlit + Plotly | Web dashboard | Observabilidade real-time |

