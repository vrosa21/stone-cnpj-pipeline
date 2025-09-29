import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path
import os
from datetime import datetime, timedelta
import time

# Configuração da página
st.set_page_config(
    page_title="Monitor de Pipeline - CNPJ", 
    layout="wide",
    initial_sidebar_state="expanded"
)

# CSS customizado com paleta Stone
st.markdown("""
<style>
/* Cores da Stone */
:root {
    --stone-primary: #00D4AA;
    --stone-dark: #003D34;
    --stone-light: #E8FFF9;
    --stone-secondary: #7ED321;
    --stone-accent: #00B894;
    --stone-warning: #F39C12;
    --stone-error: #E74C3C;
}

.metric-card {
    background-color: var(--stone-light);
    padding: 1rem;
    border-radius: 0.5rem;
    border-left: 4px solid var(--stone-primary);
}

.alert-success {
    color: var(--stone-dark);
    background-color: var(--stone-light);
    border: 1px solid var(--stone-primary);
    padding: 0.75rem;
    border-radius: 0.375rem;
    font-weight: 500;
}

.alert-warning {
    color: #8B4513;
    background-color: #FFF8DC;
    border: 1px solid var(--stone-warning);
    padding: 0.75rem;
    border-radius: 0.375rem;
    font-weight: 500;
}

/* Customizar sidebar */
.css-1d391kg {
    background-color: var(--stone-light);
}

/* Títulos com cor Stone */
h1, h2, h3 {
    color: var(--stone-dark) !important;
}

/* Métricas customizadas */
.metric-container {
    background: linear-gradient(135deg, var(--stone-light) 0%, white 100%);
    border-radius: 10px;
    padding: 1rem;
    border-left: 5px solid var(--stone-primary);
    box-shadow: 0 2px 4px rgba(0,212,170,0.1);
}
</style>
""", unsafe_allow_html=True)

# Título principal com estilo Stone
st.markdown("""
<div style="text-align: center; padding: 2rem; background: linear-gradient(135deg, #00D4AA 0%, #00B894 100%); border-radius: 10px; margin-bottom: 2rem;">
    <h1 style="color: white; margin: 0; font-weight: 700;">Monitor de Pipeline - CNPJ</h1>
    <p style="color: white; margin: 0; font-size: 1.2rem; opacity: 0.9;">Dashboard-teste de Monitoramento Stone</p>
</div>
""", unsafe_allow_html=True)

# Sidebar com controles
st.sidebar.header("Controles")
auto_refresh = st.sidebar.checkbox("Auto-refresh (30s)", False)
mostrar_graficos = st.sidebar.checkbox("Mostrar Gráficos", True)
mostrar_logs = st.sidebar.checkbox("Mostrar Logs", True)
mostrar_detalhes = st.sidebar.checkbox("Mostrar Detalhes dos Dados", False)

# Função para ler logs
def ler_logs(arquivo_log, linhas=50):
    try:
        if os.path.exists(arquivo_log):
            with open(arquivo_log, 'r', encoding='utf-8') as f:
                return f.readlines()[-linhas:]
        return ["Log não encontrado"]
    except Exception as e:
        return [f"Erro ao ler log: {str(e)}"]

# Função para obter informações do arquivo
def info_arquivo(caminho):
    try:
        if os.path.exists(caminho):
            stat = os.stat(caminho)
            return {
                'existe': True,
                'tamanho': stat.st_size,
                'modificado': datetime.fromtimestamp(stat.st_mtime),
                'tamanho_mb': round(stat.st_size / (1024*1024), 2)
            }
        return {'existe': False}
    except:
        return {'existe': False}

# Seção de métricas principais
st.header("Métricas Principais")

col1, col2, col3, col4 = st.columns(4)

with col1:
    try:
        info_empresas = info_arquivo('data/silver/empresas_silver.parquet')
        if info_empresas['existe']:
            df_empresas = pd.read_parquet('data/silver/empresas_silver.parquet')
            st.metric(
                "Empresas Processadas", 
                f"{len(df_empresas):,}",
                delta=f"{info_empresas['tamanho_mb']} MB"
            )
        else:
            st.metric("Empresas Processadas", "0", delta="Arquivo não encontrado")
    except Exception as e:
        st.metric("Empresas Processadas", "Erro", delta=str(e)[:20])

with col2:
    try:
        info_socios = info_arquivo('data/silver/socios_silver.parquet')
        if info_socios['existe']:
            df_socios = pd.read_parquet('data/silver/socios_silver.parquet')
            st.metric(
                "Sócios Processados", 
                f"{len(df_socios):,}",
                delta=f"{info_socios['tamanho_mb']} MB"
            )
        else:
            st.metric("Sócios Processados", "0", delta="Arquivo não encontrado")
    except Exception as e:
        st.metric("Sócios Processados", "Erro", delta=str(e)[:20])

with col3:
    try:
        info_gold = info_arquivo('data/gold/cnpj_gold.parquet')
        if info_gold['existe']:
            df_gold = pd.read_parquet('data/gold/cnpj_gold.parquet')
            st.metric(
                "CNPJs Gold", 
                f"{len(df_gold):,}",
                delta=f"{info_gold['tamanho_mb']} MB"
            )
        else:
            st.metric("CNPJs Gold", "0", delta="Arquivo não encontrado")
    except Exception as e:
        st.metric("CNPJs Gold", "Erro", delta=str(e)[:20])

with col4:
    # Status do último processamento
    logs_recentes = ['logs/silver.log', 'logs/gold.log', 'logs/bronze.log']
    ultimo_processamento = None
    
    for log_file in logs_recentes:
        if os.path.exists(log_file):
            mod_time = os.path.getmtime(log_file)
            if not ultimo_processamento or mod_time > ultimo_processamento:
                ultimo_processamento = mod_time
    
    if ultimo_processamento:
        ultimo_proc = datetime.fromtimestamp(ultimo_processamento)
        delta_tempo = datetime.now() - ultimo_proc
        if delta_tempo.total_seconds() < 3600:  # menos de 1 hora
            delta_str = f"{int(delta_tempo.total_seconds()/60)} min atrás"
        else:
            delta_str = f"{int(delta_tempo.total_seconds()/3600)} h atrás"
        
        st.metric(
            "Último Processamento", 
            ultimo_proc.strftime("%d/%m %H:%M"),
            delta=delta_str
        )
    else:
        st.metric("Último Processamento", "Nunca", delta="Sem logs")

# Seção de alertas
st.header("Status e Alertas")

alertas = []

# Verificar se arquivos existem e estão atualizados
arquivos_importantes = {
    'data/silver/empresas_silver.parquet': 'Empresas Silver',
    'data/silver/socios_silver.parquet': 'Sócios Silver', 
    'data/gold/cnpj_gold.parquet': 'CNPJs Gold'
}

for arquivo, nome in arquivos_importantes.items():
    if not os.path.exists(arquivo):
        alertas.append(f"ALERTA: {nome}: Arquivo não encontrado")
    else:
        mod_time = os.path.getmtime(arquivo)
        if (datetime.now().timestamp() - mod_time) > 86400:  # 24 horas
            alertas.append(f"ALERTA: {nome}: Arquivo desatualizado (>24h)")

# Verificar logs recentes
if os.path.exists('logs/silver.log'):
    mod_time = os.path.getmtime('logs/silver.log')
    if (datetime.now().timestamp() - mod_time) > 86400:
        alertas.append("ALERTA: Pipeline não executou nas últimas 24 horas")

if alertas:
    for alerta in alertas:
        st.markdown(f'<div class="alert-warning">{alerta}</div>', unsafe_allow_html=True)
else:
    st.markdown('<div class="alert-success">OK: Todos os sistemas operacionais!</div>', unsafe_allow_html=True)

# Seção de gráficos
if mostrar_graficos:
    st.header("Visualizações")
    
    try:
        if os.path.exists('data/gold/cnpj_gold.parquet'):
            df_gold = pd.read_parquet('data/gold/cnpj_gold.parquet')
            
            # Paleta de cores Stone
            stone_colors = ['#00D4AA', '#00B894', '#7ED321', '#003D34', '#E8FFF9']
            
            col1, col2 = st.columns(2)
            
            with col1:
                # Gráfico de CNPJs com sócios estrangeiros (CORRIGIDO)
                if 'flag_socio_estrangeiro' in df_gold.columns:
                    flag_counts = df_gold['flag_socio_estrangeiro'].value_counts()
                    
                    # Criar nomes dinâmicos baseados nos valores reais
                    if len(flag_counts) == 2:
                        labels = ['Sem Sócios Estrangeiros', 'Com Sócios Estrangeiros']
                        colors = ['#003D34', '#00D4AA']
                    elif flag_counts.index[0] == 0:
                        labels = ['Sem Sócios Estrangeiros']
                        colors = ['#003D34']
                    else:
                        labels = ['Com Sócios Estrangeiros']
                        colors = ['#00D4AA']
                    
                    fig1 = px.pie(
                        values=flag_counts.values,
                        names=labels,
                        title="CNPJs com Sócios Estrangeiros",
                        color_discrete_sequence=colors
                    )
                    fig1.update_layout(
                        title_font_color='#003D34',
                        font=dict(color='#003D34')
                    )
                    st.plotly_chart(fig1, use_container_width=True)
                else:
                    st.warning("Coluna 'flag_socio_estrangeiro' não encontrada")
            
            with col2:
                # Gráfico de quantidade de sócios
                if 'qtde_socios' in df_gold.columns:
                    fig2 = px.histogram(
                        df_gold[df_gold['qtde_socios'] <= 20],  # Filtrar outliers
                        x='qtde_socios', 
                        title="Distribuição - Quantidade de Sócios",
                        nbins=20,
                        color_discrete_sequence=['#00D4AA']
                    )
                    fig2.update_layout(
                        xaxis_title="Quantidade de Sócios", 
                        yaxis_title="Frequência",
                        title_font_color='#003D34',
                        font=dict(color='#003D34')
                    )
                    st.plotly_chart(fig2, use_container_width=True)
                else:
                    st.warning("Coluna 'qtde_socios' não encontrada")
            
            # Gráfico adicional: Doc Alvo (CORRIGIDO)
            col3, col4 = st.columns(2)
            
            with col3:
                if 'doc_alvo' in df_gold.columns:
                    doc_alvo_counts = df_gold['doc_alvo'].value_counts()
                    
                    # Criar nomes dinâmicos baseados nos valores reais
                    if len(doc_alvo_counts) == 2:
                        labels_doc = ['Não é Doc Alvo', 'É Doc Alvo']
                        colors_doc = ['#003D34', '#7ED321']
                    elif doc_alvo_counts.index[0] == 0:
                        labels_doc = ['Não é Doc Alvo']
                        colors_doc = ['#003D34']
                    else:
                        labels_doc = ['É Doc Alvo']
                        colors_doc = ['#7ED321']
                    
                    fig3 = px.bar(
                        x=labels_doc,
                        y=doc_alvo_counts.values,
                        title="Documentos Alvo",
                        color_discrete_sequence=colors_doc
                    )
                    fig3.update_layout(
                        title_font_color='#003D34',
                        font=dict(color='#003D34')
                    )
                    st.plotly_chart(fig3, use_container_width=True)
                else:
                    st.warning("Coluna 'doc_alvo' não encontrada")
            
            with col4:
                # Métricas resumidas com estilo Stone
                st.markdown("""
                <div style="background: linear-gradient(135deg, #E8FFF9 0%, white 100%); 
                           padding: 1.5rem; border-radius: 10px; border-left: 5px solid #00D4AA;">
                    <h3 style="color: #003D34; margin-top: 0;">Resumo dos Dados</h3>
                """, unsafe_allow_html=True)
                
                st.write(f"**Total de CNPJs:** {len(df_gold):,}")
                
                # Verificar se as colunas existem antes de usar
                if 'flag_socio_estrangeiro' in df_gold.columns:
                    st.write(f"**Com sócios estrangeiros:** {df_gold['flag_socio_estrangeiro'].sum():,}")
                else:
                    st.write("**Com sócios estrangeiros:** Coluna não encontrada")
                
                if 'doc_alvo' in df_gold.columns:
                    st.write(f"**Documentos alvo:** {df_gold['doc_alvo'].sum():,}")
                else:
                    st.write("**Documentos alvo:** Coluna não encontrada")
                
                if 'qtde_socios' in df_gold.columns:
                    st.write(f"**Média de sócios:** {df_gold['qtde_socios'].mean():.2f}")
                    st.write(f"**Máximo de sócios:** {df_gold['qtde_socios'].max()}")
                else:
                    st.write("**Média de sócios:** Coluna não encontrada")
                    st.write("**Máximo de sócios:** Coluna não encontrada")
                
                st.markdown("</div>", unsafe_allow_html=True)
        
        else:
            st.warning("Dados da camada Gold não disponíveis para gráficos")
    
    except Exception as e:
        st.error(f"Erro ao gerar gráficos: {str(e)}")
        
        # Debug: mostrar informações sobre as colunas disponíveis
        try:
            if os.path.exists('data/gold/cnpj_gold.parquet'):
                df_gold = pd.read_parquet('data/gold/cnpj_gold.parquet')
                st.write("**Colunas disponíveis no dataset Gold:**")
                st.write(list(df_gold.columns))
                st.write("**Primeiras linhas do dataset:**")
                st.dataframe(df_gold.head(3))
        except Exception as debug_e:
            st.error(f"Erro ao fazer debug: {str(debug_e)}")

# Seção de detalhes dos dados
if mostrar_detalhes:
    st.header("Detalhes dos Dados")
    
    tab1, tab2, tab3 = st.tabs(["Empresas", "Sócios", "Gold"])
    
    with tab1:
        try:
            if os.path.exists('data/silver/empresas_silver.parquet'):
                df_empresas = pd.read_parquet('data/silver/empresas_silver.parquet')
                st.write(f"**Shape:** {df_empresas.shape}")
                st.write("**Amostra dos dados:**")
                st.dataframe(df_empresas.head(10))
                
                # Informações sobre colunas
                st.write("**Informações das colunas:**")
                dtypes_info = []
                for col in df_empresas.columns:
                    dtypes_info.append({
                        'Coluna': col,
                        'Tipo': str(df_empresas[col].dtype),
                        'Valores_Nulos': df_empresas[col].isnull().sum()
                    })
                dtypes_df = pd.DataFrame(dtypes_info)
                st.dataframe(dtypes_df)
        except Exception as e:
            st.error(f"Erro ao carregar dados de empresas: {e}")
    
    with tab2:
        try:
            if os.path.exists('data/silver/socios_silver.parquet'):
                df_socios = pd.read_parquet('data/silver/socios_silver.parquet')
                st.write(f"**Shape:** {df_socios.shape}")
                st.write("**Amostra dos dados:**")
                st.dataframe(df_socios.head(10))
                
                # Informações sobre colunas
                st.write("**Informações das colunas:**")
                dtypes_info = []
                for col in df_socios.columns:
                    dtypes_info.append({
                        'Coluna': col,
                        'Tipo': str(df_socios[col].dtype),
                        'Valores_Nulos': df_socios[col].isnull().sum()
                    })
                dtypes_df = pd.DataFrame(dtypes_info)
                st.dataframe(dtypes_df)
        except Exception as e:
            st.error(f"Erro ao carregar dados de sócios: {e}")
    
    with tab3:
        try:
            if os.path.exists('data/gold/cnpj_gold.parquet'):
                df_gold = pd.read_parquet('data/gold/cnpj_gold.parquet')
                st.write(f"**Shape:** {df_gold.shape}")
                st.write("**Amostra dos dados:**")
                st.dataframe(df_gold.head(10))
                
                # Informações sobre colunas
                st.write("**Informações das colunas:**")
                dtypes_info = []
                for col in df_gold.columns:
                    dtypes_info.append({
                        'Coluna': col,
                        'Tipo': str(df_gold[col].dtype),
                        'Valores_Nulos': df_gold[col].isnull().sum()
                    })
                dtypes_df = pd.DataFrame(dtypes_info)
                st.dataframe(dtypes_df)
        except Exception as e:
            st.error(f"Erro ao carregar dados gold: {e}")

# Seção de logs
if mostrar_logs:
    st.header("Logs do Pipeline")
    
    tab1, tab2, tab3 = st.tabs(["Silver", "Gold", "Bronze"])
    
    with tab1:
        st.subheader("Silver Layer Log")
        logs_silver = ler_logs('logs/silver.log', 30)
        with st.container():
            st.markdown("""
            <div style="background-color: #f8f9fa; padding: 1rem; border-radius: 5px; 
                       border-left: 4px solid #00D4AA; font-family: monospace; font-size: 12px;">
            """, unsafe_allow_html=True)
            for linha in logs_silver:
                st.text(linha.strip())
            st.markdown("</div>", unsafe_allow_html=True)
    
    with tab2:
        st.subheader("Gold Layer Log")
        logs_gold = ler_logs('logs/gold.log', 30)
        with st.container():
            st.markdown("""
            <div style="background-color: #f8f9fa; padding: 1rem; border-radius: 5px; 
                       border-left: 4px solid #7ED321; font-family: monospace; font-size: 12px;">
            """, unsafe_allow_html=True)
            for linha in logs_gold:
                st.text(linha.strip())
            st.markdown("</div>", unsafe_allow_html=True)
    
    with tab3:
        st.subheader("Bronze Layer Log")
        logs_bronze = ler_logs('logs/bronze.log', 30)
        with st.container():
            st.markdown("""
            <div style="background-color: #f8f9fa; padding: 1rem; border-radius: 5px; 
                       border-left: 4px solid #00B894; font-family: monospace; font-size: 12px;">
            """, unsafe_allow_html=True)
            for linha in logs_bronze:
                st.text(linha.strip())
            st.markdown("</div>", unsafe_allow_html=True)

# Footer com estilo Stone
st.markdown("---")
st.markdown("""
<div style="text-align: center; padding: 1rem; background-color: #003D34; color: white; border-radius: 5px;">
    <strong>Desenvolvido por Vinicius Rosa Ribeiro</strong> | Última atualização: """ + datetime.now().strftime("%d/%m/%Y %H:%M:%S") + """
</div>
""", unsafe_allow_html=True)

# Auto-refresh
if auto_refresh:
    time.sleep(30)
    st.rerun()
