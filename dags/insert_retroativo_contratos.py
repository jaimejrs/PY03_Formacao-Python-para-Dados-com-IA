import pandas as pd
import requests
from io import BytesIO
from hdfs import InsecureClient
from datetime import datetime
import time

# ----------------- CONFIGURAÇÕES -----------------
API_URL = "https://api-dados-abertos.cearatransparente.ce.gov.br/transparencia/contratos/contratos"
HDFS_URL = "http://host.docker.internal:9870"  
HDFS_USER = "root"
HDFS_BASE_PATH = "/contratos"
TIMEZONE = "America/Fortaleza"
# -------------------------------------------------

def converter_data_assinatura(datas: pd.Series) -> pd.Series:
    datas_texto = datas.astype(str).str.strip()
    data_convertida = pd.Series(pd.NaT, index=datas.index, dtype="datetime64[ns, UTC]")
    formatos = ["%Y-%m-%dT%H:%M:%S.%f%z", "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%d %H:%M:%S%z", "%Y-%m-%d", "%d/%m/%Y"]
    for formato in formatos:
        mascara_pendente = data_convertida.isna()
        if not mascara_pendente.any():
            break
        data_convertida.loc[mascara_pendente] = pd.to_datetime(
            datas_texto.loc[mascara_pendente], format=formato, errors="coerce", utc=True
        )
    return data_convertida.dt.tz_convert(TIMEZONE)

def coletar_contratos(data_inicio: str, data_fim: str) -> list[dict]:
    session = requests.Session()
    params = {"page": 1, "data_assinatura_inicio": data_inicio, "data_assinatura_fim": data_fim}
    try:
        response = session.get(API_URL, params=params, timeout=60)
        response.raise_for_status()
        payload = response.json()
        summary = payload.get("sumary", {})
        total_pages = int(summary.get("total_pages", 1))
        registros = []
        for page in range(1, total_pages + 1):
            if page != 1:
                params["page"] = page
                page_response = session.get(API_URL, params=params, timeout=60)
                page_response.raise_for_status()
                payload = page_response.json()
            page_data = payload.get("data", [])
            if page_data:
                registros.extend(page_data)
        return registros
    finally:
        session.close()

def preparar_contratos(registros: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(registros)
    if "data_assinatura" not in df.columns:
        return pd.DataFrame()
    df["data_assinatura_dt"] = converter_data_assinatura(df["data_assinatura"])
    df = df.dropna(subset=["data_assinatura_dt"]).copy()
    if df.empty:
        return pd.DataFrame()
    df["ano"] = df["data_assinatura_dt"].dt.strftime("%Y")
    df["mes"] = df["data_assinatura_dt"].dt.strftime("%m")
    df = df.drop(columns=["data_assinatura_dt"])
    return df

def salvar_grupo_no_hdfs(client: InsecureClient, df_mes: pd.DataFrame, ano: str, mes: str) -> None:
    hdfs_path = f"{HDFS_BASE_PATH}/{ano}/{mes}/contratos_{ano}_{mes}.csv"
    csv_bytes = df_mes.to_csv(index=False).encode("utf-8")
    with BytesIO(csv_bytes) as reader:
        client.write(hdfs_path, reader, overwrite=True)
    print(f"Salvo no HDFS: {hdfs_path} ({len(df_mes)} registros)")

def processar_carga_historica():
    # (Jan/2025 até Mar/2026)
    meses_para_processar = pd.date_range(start='2025-01-01', end='2026-03-01', freq='MS')
    client = InsecureClient(HDFS_URL, user=HDFS_USER)

    for data_inicio_mes in meses_para_processar:
        # Pega o último dia do mês atual do loop
        data_fim_mes = data_inicio_mes + pd.offsets.MonthEnd(1)
        
        # Se for março de 2026, trava no dia 16/03 
        if data_fim_mes > pd.to_datetime('2026-03-16'):
            data_fim_mes = pd.to_datetime('2026-03-16')

        str_inicio = data_inicio_mes.strftime('%d/%m/%Y')
        str_fim = data_fim_mes.strftime('%d/%m/%Y')

        print(f"\n==============================================")
        print(f"Iniciando coleta do periodo: {str_inicio} a {str_fim}")
        
        # 1. Coleta
        registros_brutos = coletar_contratos(str_inicio, str_fim)
        if not registros_brutos:
            print("Sem dados neste período.")
            continue
            
        # 2. Prepara
        df_preparado = preparar_contratos(registros_brutos)
        if df_preparado.empty:
            print("Nenhum dado válido após preparação.")
            continue
            
        # 3. Salva no HDFS separando por ano/mês
        for (ano, mes), df_mes in df_preparado.groupby(["ano", "mes"], dropna=False):
            df_saida = df_mes.drop(columns=["ano", "mes"])
            salvar_grupo_no_hdfs(client, df_saida, str(ano), str(mes))
            
        # Pequena pausa para não derrubar a API com requisições seguidas
        time.sleep(2)

    print("\nCarga Histórica Finalizada com Sucesso!")

if __name__ == "__main__":
    processar_carga_historica()