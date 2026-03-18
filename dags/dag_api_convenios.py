from datetime import timedelta
from io import BytesIO
import json
import os

import pandas as pd
import pendulum
import requests
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from hdfs import InsecureClient


API_URL = "https://api-dados-abertos.cearatransparente.ce.gov.br/transparencia/contratos/convenios"
HDFS_URL = "http://host.docker.internal:9870"
HDFS_USER = "root"
HDFS_BASE_PATH = "/convenios"
TIMEZONE = "America/Fortaleza"

TMP_RAW = "/tmp/convenios_raw.json"
TMP_PREP = "/tmp/convenios_prep.json"

COLUNAS_ESPERADAS = [
    "id", "cod_concedente", "cod_financiador", "cod_gestora", "cod_orgao",
    "cod_secretaria", "descricao_modalidade", "descricao_objeto", "descricao_tipo",
    "descricao_url", "data_assinatura", "data_processamento", "data_termino",
    "flg_tipo", "isn_parte_destino", "isn_sic", "num_spu", "valor_contrato",
    "isn_modalidade", "isn_entidade", "tipo_objeto", "num_spu_licitacao",
    "descricao_justificativa", "valor_can_rstpg", "data_publicacao_portal",
    "descricao_url_pltrb", "descricao_url_ddisp", "descricao_url_inexg",
    "cod_plano_trabalho", "num_certidao", "descriaco_edital",
    "cpf_cnpj_financiador", "num_contrato", "valor_original_concedente",
    "valor_original_contrapartida", "valor_atualizado_concedente",
    "valor_atualizado_contrapartida", "created_at", "updated_at",
    "plain_num_contrato", "calculated_valor_aditivo", "calculated_valor_ajuste",
    "calculated_valor_empenhado", "calculated_valor_pago", "contract_type",
    "infringement_status", "cod_financiador_including_zeroes",
    "accountability_status", "plain_cpf_cnpj_financiador", "descricao_situacao",
    "data_publicacao_doe", "descricao_nome_credor", "isn_parte_origem",
    "data_auditoria", "data_termino_original", "data_inicio", "data_rescisao",
    "confidential", "gestor_contrato",
]


def converter_data_assinatura(datas: pd.Series) -> pd.Series:
    datas_texto = datas.astype(str).str.strip()
    data_convertida = pd.Series(pd.NaT, index=datas.index, dtype="datetime64[ns, UTC]")

    formatos = [
        "%Y-%m-%dT%H:%M:%S.%f%z",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%d %H:%M:%S%z",
        "%Y-%m-%d",
        "%d/%m/%Y",
    ]

    for formato in formatos:
        mascara_pendente = data_convertida.isna()
        if not mascara_pendente.any():
            break

        data_convertida.loc[mascara_pendente] = pd.to_datetime(
            datas_texto.loc[mascara_pendente],
            format=formato,
            errors="coerce",
            utc=True,
        )

    return data_convertida.dt.tz_convert(TIMEZONE)


def salvar_grupo_no_hdfs(client: InsecureClient, df_mes: pd.DataFrame, ano: str, mes: str) -> None:
    hdfs_path = f"{HDFS_BASE_PATH}/{ano}/{mes}/convenios_{ano}_{mes}.csv"
    csv_bytes = df_mes.to_csv(index=False).encode("utf-8")

    with BytesIO(csv_bytes) as reader:
        client.write(hdfs_path, reader, overwrite=True)

    print(f"Arquivo salvo: {hdfs_path} ({len(df_mes)} registros)")


def coletar_convenios(data_inicio: str, data_fim: str) -> list[dict]:
    session = requests.Session()
    params = {
        "page": 1,
        "data_assinatura_inicio": data_inicio,
        "data_assinatura_fim": data_fim,
    }

    try:
        response = session.get(API_URL, params=params, timeout=60)
        response.raise_for_status()

        payload = response.json()
        summary = payload.get("sumary", {})
        total_pages = int(summary.get("total_pages", 1))
        total_records = int(summary.get("total_records", 0))

        print(f"Periodo consultado: {data_inicio} a {data_fim}")
        print(f"Total de paginas: {total_pages}")
        print(f"Total de registros: {total_records}")

        registros = []

        for page in range(1, total_pages + 1):
            if page == 1:
                page_payload = payload
            else:
                params["page"] = page
                page_response = session.get(API_URL, params=params, timeout=60)
                page_response.raise_for_status()
                page_payload = page_response.json()

            page_data = page_payload.get("data", [])

            if page_data:
                registros.extend(page_data)
                print(f"Pagina {page}: {len(page_data)} registros coletados")
            else:
                print(f"Pagina {page}: sem registros")

        return registros
    finally:
        session.close()


def preparar_convenios(registros: list[dict]) -> list[dict]:
    if not registros:
        print("Nenhum registro encontrado para processar.")
        return []

    df = pd.DataFrame(registros)
    colunas_existentes = [col for col in COLUNAS_ESPERADAS if col in df.columns]
    df = df[colunas_existentes].copy()

    if "data_assinatura" not in df.columns:
        raise ValueError("A coluna 'data_assinatura' nao foi encontrada nos dados.")

    df["data_assinatura_dt"] = converter_data_assinatura(df["data_assinatura"])
    df = df.dropna(subset=["data_assinatura_dt"]).copy()

    if df.empty:
        print("Nenhum registro com data_assinatura valida para salvar.")
        return []

    print(
        "Amostra de data_assinatura_dt antes da extracao de ano/mes:",
        df["data_assinatura_dt"].head(10).astype(str).tolist(),
    )
    df["ano"] = df["data_assinatura_dt"].dt.strftime("%Y")
    df["mes"] = df["data_assinatura_dt"].dt.strftime("%m")
    print(
        "Amostra de ano/mes extraidos:",
        df[["data_assinatura", "ano", "mes"]].head(10).to_dict(orient="records"),
    )
    df = df.drop(columns=["data_assinatura_dt"])

    return df.to_dict(orient="records")


def salvar_convenios_hdfs(registros_processados: list[dict]) -> None:
    if not registros_processados:
        print("Nenhum registro processado para salvar no HDFS.")
        return

    df = pd.DataFrame(registros_processados)
    client = InsecureClient(HDFS_URL, user=HDFS_USER)
    total_salvos = 0

    for (ano, mes), df_mes in df.groupby(["ano", "mes"], dropna=False):
        df_saida = df_mes.drop(columns=["ano", "mes"])
        salvar_grupo_no_hdfs(client, df_saida, str(ano), str(mes))
        print(f"Grupo {ano}/{mes}: {len(df_saida)} registros salvos no HDFS.")
        total_salvos += len(df_saida)

    print(f"Total de registros salvos no HDFS: {total_salvos}")


def definir_periodo_execucao(**context) -> dict:
    logical_date = context["logical_date"].in_tz(TIMEZONE)
    inicio_mes = logical_date.start_of("month")
    print(f"Inicio do mes: {inicio_mes}")
    fim_mes = logical_date.end_of("month")
    print(f"Fim do mes: {fim_mes}")
    periodo = {
        "data_inicio": inicio_mes.format("DD/MM/YYYY"),
        "data_fim": fim_mes.format("DD/MM/YYYY"),
    }
    print(f"Periodo definido: {periodo['data_inicio']} a {periodo['data_fim']}")
    return periodo


def task_coletar_convenios(**context) -> None:
    ti = context["ti"]
    periodo = ti.xcom_pull(task_ids="definir_periodo_execucao")
    registros = coletar_convenios(periodo["data_inicio"], periodo["data_fim"])

    with open(TMP_RAW, "w", encoding="utf-8") as f:
        json.dump(registros, f, ensure_ascii=False, default=str)

    print(f"Arquivo bruto salvo em {TMP_RAW} com {len(registros)} registros")


def task_preparar_convenios(**context) -> None:
    if not os.path.exists(TMP_RAW):
        raise FileNotFoundError(f"Arquivo nao encontrado: {TMP_RAW}")

    with open(TMP_RAW, "r", encoding="utf-8") as f:
        registros = json.load(f)

    registros_processados = preparar_convenios(registros)

    with open(TMP_PREP, "w", encoding="utf-8") as f:
        json.dump(registros_processados, f, ensure_ascii=False, default=str)

    print(f"Arquivo preparado salvo em {TMP_PREP} com {len(registros_processados)} registros")


def task_salvar_convenios_hdfs(**context) -> None:
    if not os.path.exists(TMP_PREP):
        raise FileNotFoundError(f"Arquivo nao encontrado: {TMP_PREP}")

    with open(TMP_PREP, "r", encoding="utf-8") as f:
        registros_processados = json.load(f)

    salvar_convenios_hdfs(registros_processados)


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="api_convenios_ceara_hdfs",
    default_args=default_args,
    description="Extrai convenios do Ceara Transparente e grava no HDFS",
    schedule="0 18 * * *",
    start_date=pendulum.datetime(2025, 1, 1, tz=TIMEZONE),
    catchup=False,
    tags=["ceara", "convenios", "hdfs"],
) as dag:
    task_definir_periodo_execucao = PythonOperator(
        task_id="definir_periodo_execucao",
        python_callable=definir_periodo_execucao,
    )

    task_coletar_registros = PythonOperator(
        task_id="coletar_convenios",
        python_callable=task_coletar_convenios,
        do_xcom_push=False,
    )

    task_preparar_registros = PythonOperator(
        task_id="preparar_convenios",
        python_callable=task_preparar_convenios,
        do_xcom_push=False,
    )

    task_salvar_registros = PythonOperator(
        task_id="salvar_convenios_hdfs",
        python_callable=task_salvar_convenios_hdfs,
        do_xcom_push=False,
    )

    task_definir_periodo_execucao >> task_coletar_registros >> task_preparar_registros >> task_salvar_registros
