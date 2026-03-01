import os
import sys
from datetime import datetime
import pandas as pd
import psycopg2
import psycopg2.extras
import mysql.connector
from dotenv import load_dotenv

def log(mensagem, tipo='INFO'):
    """Função auxiliar para registrar logs com data e hora."""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f'[{timestamp}] [{tipo}] {mensagem}')

def extrair_dados_origem() -> pd.DataFrame:
    """
    Lê os dados e as análises diretamente das bases de origem (PostgreSQL e MySQL),
    simulando o processo original que consolida os dataframes.
    """
    load_dotenv()
    
    # 1. Conexão e consulta MySQL (Empresa 01)
    log('Conectando ao MySQL e extraindo dados...')
    try:
        conn_mysql = mysql.connector.connect(
            host=os.getenv('MYSQL_HOST'),
            database=os.getenv('MYSQL_DATABASE'),
            user=os.getenv('MYSQL_USER'),
            password=os.getenv('MYSQL_PASSWORD')
        )
        query_mysql = """
        SELECT 'Empresa 01' as empresa, nome_vendedor, sum(valor_venda) as total_vendas        
        FROM datadt_curso_python.vendas
        GROUP BY nome_vendedor
        """
        # Suprime o warning do Pandas sobre a falta do SQLAlchemy connection
        import warnings
        with warnings.catch_warnings():
            warnings.simplefilter('ignore', UserWarning)
            df_mysql = pd.read_sql(query_mysql, conn_mysql)
            
        log(f'Dados MySQL extraídos: {len(df_mysql)} registros')
    except Exception as e:
        log(f'Erro ao extrair do MySQL: {e}', 'ERROR')
        raise
    finally:
        if 'conn_mysql' in locals() and conn_mysql.is_connected():
            conn_mysql.close()
            

    # 2. Conexão e consulta PostgreSQL (Empresa 02)
    log('Conectando ao PostgreSQL e extraindo dados...')
    try:
        conn_pg = psycopg2.connect(
            host=os.getenv('POSTGRES_HOST'),
            database=os.getenv('POSTGRES_DATABASE'),
            user=os.getenv('POSTGRES_USER'),
            password=os.getenv('POSTGRES_PASSWORD'),
            port=os.getenv('POSTGRES_PORT')
        )
        query_pg = """
        SELECT 'Empresa 02' as empresa, pf.nome as nome_vendedor, sum(valor) as total_vendas
        FROM vendas.nota_fiscal nf
        JOIN geral.pessoa_fisica pf ON pf.id = nf.id_vendedor
        GROUP BY pf.nome
        """
        with warnings.catch_warnings():
            warnings.simplefilter('ignore', UserWarning)
            df_pg = pd.read_sql(query_pg, conn_pg)
            
        log(f'Dados PostgreSQL extraídos: {len(df_pg)} registros')
    except Exception as e:
        log(f'Erro ao extrair do PostgreSQL: {e}', 'ERROR')
        raise
    finally:
        if 'conn_pg' in locals() and not conn_pg.closed:
            conn_pg.close()

    # 3. Consolidar DataFrames
    log('Consolidando resultados das análises...')
    df_uniao = pd.concat([df_pg, df_mysql], ignore_index=True)
    return df_uniao

def configurar_banco_armazenamento():
    """
    Cria uma conexão com o banco de dados PostgreSQL destino (Neon.tech).
    A estrutura atende ao requisito de armazenar o resultado da análise de forma desacoplada.
    """
    url_banco = os.getenv('NEON_DATABASE_URL')
    if not url_banco:
        raise ValueError("A variável de ambiente 'NEON_DATABASE_URL' não foi encontrada no arquivo .env.")

    log('Conectando ao banco de dados PostgreSQL (Neon.tech)...')
    conn = psycopg2.connect(url_banco)
    cursor = conn.cursor()
    
    # Criar tabela com chave primária composta (empresa, nome_vendedor)
    # Isso atende ao requisito da idempotência
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS vendas_consolidadas (
            empresa VARCHAR(100),
            nome_vendedor VARCHAR(150),
            total_vendas NUMERIC(15, 2),
            data_atualizacao TIMESTAMP,
            PRIMARY KEY (empresa, nome_vendedor)
        )
    ''')
    conn.commit()
    cursor.close()
    return conn

def carregar_dados_destino(conn, df: pd.DataFrame):
    """
    Salva o DataFrame no banco de dados PostgreSQL.
    Usa 'ON CONFLICT DO UPDATE' e 'execute_values' para inserção rápida em lote.
    """
    log('Iniciando carga de dados em lote no banco de armazenamento PostgreSQL...')
    
    if df.empty:
        log('DataFrame vazio, nenhuma carga será efetuada.')
        return

    cursor = conn.cursor()
    
    query = '''
        INSERT INTO vendas_consolidadas (empresa, nome_vendedor, total_vendas, data_atualizacao)
        VALUES %s
        ON CONFLICT (empresa, nome_vendedor) DO UPDATE 
        SET total_vendas = EXCLUDED.total_vendas,
            data_atualizacao = EXCLUDED.data_atualizacao;
    '''
    
    # Prepara a lista de tuplas com os dados que virão do DataFrame
    lista_valores = [
        (row['empresa'], row['nome_vendedor'], row['total_vendas'])
        for _, row in df.iterrows()
    ]
    
    # O parametro template é responsável por configurar o CURRENT_TIMESTAMP no batch
    psycopg2.extras.execute_values(
        cursor,
        query,
        lista_valores,
        template='(%s, %s, %s, CURRENT_TIMESTAMP)',
        page_size=1000
    )
        
    conn.commit()
    cursor.close()
    registros_processados = len(lista_valores)
    log(f'{registros_processados} registros salvos/atualizados com sucesso na nova estrutura Neon.tech (via Batch Insert).')

def main():
    try:
        log('INÍCIO - Processo de armazenagem de dados desacoplado (PostgreSQL/Neon.tech)')
        
        # Passo 2: Desenvolver código python para ler as análises
        df_analises = extrair_dados_origem()
        
        # Passo 1: Criar um banco de dados para armazenar os resultados
        conn_pg_destino = configurar_banco_armazenamento()
        
        # Passo 3 & 4: Salvar na nova estrutura sem duplicar dados em execuções sucessivas
        carregar_dados_destino(conn_pg_destino, df_analises)
        
        conn_pg_destino.close()
        log('FIM - Processo concluído com sucesso!')
        
    except Exception as e:
        log(f'Falha na execução do processo: {e}', 'ERROR')
        sys.exit(1)

if __name__ == '__main__':
    main()
