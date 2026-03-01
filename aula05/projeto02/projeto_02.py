import psycopg2
from dotenv import load_dotenv
import os
import mysql.connector
import pandas as pd
import matplotlib.pyplot as plt
from email_sender import EmailSender
import sys
from datetime import datetime


def log(mensagem, tipo='INFO'):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f'[{timestamp}] [{tipo}] {mensagem}')


try:
    log('Iniciando processo de geração de relatório')
    load_dotenv()
    log('Variáveis de ambiente carregadas')
    
    # Conexão PostgreSQL
    log('Conectando ao PostgreSQL...')
    conn = psycopg2.connect(
        host=os.getenv('POSTGRES_HOST'),
        database=os.getenv('POSTGRES_DATABASE'),
        user=os.getenv('POSTGRES_USER'),
        password=os.getenv('POSTGRES_PASSWORD'),
        port=os.getenv('POSTGRES_PORT')
    )
    log('Conexão PostgreSQL estabelecida com sucesso')
    
    # Conexão MySQL
    log('Conectando ao MySQL...')
    conn_mysql = mysql.connector.connect(
        host=os.getenv('MYSQL_HOST'),
        database=os.getenv('MYSQL_DATABASE'),
        user=os.getenv('MYSQL_USER'),
        password=os.getenv('MYSQL_PASSWORD')
    )
    log('Conexão MySQL estabelecida com sucesso')
    
    # Consulta MySQL
    log('Executando consulta no MySQL...')
    query = """
    SELECT 'Empresa 01' as empresa, nome_vendedor, sum(valor_venda) as total_vendas        
    FROM datadt_curso_python.vendas
    GROUP BY nome_vendedor
    """
    df = pd.read_sql(query, conn_mysql)
    log(f'Dados MySQL obtidos: {len(df)} registros')
    
    # Consulta PostgreSQL
    log('Executando consulta no PostgreSQL...')
    query_pg = """
    SELECT 'Empresa 02' as empresa, pf.nome as nome_vendedor, sum(valor) as total_vendas
    FROM vendas.nota_fiscal nf
    JOIN geral.pessoa_fisica pf ON pf.id = nf.id_vendedor
    GROUP BY pf.nome
    """
    df_pg = pd.read_sql(query_pg, conn)
    log(f'Dados PostgreSQL obtidos: {len(df_pg)} registros')
    
    # União dos dados
    log('Consolidando dados...')
    df_uniao = pd.concat([df_pg, df], ignore_index=True)
    log(f'Total de registros consolidados: {len(df_uniao)}')
    print('\n' + '='*50)
    print(df_uniao)
    print('='*50 + '\n')
    
    # Criar gráfico
    log('Gerando gráfico de barras...')
    plt.figure(figsize=(10, 6))
    df_grouped = df_uniao.groupby('empresa')['total_vendas'].sum()
    df_grouped.plot(kind='bar', color=['#3498db', '#2ecc71'])
    plt.title('Total de Vendas por Empresa', fontsize=16, fontweight='bold')
    plt.xlabel('Empresa', fontsize=12)
    plt.ylabel('Total de Vendas (R$)', fontsize=12)
    plt.xticks(rotation=0)
    plt.tight_layout()
    caminho_grafico = 'grafico_vendas.png'
    plt.savefig(caminho_grafico, dpi=300, bbox_inches='tight')
    plt.close()
    log(f'Gráfico salvo em: {caminho_grafico}')
    
    # Enviar email
    log('Preparando envio de email...')
    email_sender = EmailSender(
        smtp_server=os.getenv('SMTP_SERVER'),
        smtp_port=int(os.getenv('SMTP_PORT')),
        email=os.getenv('EMAIL_USER'),
        password=os.getenv('EMAIL_PASSWORD')
    )
    
    email_sender.enviar_relatorio(
        destinatario=os.getenv('EMAIL_DESTINATARIO'),
        assunto='Relatório de Vendas - Consolidado',
        df_uniao=df_uniao,
        caminho_grafico=caminho_grafico
    )
    log('Email enviado com sucesso!', 'SUCCESS')
    
except psycopg2.Error as e:
    log(f'Erro na conexão/consulta PostgreSQL: {e}', 'ERROR')
    sys.exit(1)
except mysql.connector.Error as e:
    log(f'Erro na conexão/consulta MySQL: {e}', 'ERROR')
    sys.exit(1)
except FileNotFoundError as e:
    log(f'Arquivo não encontrado: {e}', 'ERROR')
    sys.exit(1)
except Exception as e:
    log(f'Erro inesperado: {e}', 'ERROR')
    sys.exit(1)
finally:
    # Fechar conexões
    try:
        if 'conn' in locals() and conn:
            conn.close()
            log('Conexão PostgreSQL fechada')
    except:
        pass
    try:
        if 'conn_mysql' in locals() and conn_mysql:
            conn_mysql.close()
            log('Conexão MySQL fechada')
    except:
        pass
    log('Processo finalizado')