import os
import psycopg2
import mysql.connector
import pandas as pd
import matplotlib.pyplot as plt
from dotenv import load_dotenv
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage

# Carrega as variáveis de ambiente do arquivo .env
load_dotenv()

def main():
    # --- 1. Conexão com o PostgreSQL (Empresa 02) ---
    conn_pg = psycopg2.connect(
        host=os.getenv('POSTGRES_HOST'),
        database=os.getenv('POSTGRES_DATABASE'),
        user=os.getenv('POSTGRES_USER'),
        password=os.getenv('POSTGRES_PASSWORD'),
        port=os.getenv('POSTGRES_PORT')
    )

    # --- 2. Conexão com o MySQL (Empresa 01) ---
    conn_mysql = mysql.connector.connect(
        host=os.getenv('MYSQL_HOST'),
        database=os.getenv('MYSQL_DATABASE'),
        user=os.getenv('MYSQL_USER'),
        password=os.getenv('MYSQL_PASSWORD')
    )

    # --- 3. Extração de Dados do MySQL ---
    query_mysql = """
    SELECT id_venda, cod_produto, nome_produto, categoria_produto, segmento_produto, 
           marca_produto, cod_vendedor, nome_vendedor, cod_loja, cidade_loja, 
           estado_loja, data_venda, valor_venda
    FROM datadt_curso_python.vendas
    """
    df_mysql = pd.read_sql(query_mysql, conn_mysql)

    # --- 4. Extração de Dados do PostgreSQL ---
    query_pg = """
    SELECT 'Empresa 02' as empresa, date_part('Year', data_venda) as ano, sum(valor) as venda
    FROM vendas.nota_fiscal
    GROUP BY date_part('Year', data_venda)
    """
    df_pg = pd.read_sql(query_pg, conn_pg)

    # --- 5. Processamento dos Dados da Empresa 01 (MySQL) ---
    df_mysql['data_venda'] = pd.to_datetime(df_mysql['data_venda'])
    df_mysql['ano'] = df_mysql['data_venda'].dt.year        
    
    df_vendas_ano_e1 = df_mysql.groupby('ano')['valor_venda'].sum().reset_index()
    df_vendas_ano_e1['empresa'] = 'Empresa 01'

    # --- 6. União dos Dados ---
    # Renomeia coluna do PG para manter consistência
    df_pg_renamed = df_pg.rename(columns={'venda': 'valor_venda'})
    
    # Concatena os DataFrames de ambas as empresas
    df_uniao = pd.concat([df_vendas_ano_e1, df_pg_renamed], ignore_index=True)

    # Exibe o resultado da união no console
    print("Dados Consolidados:")
    print(df_uniao)

    # --- 7. Visualização ---
    df_pivot = df_uniao.pivot(index='ano', columns='empresa', values='valor_venda')

    df_pivot.plot(kind='bar', figsize=(10, 6), logy=True)
    plt.title('Vendas por Ano - Comparação entre Empresas')
    plt.xlabel('Ano')
    plt.ylabel('Valor de Vendas (Escala Logarítmica)')
    plt.xticks(rotation=0)
    plt.legend(title='Empresa')
    plt.tight_layout()
    
    # Salva o gráfico ou exibe
    plt.savefig('comparativo_vendas.png')
    print("\nGráfico salvo como 'comparativo_vendas.png'")
    plt.show()

    # --- 8. Envio de E-mail ---
    email_sender = EmailSender(
        os.getenv('SMTP_SERVER'),
        int(os.getenv('SMTP_PORT')),
        os.getenv('EMAIL_FROM'),
        os.getenv('EMAIL_PASSWORD')
    )
    email_sender.enviar_relatorio(
        os.getenv('EMAIL_TO'),
        df_uniao,
        'comparativo_vendas.png'
    )

    # Fechamento das conexões
    conn_pg.close()
    conn_mysql.close()

class EmailSender:
    def __init__(self, smtp_server, smtp_port, email_from, password):
        self.smtp_server = smtp_server
        self.smtp_port = smtp_port
        self.email_from = email_from
        self.password = password
    
    def enviar_relatorio(self, email_to, df_uniao, grafico_path):
        try:
            msg = MIMEMultipart()
            msg['From'] = self.email_from
            msg['To'] = email_to
            msg['Subject'] = 'Relatório de Vendas - Comparação entre Empresas'
            
            html_body = f"""
            <html>
                <head>
                    <style>
                        body {{
                            font-family: Arial, sans-serif;
                            background-color: #f4f4f4;
                            padding: 20px;
                        }}
                        .container {{
                            background-color: white;
                            padding: 30px;
                            border-radius: 10px;
                            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
                        }}
                        h2 {{
                            color: #2c3e50;
                            border-bottom: 3px solid #3498db;
                            padding-bottom: 10px;
                        }}
                        table {{
                            border-collapse: collapse;
                            width: 100%;
                            margin-top: 20px;
                            box-shadow: 0 2px 5px rgba(0,0,0,0.1);
                        }}
                        th {{
                            background-color: #3498db;
                            color: white;
                            padding: 12px;
                            text-align: left;
                            font-weight: bold;
                        }}
                        td {{
                            padding: 10px;
                            border-bottom: 1px solid #ddd;
                        }}
                        tr:hover {{
                            background-color: #f5f5f5;
                        }}
                        tr:nth-child(even) {{
                            background-color: #f9f9f9;
                        }}
                        .footer {{
                            margin-top: 30px;
                            color: #7f8c8d;
                            font-size: 12px;
                        }}
                    </style>
                </head>
                <body>
                    <div class="container">
                        <h2>📊 Relatório de Vendas por Ano</h2>
                        <p>Segue abaixo os dados consolidados das vendas por empresa:</p>
                        {df_uniao.to_html(index=False, border=0)}
                        <div class="footer">
                            <p>Relatório gerado automaticamente | Gráfico em anexo</p>
                        </div>
                    </div>
                </body>
            </html>
            """
            
            msg.attach(MIMEText(html_body, 'html'))
            
            with open(grafico_path, 'rb') as f:
                img = MIMEImage(f.read())
                img.add_header('Content-Disposition', 'attachment', filename='comparativo_vendas.png')
                msg.attach(img)
            
            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.starttls()
                server.login(self.email_from, self.password)
                server.send_message(msg)
            
            print(f"E-mail enviado com sucesso para {email_to}")
        except Exception as e:
            print(f"Erro ao enviar e-mail: {e}")

if __name__ == "__main__":
    main()