import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
import os
import pandas as pd
from datetime import datetime


class EmailSender:
    def __init__(self, smtp_server, smtp_port, email, password):
        self.smtp_server = smtp_server
        self.smtp_port = smtp_port
        self.email = email
        self.password = password
    
    def _log(self, mensagem, tipo='INFO'):
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f'[{timestamp}] [EmailSender] [{tipo}] {mensagem}')
    
    def enviar_relatorio(self, destinatario, assunto, df_uniao, caminho_grafico):
        try:
            self._log(f'Iniciando envio de email para {destinatario}')
            
            msg = MIMEMultipart()
            msg['From'] = self.email
            msg['To'] = destinatario
            msg['Subject'] = assunto
            self._log('Cabeçalho do email configurado')
            
            # Corpo do email com o DataFrame
            html = f"""
            <!DOCTYPE html>
            <html>
                <head>
                    <meta charset="UTF-8">
                    <style>
                        body {{
                            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                            padding: 40px 20px;
                            margin: 0;
                        }}
                        .container {{
                            background-color: #ffffff;
                            padding: 40px;
                            border-radius: 15px;
                            box-shadow: 0 10px 40px rgba(0,0,0,0.2);
                            max-width: 900px;
                            margin: 0 auto;
                        }}
                        .header {{
                            text-align: center;
                            margin-bottom: 30px;
                            padding-bottom: 20px;
                            border-bottom: 4px solid #667eea;
                        }}
                        h1 {{
                            color: #2c3e50;
                            font-size: 32px;
                            margin: 0 0 10px 0;
                            font-weight: 700;
                        }}
                        .subtitle {{
                            color: #7f8c8d;
                            font-size: 16px;
                            margin: 0;
                        }}
                        .intro {{
                            background: linear-gradient(135deg, #667eea15 0%, #764ba215 100%);
                            padding: 20px;
                            border-radius: 10px;
                            margin: 20px 0;
                            border-left: 5px solid #667eea;
                        }}
                        .intro p {{
                            margin: 0;
                            color: #34495e;
                            font-size: 15px;
                            line-height: 1.6;
                        }}
                        table {{
                            border-collapse: collapse;
                            width: 100%;
                            margin: 25px 0;
                            box-shadow: 0 5px 15px rgba(0,0,0,0.08);
                            border-radius: 10px;
                            overflow: hidden;
                        }}
                        th {{
                            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                            color: white;
                            padding: 15px;
                            text-align: left;
                            font-weight: 600;
                            font-size: 14px;
                            text-transform: uppercase;
                            letter-spacing: 0.5px;
                        }}
                        td {{
                            padding: 15px;
                            border-bottom: 1px solid #ecf0f1;
                            color: #2c3e50;
                            font-size: 14px;
                        }}
                        tr:hover {{
                            background-color: #f8f9fa;
                            transform: scale(1.01);
                            transition: all 0.2s ease;
                        }}
                        tr:nth-child(even) {{
                            background-color: #f9fafb;
                        }}
                        .attachment-box {{
                            background: linear-gradient(135deg, #f093fb15 0%, #f5576c15 100%);
                            padding: 20px;
                            border-radius: 10px;
                            margin: 25px 0;
                            text-align: center;
                            border: 2px dashed #667eea;
                        }}
                        .attachment-box p {{
                            margin: 0;
                            color: #34495e;
                            font-size: 15px;
                            font-weight: 600;
                        }}
                        .footer {{
                            margin-top: 40px;
                            padding-top: 25px;
                            border-top: 3px solid #ecf0f1;
                            text-align: center;
                        }}
                        .footer p {{
                            color: #95a5a6;
                            font-size: 13px;
                            margin: 5px 0;
                        }}
                        .badge {{
                            display: inline-block;
                            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                            color: white;
                            padding: 5px 15px;
                            border-radius: 20px;
                            font-size: 12px;
                            font-weight: 600;
                            margin-top: 10px;
                        }}
                    </style>
                </head>
                <body>
                    <div class="container">
                        <div class="header">
                            <h1>📊 Relatório de Vendas</h1>
                            <p class="subtitle">Análise Consolidada de Performance</p>
                            <span class="badge">{pd.Timestamp.now().strftime('%d/%m/%Y às %H:%M')}</span>
                        </div>
                        
                        <div class="intro">
                            <p>🎯 <strong>Resumo Executivo:</strong> Segue abaixo o consolidado das vendas por vendedor e empresa. Os dados apresentam o desempenho total de cada colaborador.</p>
                        </div>
                        
                        {df_uniao.to_html(index=False, border=0, classes='data-table')}
                        
                        <div class="attachment-box">
                            <p>📈 <strong>Gráfico de Análise em Anexo</strong></p>
                            <p style="font-size: 13px; color: #7f8c8d; margin-top: 8px;">Visualização gráfica das vendas por empresa</p>
                        </div>
                        
                        <div class="footer">
                            <p><strong>Relatório Automatizado</strong></p>
                            <p>Este email foi gerado automaticamente pelo sistema de análise de vendas</p>
                            <p style="margin-top: 15px; font-size: 11px;">© {pd.Timestamp.now().year} - Sistema de Gestão de Vendas</p>
                        </div>
                    </div>
                </body>
            </html>
            """
            msg.attach(MIMEText(html, 'html'))
            self._log('Corpo HTML do email criado')
            
            # Anexar gráfico
            if not os.path.exists(caminho_grafico):
                self._log(f'Arquivo não encontrado: {caminho_grafico}', 'ERROR')
                raise FileNotFoundError(f'Arquivo não encontrado: {caminho_grafico}')
            
            self._log(f'Anexando gráfico: {caminho_grafico}')
            with open(caminho_grafico, 'rb') as f:
                img = MIMEImage(f.read())
                img.add_header('Content-Disposition', 'attachment', filename=os.path.basename(caminho_grafico))
                msg.attach(img)
            self._log('Gráfico anexado com sucesso')
            
            # Enviar email
            self._log(f'Conectando ao servidor SMTP: {self.smtp_server}:{self.smtp_port}')
            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.starttls()
                self._log('Conexão TLS estabelecida')
                server.login(self.email, self.password)
                self._log('Autenticação realizada com sucesso')
                server.send_message(msg)
                self._log('Email enviado com sucesso', 'SUCCESS')
                
        except FileNotFoundError as e:
            self._log(f'Arquivo de gráfico não encontrado: {e}', 'ERROR')
            raise Exception(f'Arquivo de gráfico não encontrado: {e}')
        except smtplib.SMTPAuthenticationError as e:
            self._log(f'Falha na autenticação: {e}', 'ERROR')
            raise Exception('Falha na autenticação do email. Verifique usuário e senha')
        except smtplib.SMTPException as e:
            self._log(f'Erro SMTP: {e}', 'ERROR')
            raise Exception(f'Erro ao enviar email: {e}')
        except Exception as e:
            self._log(f'Erro inesperado: {e}', 'ERROR')
            raise Exception(f'Erro inesperado ao enviar email: {e}')
