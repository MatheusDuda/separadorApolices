import os
import re
import zipfile
import pandas as pd
from pypdf import PdfReader, PdfWriter
import logging
from datetime import datetime
import shutil
import tempfile
from glob import glob
import tkinter as tk
from tkinter import filedialog
from tqdm import tqdm # Importa a biblioteca da barra de progresso

# ==============================================================================
# --- ÁREA DE CONFIGURAÇÃO PRINCIPAL ---
# ==============================================================================
CONFIG = {
    "PASTA_SAIDA_NOME": "Apolices_Processadas",
    "TEMPLATE_CERT_ZIP": "CERTIFICADOS_{identificador}.zip",
    "TEMPLATE_PLAN_ZIP": "PLANILHAS {identificador}.zip",
    "TEMPLATE_PDF_INTERNO": "CERTIFICADOS_{tipo}_{identificador}.pdf",
    "TEMPLATE_PLANILHA_INTERNA": "{tipo}_1X {identificador}.csv",
    "TIPO_ARQUIVO_PLANILHA": "csv",
    "CSV_DELIMITADOR": ";",
    "ENCODING_CSV": "latin-1",
    "PAGINAS_POR_APOLICE": 3,
    "MAPEAMENTO_COLUNAS": {
        "loccodigo": "ATIVIDADE",
        "nome_segurado": "NOMESEGURADOITEM",
        "cnpj": "CNPJ"
    },
    "PADRAO_NOME_ARQUIVO_SAIDA": "{loccodigo}.pdf"
}
# ==============================================================================
# --- FIM DA CONFIGURAÇÃO ---
# ==============================================================================

# Funções auxiliares (selecionar_pasta_matriz, configurar_log, etc. continuam iguais)
def selecionar_pasta_matriz():
    root = tk.Tk()
    root.withdraw()
    print("Por favor, selecione a pasta matriz que contém os arquivos ZIP...")
    pasta_selecionada = filedialog.askdirectory(title="Selecione a Pasta Matriz (a que contém os arquivos ZIP)")
    root.destroy()
    return pasta_selecionada

def configurar_log():
    pasta_saida = CONFIG["PASTA_SAIDA_GERAL"]
    os.makedirs(pasta_saida, exist_ok=True)
    log_filename = os.path.join(pasta_saida, f"log_geral_{datetime.now():%Y%m%d_%H%M%S}.txt")
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', handlers=[logging.FileHandler(log_filename, 'w', 'utf-8'), logging.StreamHandler()])

def sanitizar_nome_arquivo(nome):
    return re.sub(r'[\\/*?:"<>|]', "", str(nome))

def encontrar_lotes_processamento():
    pasta_raiz = CONFIG["PASTA_RAIZ"]
    logging.info(f"Buscando lotes de processamento em: {pasta_raiz}")
    lotes = {}
    prefixo_cert = CONFIG["TEMPLATE_CERT_ZIP"].split('{')[0]
    sufixo_cert = ".zip"
    cert_zips = glob(os.path.join(pasta_raiz, f"{prefixo_cert}*{sufixo_cert}"))
    if not cert_zips:
        logging.warning("Nenhum arquivo ZIP de certificados encontrado.")
        return lotes
    for cert_zip_path in cert_zips:
        nome_arquivo_cert = os.path.basename(cert_zip_path)
        try:
            identificador = nome_arquivo_cert[len(prefixo_cert):-len(sufixo_cert)]
            nome_planilha_zip = CONFIG["TEMPLATE_PLAN_ZIP"].format(identificador=identificador)
            planilha_zip_esperado = os.path.join(pasta_raiz, nome_planilha_zip)
            if os.path.exists(planilha_zip_esperado):
                lotes[identificador] = {"cert_zip": cert_zip_path, "plan_zip": planilha_zip_esperado}
        except Exception as e:
            logging.error(f"Erro ao processar o nome do arquivo '{nome_arquivo_cert}': {e}")
    return lotes

def encontrar_linha_cabecalho(caminho_planilha):
    try:
        with open(caminho_planilha, 'r', encoding=CONFIG["ENCODING_CSV"]) as f:
            for i, line in enumerate(f):
                if line.strip().upper().startswith('CNPJ'): return i
    except Exception: return None
    return None

def processar_pdf_individual(pdf_path, planilha_path, identificador, tipo, relatorio_geral):
    try:
        linha_cabecalho = encontrar_linha_cabecalho(planilha_path)
        if linha_cabecalho is None:
            logging.error(f"Cabeçalho 'CNPJ' não encontrado em '{os.path.basename(planilha_path)}'.")
            return
        
        df_planilha = pd.read_csv(planilha_path, delimiter=CONFIG["CSV_DELIMITADOR"], encoding=CONFIG["ENCODING_CSV"], header=linha_cabecalho)
        df_planilha.dropna(axis=1, how='all', inplace=True); df_planilha.dropna(axis=0, how='all', inplace=True)
        
        leitor_pdf = PdfReader(pdf_path); num_paginas_pdf, num_linhas_planilha = len(leitor_pdf.pages), len(df_planilha)
        paginas_por_apolice = CONFIG["PAGINAS_POR_APOLICE"]

        if num_paginas_pdf % paginas_por_apolice != 0 or (num_paginas_pdf // paginas_por_apolice) != num_linhas_planilha:
            msg = f"Validação falhou para '{os.path.basename(pdf_path)}': PDF ({num_paginas_pdf // paginas_por_apolice} apólices) e planilha ({num_linhas_planilha} linhas) não batem."
            logging.error(msg); relatorio_geral.append({"Lote": identificador, "Tipo": tipo, "Status": "Erro de Validação", "Detalhe": msg}); return

        # Adiciona a barra de progresso aqui!
        for index, linha in tqdm(df_planilha.iterrows(), total=num_linhas_planilha, desc=f"Processando {os.path.basename(pdf_path)}"):
            apolice_info = {"Lote": identificador, "Tipo": tipo, "Arquivo Origem": os.path.basename(planilha_path)}
            try:
                colunas_mapeadas = CONFIG["MAPEAMENTO_COLUNAS"]
                if not all(col in df_planilha.columns for col in colunas_mapeadas.values()):
                    raise ValueError(f"Uma ou mais colunas mapeadas não foram encontradas na planilha.")

                dados = {chave: linha.get(valor) for chave, valor in colunas_mapeadas.items()}
                cnpj = sanitizar_nome_arquivo(dados.get("cnpj", "CNPJ_NAO_INFORMADO"))
                nome_segurado = sanitizar_nome_arquivo(dados.get("nome_segurado", "NOME_NAO_INFORMADO"))
                loccodigo_bruto = sanitizar_nome_arquivo(dados.get("loccodigo", "CODIGO_NAO_INFORMADO"))

                if not all([loccodigo_bruto, nome_segurado]) or loccodigo_bruto == "CODIGO_NAO_INFORMADO" or nome_segurado == "NOME_NAO_INFORMADO":
                    raise ValueError("Dados essenciais (ATIVIDADE, NOMESEGURADOITEM) estão faltando na linha.")

                loccodigo_formatado = loccodigo_bruto.lstrip('T0')
                if not loccodigo_formatado.isdigit():
                    raise ValueError(f"Código de contrato '{loccodigo_bruto}' não é numérico após formatação.")

                nome_final = CONFIG["PADRAO_NOME_ARQUIVO_SAIDA"].format(loccodigo=loccodigo_formatado)
                pasta_destino = os.path.join(CONFIG["PASTA_SAIDA_GERAL"], identificador, tipo)
                os.makedirs(pasta_destino, exist_ok=True)
                
                escritor = PdfWriter(); pag_inicial = index * paginas_por_apolice
                for i in range(paginas_por_apolice): escritor.add_page(leitor_pdf.pages[pag_inicial + i])
                with open(os.path.join(pasta_destino, nome_final), "wb") as f_out: escritor.write(f_out)
                
                apolice_info.update({"Status": "Sucesso", "Arquivo Gerado": nome_final, "CNPJ": cnpj, "Detalhe": ""})
            except Exception as e:
                apolice_info.update({"Status": "Erro", "Detalhe": str(e)})
            relatorio_geral.append(apolice_info)
    except Exception as e:
        logging.critical(f"Erro fatal ao processar o par de arquivos. Detalhe: {e}")

def main():
    pasta_matriz = selecionar_pasta_matriz()
    if not pasta_matriz: print("Nenhuma pasta foi selecionada. O programa será encerrado."); return
        
    CONFIG["PASTA_RAIZ"] = pasta_matriz
    CONFIG["PASTA_SAIDA_GERAL"] = os.path.join(pasta_matriz, CONFIG["PASTA_SAIDA_NOME"])
    
    configurar_log()
    lotes = encontrar_lotes_processamento()
    if not lotes: logging.info("Nenhum lote válido para processar."); input("Pressione Enter para fechar..."); return
        
    relatorio_geral = []
    with tempfile.TemporaryDirectory(dir=CONFIG["PASTA_RAIZ"], prefix="extracao_temp_") as temp_dir:
        for identificador, caminhos in lotes.items():
            logging.info(f"\n{'='*20} PROCESSANDO LOTE: {identificador.upper()} {'='*20}")
            try:
                dir_certs, dir_plans = os.path.join(temp_dir, f"{identificador}_c"), os.path.join(temp_dir, f"{identificador}_p")
                with zipfile.ZipFile(caminhos["cert_zip"], 'r') as zr: zr.extractall(dir_certs)
                with zipfile.ZipFile(caminhos["plan_zip"], 'r') as zr: zr.extractall(dir_plans)

                pdfs = glob(os.path.join(dir_certs, CONFIG["TEMPLATE_PDF_INTERNO"].format(identificador=identificador,tipo="*")))
                for pdf_path in pdfs:
                    nome_pdf, tipo_arquivo = os.path.basename(pdf_path), ""
                    match = re.search(r'CERTIFICADOS_(.*?)_', nome_pdf, re.IGNORECASE)
                    if match: tipo_arquivo = match.group(1).upper()
                    else: continue
                    
                    nome_planilha = CONFIG["TEMPLATE_PLANILHA_INTERNA"].format(tipo=tipo_arquivo, identificador=identificador)
                    planilha_path = os.path.join(dir_plans, nome_planilha)
                    
                    if os.path.exists(planilha_path): processar_pdf_individual(pdf_path, planilha_path, identificador, tipo_arquivo, relatorio_geral)
                    else: logging.warning(f"PDF '{nome_pdf}' encontrado, mas sua planilha par '{nome_planilha}' não foi localizada.")
            except Exception as e:
                logging.critical(f"Erro inesperado no lote '{identificador}': {e}")
    
    # Resumo Final e Abertura da Pasta
    if relatorio_geral:
        df_relatorio = pd.DataFrame(relatorio_geral)
        sucessos = df_relatorio[df_relatorio['Status'] == 'Sucesso'].shape[0]
        erros = df_relatorio.shape[0] - sucessos
        
        logging.info("\n" + "="*50)
        logging.info("PROCESSO FINALIZADO!")
        logging.info(f"✔  {sucessos} apólices processadas com sucesso.")
        logging.info(f"❌ {erros} apólices com erro.")
        
        caminho_relatorio = os.path.join(CONFIG['PASTA_SAIDA_GERAL'], "relatorio_final.xlsx")
        df_relatorio.to_excel(caminho_relatorio, index=False)
        logging.info(f"Relatório detalhado salvo em: {caminho_relatorio}")
        
        if erros == 0 and sucessos > 0:
            logging.info("Abrindo a pasta de resultados...")
            os.startfile(CONFIG["PASTA_SAIDA_GERAL"]) # Abre a pasta no Windows Explorer

if __name__ == "__main__":
    main()
    input("\nPressione Enter para fechar...")