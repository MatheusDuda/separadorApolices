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

# ==============================================================================
# --- ÁREA DE CONFIGURAÇÃO PRINCIPAL ---
# ==============================================================================
CONFIG = {
    "PASTA_RAIZ": r"C:\Users\Matheus Duda\Downloads\thais",
    "PASTA_SAIDA_GERAL": r"C:\Users\Matheus Duda\Downloads\thais\Apolices_Processadas",
    "TEMPLATE_CERT_ZIP": "CERTIFICADOS_{identificador}.zip",
    "TEMPLATE_PLAN_ZIP": "PLANILHAS {identificador}.zip",
    "TEMPLATE_PDF_INTERNO": "CERTIFICADOS_{tipo}_{identificador}.pdf",
    "TEMPLATE_PLANILHA_INTERNA": "{tipo}_1X {identificador}.csv",

    # --- CONFIGURAÇÕES DE LEITURA ---
    "TIPO_ARQUIVO_PLANILHA": "csv",
    "CSV_DELIMITADOR": ";",
    "ENCODING_CSV": "latin-1",

    # --- CONFIGURAÇÕES DA APÓLICE ---
    "PAGINAS_POR_APOLICE": 3,
    "MAPEAMENTO_COLUNAS": {
        "loccodigo": "ATIVIDADE",
        "nome_segurado": "NOMESEGURADOITEM", # <-- AJUSTADO AQUI
        "cnpj": "CNPJ"
    },
    # Padrão de nome simplificado para usar apenas colunas confirmadas
    "PADRAO_NOME_ARQUIVO_SAIDA": "{nome_segurado}_{loccodigo}.pdf"
}
# ==============================================================================
# --- FIM DA CONFIGURAÇÃO ---
# ==============================================================================


def configurar_log():
    os.makedirs(CONFIG["PASTA_SAIDA_GERAL"], exist_ok=True)
    log_filename = os.path.join(CONFIG["PASTA_SAIDA_GERAL"], f"log_geral_{datetime.now():%Y%m%d_%H%M%S}.txt")
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', handlers=[logging.FileHandler(log_filename, 'w', 'utf-8'), logging.StreamHandler()])

def sanitizar_nome_arquivo(nome):
    return re.sub(r'[\\/*?:"<>|]', "", str(nome))

def encontrar_lotes_processamento():
    logging.info(f"Buscando lotes de processamento em: {CONFIG['PASTA_RAIZ']}")
    lotes = {}
    prefixo_cert = CONFIG["TEMPLATE_CERT_ZIP"].split('{')[0]
    sufixo_cert = ".zip"
    cert_zips = glob(os.path.join(CONFIG["PASTA_RAIZ"], f"{prefixo_cert}*{sufixo_cert}"))

    for cert_zip_path in cert_zips:
        nome_arquivo_cert = os.path.basename(cert_zip_path)
        try:
            identificador = nome_arquivo_cert[len(prefixo_cert):-len(sufixo_cert)]
            nome_planilha_zip = CONFIG["TEMPLATE_PLAN_ZIP"].format(identificador=identificador)
            planilha_zip_esperado = os.path.join(CONFIG["PASTA_RAIZ"], nome_planilha_zip)

            if os.path.exists(planilha_zip_esperado):
                logging.info(f"Lote encontrado: '{identificador}' (Certificados: '{nome_arquivo_cert}', Planilhas: '{nome_planilha_zip}')")
                lotes[identificador] = {"cert_zip": cert_zip_path, "plan_zip": planilha_zip_esperado}
            else:
                logging.warning(f"Certificados '{nome_arquivo_cert}' encontrado, mas seu par '{nome_planilha_zip}' não foi localizado. Pulando.")
        except Exception as e:
            logging.error(f"Erro ao processar o nome do arquivo '{nome_arquivo_cert}': {e}")
    return lotes

def encontrar_linha_cabecalho(caminho_planilha):
    try:
        with open(caminho_planilha, 'r', encoding=CONFIG["ENCODING_CSV"]) as f:
            for i, line in enumerate(f):
                if line.strip().upper().startswith('CNPJ'):
                    return i
    except Exception as e:
        logging.error(f"Não foi possível ler o arquivo {os.path.basename(caminho_planilha)} para encontrar o cabeçalho. Erro: {e}")
    return None

def processar_pdf_individual(pdf_path, planilha_path, identificador, tipo, relatorio_geral):
    logging.info(f"--- Processando par: {os.path.basename(pdf_path)} e {os.path.basename(planilha_path)} ---")
    try:
        linha_cabecalho = encontrar_linha_cabecalho(planilha_path)
        if linha_cabecalho is None:
            logging.error(f"Não foi possível encontrar a linha de cabeçalho (iniciando com 'CNPJ') no arquivo '{os.path.basename(planilha_path)}'.")
            return

        logging.info(f"Cabeçalho encontrado na linha {linha_cabecalho + 1} do arquivo CSV.")
        
        df_planilha = pd.read_csv(
            planilha_path,
            delimiter=CONFIG["CSV_DELIMITADOR"],
            encoding=CONFIG["ENCODING_CSV"],
            header=linha_cabecalho
        )
        
        df_planilha.dropna(axis=1, how='all', inplace=True)
        df_planilha.dropna(axis=0, how='all', inplace=True)
        
        leitor_pdf = PdfReader(pdf_path)
        num_paginas_pdf, num_linhas_planilha = len(leitor_pdf.pages), len(df_planilha)
        paginas_por_apolice = CONFIG["PAGINAS_POR_APOLICE"]

        if num_paginas_pdf % paginas_por_apolice != 0 or (num_paginas_pdf // paginas_por_apolice) != num_linhas_planilha:
            logging.error(f"Validação falhou para '{os.path.basename(pdf_path)}': PDF tem {num_paginas_pdf // paginas_por_apolice} apólices e planilha tem {num_linhas_planilha} linhas.")
            relatorio_geral.append({"Lote": identificador, "Tipo": tipo, "Status": "Erro de Validação", "Detalhe": f"PDF: {num_paginas_pdf // paginas_por_apolice} apólices, Planilha: {num_linhas_planilha} linhas."})
            return

        for index, linha in df_planilha.iterrows():
            apolice_info = {"Lote": identificador, "Tipo": tipo, "Arquivo Origem": os.path.basename(planilha_path)}
            cnpj = ""
            try:
                colunas_mapeadas = CONFIG["MAPEAMENTO_COLUNAS"]
                for chave, nome_coluna in colunas_mapeadas.items():
                    if nome_coluna not in df_planilha.columns:
                        raise ValueError(f"Coluna obrigatória '{nome_coluna}' não encontrada na planilha.")

                dados = {chave: linha.get(valor) for chave, valor in colunas_mapeadas.items()}
                cnpj = sanitizar_nome_arquivo(dados.get("cnpj", "CNPJ_NAO_INFORMADO"))
                nome_segurado = sanitizar_nome_arquivo(dados.get("nome_segurado", "NOME_NAO_INFORMADO"))
                loccodigo = sanitizar_nome_arquivo(dados.get("loccodigo", "CODIGO_NAO_INFORMADO"))

                if not all([loccodigo, nome_segurado]) or loccodigo == "CODIGO_NAO_INFORMADO" or nome_segurado == "NOME_NAO_INFORMADO":
                    raise ValueError("Dados essenciais (ATIVIDADE, NOMESEGURADOITEM) estão faltando na linha.")

                nome_final = CONFIG["PADRAO_NOME_ARQUIVO_SAIDA"].format(nome_segurado=nome_segurado, loccodigo=loccodigo)
                pasta_destino = os.path.join(CONFIG["PASTA_SAIDA_GERAL"], identificador, tipo, cnpj)
                os.makedirs(pasta_destino, exist_ok=True)
                
                escritor = PdfWriter()
                for i in range(paginas_por_apolice):
                    escritor.add_page(leitor_pdf.pages[index * paginas_por_apolice + i])
                with open(os.path.join(pasta_destino, nome_final), "wb") as f_out:
                    escritor.write(f_out)
                
                apolice_info.update({"Status": "Sucesso", "Arquivo Gerado": nome_final, "CNPJ": cnpj, "Detalhe": ""})
            except Exception as e:
                logging.error(f"Erro na linha de dados {index + 1} da planilha '{os.path.basename(planilha_path)}': {e}")
                apolice_info.update({"Status": "Erro", "Arquivo Gerado": "", "CNPJ": cnpj, "Detalhe": str(e)})
            relatorio_geral.append(apolice_info)
    except Exception as e:
        logging.critical(f"Erro fatal ao processar o par de arquivos. Detalhe: {e}")
        relatorio_geral.append({"Lote": identificador, "Tipo": tipo, "Status": "Erro Fatal", "Detalhe": str(e)})

def main():
    configurar_log()
    lotes = encontrar_lotes_processamento()
    if not lotes: return
    relatorio_geral = []
    with tempfile.TemporaryDirectory(dir=CONFIG["PASTA_RAIZ"], prefix="extracao_temp_") as temp_dir:
        logging.info(f"Usando pasta temporária: {temp_dir}")
        for identificador, caminhos in lotes.items():
            try:
                logging.info(f"\n{'='*20} PROCESSANDO LOTE: {identificador.upper()} {'='*20}")
                dir_certs, dir_plans = os.path.join(temp_dir, f"{identificador}_c"), os.path.join(temp_dir, f"{identificador}_p")
                with zipfile.ZipFile(caminhos["cert_zip"], 'r') as zr: zr.extractall(dir_certs)
                with zipfile.ZipFile(caminhos["plan_zip"], 'r') as zr: zr.extractall(dir_plans)
                logging.info(f"Arquivos de '{identificador}' extraídos com sucesso.")

                pdfs = glob(os.path.join(dir_certs, CONFIG["TEMPLATE_PDF_INTERNO"].format(identificador=identificador,tipo="*")))
                for pdf_path in pdfs:
                    nome_pdf = os.path.basename(pdf_path)
                    match = re.search(r'CERTIFICADOS_(.*?)_', nome_pdf, re.IGNORECASE)
                    if not match: continue
                    tipo_arquivo = match.group(1).upper()
                    
                    nome_planilha = CONFIG["TEMPLATE_PLANILHA_INTERNA"].format(tipo=tipo_arquivo, identificador=identificador)
                    planilha_path = os.path.join(dir_plans, nome_planilha)
                    
                    if os.path.exists(planilha_path):
                        processar_pdf_individual(pdf_path, planilha_path, identificador, tipo_arquivo, relatorio_geral)
                    else:
                        logging.warning(f"PDF '{nome_pdf}' encontrado, mas sua planilha par '{nome_planilha}' não foi localizada. Pulando.")
            except Exception as e:
                logging.critical(f"Erro inesperado no lote '{identificador}': {e}")
                relatorio_geral.append({"Lote": identificador, "Status": "Erro Fatal no Lote", "Detalhe": str(e)})
    if relatorio_geral:
        df_relatorio = pd.DataFrame(relatorio_geral)
        df_relatorio.to_excel(os.path.join(CONFIG["PASTA_SAIDA_GERAL"], "relatorio_final.xlsx"), index=False)
        logging.info(f"\nRelatório final salvo em: {CONFIG['PASTA_SAIDA_GERAL']}\\relatorio_final.xlsx")
    logging.info("\nProcesso finalizado.")

if __name__ == "__main__":
    main()
    input("Pressione Enter para fechar...")