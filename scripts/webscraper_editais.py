import requests
from bs4 import BeautifulSoup
import pandas as pd
from pathlib import Path
import re
import pyarrow as pa
import pyarrow.parquet as pq
import logging

# logger
logger = logging.getLogger(__name__)

# Diretórios de saída
output_dir = Path("./data/")
output_dir.mkdir(parents=True, exist_ok=True)

# Arquivo de saída
output_path = output_dir / f"chamadas_bolsas_ipea.parquet"


# Funções principais
def extrair_dados_ipea(url):
    """Extrai dados das chamadas públicas da página de bolsas do IPEA.

    Args:
        url (str): A URL da página de bolsas do IPEA.

    Returns:
        pandas.DataFrame: DataFrame com os dados extraídos ou None se ocorrer erro.
    """
    dados_chamadas = []
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }

    try:
        response = requests.get(url, headers=headers, timeout=120)
        response.raise_for_status() # Verifica se houve erro HTTP
        soup = BeautifulSoup(response.text, 'html.parser')

        # Encontra a lista principal que contém as chamadas

        lista_chamadas = soup.find('div', id='resultado_busca_ajax')
        if not lista_chamadas:
            logger.warning("Container #resultado_busca_ajax não encontrado. Tentando fallback com classe antiga.")
            # Tentativa com a classe da imagem original, caso a estrutura varie
            lista_chamadas = soup.find('ul', class_='search-resultsbolsas list-striped')
            if not lista_chamadas:
                logger.error("Não foi possível localizar nenhum container de chamadas.")
                return None
            itens = lista_chamadas.find_all('li')
        else:
             # A estrutura atual parece usar divs dentro do #resultado_busca_ajax
             itens = lista_chamadas.find_all('div', class_='search-item-wrap', recursive=False)

        if not itens:
            logger.warning("Nenhum item de chamada encontrado dentro do container.")
            return None

        logger.info(f"Encontrados {len(itens)} itens de chamada. Começando a extrair os dados de interesse")

        for item in itens:
            numero_chamada = None
            ano_chamada = None
            link_chamada = None
            programa = None
            periodo_inscricao = None
            base_url = "https://www.ipea.gov.br"

            # --- Extração do Título, Link, Número e Ano ---
            titulo_tag = item.find('h4', class_='result-title')
            titulo_texto = None 
            if titulo_tag and titulo_tag.find('a'):
                link_tag = titulo_tag.find('a')
                titulo_texto = link_tag.get_text(strip=True)
                link_chamada = link_tag.get('href')
                if link_chamada and not link_chamada.startswith('http'):
                    link_chamada = base_url + link_chamada

                # Extrair número e ano do título apenas se titulo_texto for valido
                if titulo_texto:
                    match = re.search(r'nº\s*(\d+)/(\d{4})', titulo_texto) # padrão 1: nº XXX/YYYY
                    if not match: # Se primeiro padrão falhar, tentar padrão primeiro alternativo
                        match = re.search(r'Chamada Pública\s*(\d+)/(\d{4})', titulo_texto) # padrão 2: Chamada Pública XXX/YYYY
                    if not match: # Se segundo padrão falhar, tentar fallback amplo
                         match = re.search(r'(\d+)/(\d{4})', titulo_texto) # padrão 3: XXX/YYYY

                    if match: # Se existe alguma correspondência foi encontrada antes de acessar os grupos
                        numero_chamada = match.group(1)
                        ano_chamada = match.group(2)

            # --- Extração dos outros campos (baseado na estrutura da imagem e inspeção) ---
            # A estrutura real pode usar <p> ou <div> para os detalhes
            paragrafos = item.find_all('p')
            if not paragrafos:
                 # Se não houver <p>, tentar com <div> dentro do item
                 paragrafos = item.find_all('div') # Ajuste genérico

            for p in paragrafos:
                strong_tag = p.find('strong')
                if strong_tag:
                    campo = strong_tag.get_text(strip=True).lower()
                    valor = strong_tag.next_sibling
                    if valor:
                         valor = valor.strip()
                         if 'programa:' in campo:
                             programa = valor
                         elif 'prazo de inscrição:' in campo:
                             periodo_inscricao = valor
                         elif 'ano:' in campo and not ano_chamada:
                             # Pega o ano daqui se não conseguiu do título
                             ano_chamada = valor

            # Adiciona os dados extraídos à lista
            dados_chamadas.append({
                'numero_chamada': numero_chamada,
                'ano_chamada': ano_chamada,
                'link_chamada': link_chamada,
                'programa': programa,
                'periodo_inscricao': periodo_inscricao
            })

        # Cria o DataFrame
        df = pd.DataFrame(dados_chamadas)
        return df

    except requests.exceptions.RequestException as e:
        logger.error(f"Erro ao acessar a URL: {e}")
        return None
    except Exception as e:
        logger.error(f"Ocorreu um erro inesperado: {e}")
        return None



def run():
    url_ipea_bolsas = "https://www.ipea.gov.br/portal/bolsas-de-pesquisa"
    logger.info(f"Iniciando scraping da URL: {url_ipea_bolsas}")
    dataframe_chamadas = extrair_dados_ipea(url_ipea_bolsas)

    if dataframe_chamadas is not None:
        logger.info("Preparando para salvar")
        logger.info(f"\n{dataframe_chamadas}")

        # Patronizando a coluna númerica da chamada
        dataframe_chamadas["numero_chamada"] = dataframe_chamadas["numero_chamada"].astype(int)

        # Salvar em parquet
        try:
            pq.write_table(pa.Table.from_pandas(dataframe_chamadas), output_path)
            logger.info("Dataframe salvo com sucesso em 'chamadas_bolsas_ipea_bronze.parquet'")
        except Exception as e:
            logger.error(f"Erro ao salvar o dataframe em .parquet: {e}")
    else:
        logger.warning("Não foi possível gerar o dataframe.")


def main():
    run()

# --- Execução ---
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    main()

