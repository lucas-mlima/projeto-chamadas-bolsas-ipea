import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
import datetime
import logging

# logger
logger = logging.getLogger(__name__)

# Diretórios de entrada e saída
input_dir = Path("./data/")
output_dir = Path("./data/")

# Lista os arquivos brutos
parquet_file = sorted(input_dir.glob("chamadas_bolsas_ipea_bronze.parquet"))

def processar_parquet(file_path: Path):
    try:
        logger.info(f"Processando: {file_path}")

        # Lê o arquivo .parquet
        df = pd.read_parquet(file_path)
        

        # SILVER

        # Prepara colunas
        df["numero_chamada"] = df["numero_chamada"].astype(int)
        df["ano_chamada"] = df["ano_chamada"].astype(str)

        # Salva arquivo temporario - que seria a camada silver
        output_file = output_dir / f"chamadas_bolsas_ipea_silver.parquet"

        pq.write_table(pa.Table.from_pandas(df), output_file)
        logger.info(f"Salvo: {output_file}")
        
        
        # GOLD

        # Criando novas colunas
        df[['dt_inicio', 'dt_fim']] = df['periodo_inscricao'].str.split(' à ', expand=True)
        #df['dt_hoje'] = pd.to_datetime(datetime.date.today())
        df['dt_hoje'] = pd.to_datetime(datetime.datetime.now())

        # Converter para datetime se desejar
        df['dt_inicio'] = pd.to_datetime(df['dt_inicio'], format='%d/%m/%Y')
        df['dt_fim'] = pd.to_datetime(df['dt_fim'], format='%d/%m/%Y')

        # Criando novas colunas
        df['edital_aberto'] = (df['dt_hoje'] <= df['dt_fim']).astype(int)

        df['horas_restantes'] = df.apply(
            lambda row: (datetime.datetime.combine(row['dt_fim'], datetime.time(23, 59, 59)) - row['dt_hoje']).total_seconds() / 3600 if row['edital_aberto'] == 1 else 0,
            axis=1
        )

        # Salva arquivo temporario - que seria a camada gold
        output_file = output_dir / f"chamadas_bolsas_ipea_gold.parquet"

        pq.write_table(pa.Table.from_pandas(df), output_file)
        logger.info(f"Salvo: {output_file}")

    except Exception as e:
        logger.error(f"Erro ao processar {file_path.name}: {e}")


def run():
    if not parquet_file:
        logger.warning("Nenhum arquivo .parquet encontrado no diretório de entrada.")
        return

    processar_parquet(parquet_file)


def main():
    run()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    main()

