import time
import logging
import importlib
import traceback

# --- Configurações --- 
SCRAPER_MODULE_NAME = "webscrapper_editais"  # Nome do arquivo .py sem a extensão
INTERVAL_HOURS = 6
INTERVAL_SECONDS = INTERVAL_HOURS * 60 * 60

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Função Principal do Agendador --- 

def run_scheduler():
    """Executa o scraper periodicamente."""
    try:
        logger.info(f"Importando o módulo do scraper: {SCRAPER_MODULE_NAME}")
        scraper_module = importlib.import_module(SCRAPER_MODULE_NAME)
        logger.info("Módulo importado com sucesso.")
    except ImportError as e:
        logger.critical(f"Falha ao importar o módulo {SCRAPER_MODULE_NAME}.py: {e}")
        logger.critical("Verifique se o arquivo está no mesmo diretório ou no PYTHONPATH.")
        return # Encerra se não conseguir importar
    except Exception as e:
        logger.critical(f"Erro inesperado ao tentar importar {SCRAPER_MODULE_NAME}.py: {e}")
        return

    if not hasattr(scraper_module, 'main'): # Corrigido: aspas simples
        logger.critical(f"O módulo {SCRAPER_MODULE_NAME}.py não possui uma função 'main()'.") # Corrigido: aspas simples
        return

    logger.info(f"Agendador iniciado. Executando {SCRAPER_MODULE_NAME}.py a cada {INTERVAL_HOURS} hora(s).")
    logger.info("Pressione Ctrl+C para parar.")

    while True:
        try:
            logger.info(f"--- Iniciando execução do {SCRAPER_MODULE_NAME}.main() ---")
            # Chama a função main do scraper importado
            scraper_module.main()
            logger.info(f"--- Execução do {SCRAPER_MODULE_NAME}.main() concluída com sucesso ---")

        except Exception as e:
            logger.error(f"!!! Erro durante a execução do {SCRAPER_MODULE_NAME}.main(): {e} !!!")
            logger.error(traceback.format_exc()) # Loga o traceback completo do erro
            logger.info("Continuando para a próxima execução agendada apesar do erro.")

        try:
            logger.info(f"Aguardando {INTERVAL_HOURS} hora(s) para a próxima execução...")
            time.sleep(INTERVAL_SECONDS)
        except KeyboardInterrupt:
            # Captura Ctrl+C durante o sleep também
            logger.info("Interrupção recebida durante a espera. Encerrando o agendador...")
            break # Sai do loop while

# --- Execução --- 
if __name__ == "__main__":
    try:
        run_scheduler()
    except KeyboardInterrupt:
        logger.info("Agendador encerrado pelo usuário (Ctrl+C).")
    except Exception as e:
        logger.critical(f"Erro crítico no agendador: {e}")
        logger.critical(traceback.format_exc())