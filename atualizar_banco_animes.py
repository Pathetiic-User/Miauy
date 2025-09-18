import time
import os
import json
import logging

# Configuração de logging para o updater
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('anime_updater.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Importar o main do script de scraping
# O arquivo bando_animes.py está localizado em bot/scripts/
from bot.scripts.bando_animes import main

# Arquivo de progresso
PROGRESS_FILE = "progress.json"

def reset_progress():
    """Reseta o progresso para forçar uma atualização completa na próxima execução"""
    try:
        if os.path.exists(PROGRESS_FILE):
            os.remove(PROGRESS_FILE)
        logger.info("Progresso resetado para forçar atualização completa.")
    except Exception as e:
        logger.error(f"Erro ao resetar progresso: {e}")

def run_update_cycle():
    """Executa uma ciclo de atualização: reseta progresso e roda o main()"""
    logger.info("=== Iniciando ciclo de atualização ===")
    reset_progress()
    try:
        main()
        logger.info("Ciclo de atualização concluído com sucesso.")
    except Exception as e:
        logger.error(f"Erro durante o ciclo de atualização: {e}")

if __name__ == "__main__":
    # Intervalo de 12 horas em segundos (12 * 60 * 60 = 43200)
    UPDATE_INTERVAL = 43200
    
    logger.info(f"Iniciando updater de animes. Atualizações a cada {UPDATE_INTERVAL / 3600} horas.")
    
    while True:
        run_update_cycle()
        logger.info(f"Próxima atualização em {UPDATE_INTERVAL / 3600} horas. Dormindo...")
        time.sleep(UPDATE_INTERVAL)