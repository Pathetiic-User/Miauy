import time
import os
import json
import logging
from datetime import datetime, timedelta
from bot.scripts.banco_animes import main

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('anime_updater.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Arquivo de progresso
PROGRESS_FILE = "progress.json"

def get_last_updated():
    """Lê a última data de atualização do progress.json ou retorna uma data padrão (7 dias atrás)."""
    try:
        if os.path.exists(PROGRESS_FILE):
            with open(PROGRESS_FILE, 'r') as f:
                data = json.load(f)
                return data.get('last_updated', (datetime.now() - timedelta(days=7)).isoformat())
        return (datetime.now() - timedelta(days=7)).isoformat()
    except Exception as e:
        logger.error(f"Erro ao ler progress.json: {e}")
        return (datetime.now() - timedelta(days=7)).isoformat()

def update_progress(last_updated):
    """Salva a data de atualização no progress.json."""
    try:
        with open(PROGRESS_FILE, 'w') as f:
            json.dump({'last_updated': last_updated}, f)
        logger.info(f"Progresso atualizado: last_updated={last_updated}")
    except Exception as e:
        logger.error(f"Erro ao salvar progress.json: {e}")

def run_update_cycle():
    """Executa um ciclo de atualização buscando novos ou atualizados animes."""
    logger.info("=== Iniciando ciclo de atualização ===")
    last_updated = get_last_updated()
    try:
        # Chama main() com modo de atualização e última data
        main(update_mode=True, last_updated=last_updated)
        # Atualiza a data de progresso para agora
        update_progress(datetime.now().isoformat())
        logger.info("Ciclo de atualização concluído com sucesso.")
    except Exception as e:
        logger.error(f"Erro durante o ciclo de atualização: {e}", exc_info=True)

if __name__ == "__main__":
    UPDATE_INTERVAL = 43200  # 12 horas
    logger.info(f"Iniciando updater de animes. Atualizações a cada {UPDATE_INTERVAL / 3600} horas.")
    while True:
        run_update_cycle()
        logger.info(f"Próxima atualização em {UPDATE_INTERVAL / 3600} horas. Dormindo...")
        time.sleep(UPDATE_INTERVAL)