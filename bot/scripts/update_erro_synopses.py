import time
import psycopg2
from psycopg2.pool import SimpleConnectionPool
import json
import logging
import os
from datetime import datetime
from deep_translator import GoogleTranslator
from deep_translator.exceptions import TooManyRequests
from tqdm import tqdm
from dotenv import load_dotenv
import argparse
import sys
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
import backoff

if sys.platform == "win32":
    import codecs
    sys.stdout = codecs.getwriter("utf-8")(sys.stdout.detach())

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('update_failed_synopses.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def load_config():
    parser = argparse.ArgumentParser(description='Atualiza sinopses falhadas no banco de dados')
    parser.add_argument('--database-url', type=str, help='URL do banco de dados (sobrescreve .env)')
    parser.add_argument('--input-file', type=str, default='rejected_translations.json', 
                        help='Arquivo JSON com entradas falhadas')
    parser.add_argument('--max-retries', type=int, default=3, help='Número máximo de tentativas de tradução')
    parser.add_argument('--max-workers', type=int, default=5, help='Número máximo de threads para tradução')
    parser.add_argument('--dry-run', action='store_true', help='Modo teste: não atualiza o DB')
    parser.add_argument('--max-synopsis-length', type=int, default=2700, help='Tamanho máximo da sinopse')
    args = parser.parse_args()

    database_url = args.database_url or os.getenv('DATABASE_URL')
    if not database_url:
        raise ValueError("DATABASE_URL não encontrado. Configure no arquivo .env ou use --database-url")

    return {
        'DATABASE_URL': database_url,
        'INPUT_FILE': args.input_file,
        'MAX_RETRIES': args.max_retries,
        'MAX_WORKERS': args.max_workers,
        'DRY_RUN': args.dry_run,
        'MAX_SYNOPSIS_LENGTH': args.max_synopsis_length
    }

def clean_text(text):
    if not text:
        return text
    text = re.sub(r'[\u200b-\u200f\u202a-\u202e]', '', text)
    return text.encode('utf-8', errors='ignore').decode('utf-8')

def load_failed_entries(config):
    try:
        with open(config['INPUT_FILE'], 'r', encoding='utf-8') as f:
            data = json.load(f)
        entries = []
        for item in data:
            if 'mal_id' not in item or 'original' not in item:
                logger.warning(f"Entrada inválida no JSON: {item}")
                continue
            entries.append((item['mal_id'], clean_text(item['original'])))
        logger.info(f"Carregadas {len(entries)} entradas válidas de {config['INPUT_FILE']}")
        return entries
    except Exception as e:
        logger.error(f"Erro ao carregar {config['INPUT_FILE']}: {e}")
        raise

def init_translator():
    return GoogleTranslator(source='auto', target='pt')

@backoff.on_exception(backoff.expo, TooManyRequests, max_tries=5, max_time=300)
def safe_translate_single(mal_id, original_text, translator, config, translation_cache=None):
    if translation_cache is None:
        translation_cache = {}

    if not original_text or not original_text.strip():
        logger.warning(f"Sinopse ID {mal_id} vazia ou inválida")
        return None
    
    original_clean = clean_text(original_text.strip())
    text_hash = hash(original_clean)
    if text_hash in translation_cache:
        logger.info(f"Usando tradução em cache para ID {mal_id}")
        return translation_cache[text_hash]
    
    if len(original_clean) > config['MAX_SYNOPSIS_LENGTH']:
        original_clean = original_clean[:config['MAX_SYNOPSIS_LENGTH']]
        logger.warning(f"Sinopse ID {mal_id} truncada para {config['MAX_SYNOPSIS_LENGTH']} caracteres.")
    
    try:
        translated = translator.translate(original_clean)
        if translated and translated.strip():
            translated = clean_text(translated.strip())
            translation_cache[text_hash] = translated
            logger.info(f"Tradução bem-sucedida para ID {mal_id}: {translated[:100]}...")
            return translated
        else:
            logger.warning(f"Tradução vazia para ID {mal_id}")
            return None
    except Exception as e:
        logger.warning(f"Erro na tradução para ID {mal_id}: {e}")
        return None

def translate_batch(entries, translator, config):
    translated_batch = []
    failed_translations = []
    translation_cache = {}
    max_workers = min(config['MAX_WORKERS'], len(entries))

    def translate_entry(entry):
        mal_id, original_text = entry
        translated = safe_translate_single(mal_id, original_text, translator, config, translation_cache)
        return (mal_id, translated) if translated else (mal_id, None)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(translate_entry, entry) for entry in entries]
        with tqdm(total=len(entries), desc="Atualizando sinopses") as pbar:
            for future in as_completed(futures):
                mal_id, translated = future.result()
                if translated:
                    translated_batch.append((mal_id, translated))
                else:
                    failed_translations.append({"mal_id": mal_id, "original": [e[1] for e in entries if e[0] == mal_id][0]})
                pbar.update(1)

    if failed_translations:
        with open('failed_translations.json', 'w', encoding='utf-8') as f:
            json.dump(failed_translations, f, ensure_ascii=False, indent=2)
        logger.info(f"Salvadas {len(failed_translations)} traduções falhadas em failed_translations.json")

    return translated_batch

def update_synopsis_pt(cursor, conn, translated_batch, dry_run=False, batch_size=1000):
    if not translated_batch:
        return 0
    
    updated_count = 0
    query = """
    UPDATE animes 
    SET synopsis_pt = %s, 
        updated_at = NOW()
    WHERE mal_id = %s
    """
    
    try:
        if dry_run:
            logger.info(f"[DRY-RUN] Simulando update de {len(translated_batch)} registros")
            return len(translated_batch)
        
        for i in range(0, len(translated_batch), batch_size):
            batch = translated_batch[i:i + batch_size]
            cursor.executemany(query, [(trans, mal_id) for mal_id, trans in batch])
            conn.commit()
            updated_count += cursor.rowcount
            logger.info(f"Atualizadas {cursor.rowcount} sinopses no lote {i//batch_size + 1}")
        
        logger.info(f"Total de sinopses atualizadas: {updated_count}")
        return updated_count
    except Exception as e:
        conn.rollback()
        logger.error(f"Erro ao atualizar sinopses: {e}")
        return 0

def init_database_pool(config):
    try:
        pool = SimpleConnectionPool(minconn=1, maxconn=5, dsn=config['DATABASE_URL'])
        conn = pool.getconn()
        cursor = conn.cursor()
        logger.info("Pool de conexões com o banco de dados estabelecido.")
        return pool, conn, cursor
    except Exception as e:
        logger.error(f"Erro ao conectar ao banco de dados: {e}")
        raise

def close_database_connection(pool, conn, cursor):
    try:
        cursor.close()
        pool.putconn(conn)
        pool.closeall()
        logger.info("Conexões do pool fechadas.")
    except Exception as e:
        logger.error(f"Erro ao fechar conexão: {e}")

def main():
    config = load_config()
    translator = init_translator()
    
    try:
        pool, conn, cursor = init_database_pool(config)
        entries = load_failed_entries(config)
        
        if not entries:
            logger.info("Nenhuma entrada para processar. Encerrando.")
            return
        
        logger.info(f"Iniciando atualização de {len(entries)} sinopses falhadas")
        translated_batch = translate_batch(entries, translator, config)
        updated = update_synopsis_pt(cursor, conn, translated_batch, config['DRY_RUN'])
        
        logger.info(f"\n=== ATUALIZAÇÃO CONCLUÍDA ===")
        logger.info(f"Total de sinopses atualizadas: {updated}")
        logger.info(f"Total processadas: {len(entries)}")
        
    except KeyboardInterrupt:
        logger.info("\nProcesso interrompido pelo usuário.")
    except Exception as e:
        logger.error(f"Erro durante a atualização: {e}")
    finally:
        close_database_connection(pool, conn, cursor)

if __name__ == "__main__":
    main()