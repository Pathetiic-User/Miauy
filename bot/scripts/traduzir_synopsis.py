import time
import psycopg2
import psycopg2.pool
import json
import logging
import os
from datetime import datetime
from deep_translator import GoogleTranslator
from deep_translator.exceptions import TooManyRequests
from tqdm import tqdm
from dotenv import load_dotenv
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import difflib
import psutil
import sys
import re

# Forçar codificação UTF-8 no console do Windows
if sys.platform == "win32":
    import codecs
    sys.stdout = codecs.getwriter("utf-8")(sys.stdout.detach())

# Configuração de logging com codificação UTF-8
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('synopsis_translator_efficient.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def check_dependencies():
    """Verifica se todas as dependências necessárias estão instaladas."""
    required = ['psycopg2', 'deep_translator', 'tqdm', 'dotenv', 'argparse', 'psutil']
    for module in required:
        try:
            __import__(module)
        except ImportError:
            logger.error(f"Módulo '{module}' não encontrado. Instale com 'pip install {module}'")
            raise

def load_config():
    """Carrega configurações do arquivo .env e argumentos da linha de comando"""
    parser = argparse.ArgumentParser(description='Tradutor de Sinopses para Português - Versão Eficiente')
    parser.add_argument('--database-url', type=str, help='URL do banco de dados (sobrescreve .env)')
    parser.add_argument('--batch-size', type=int, default=100, help='Tamanho do lote para tradução e busca no DB')
    parser.add_argument('--max-workers', type=int, default=5, help='Número de workers para tradução paralela')
    parser.add_argument('--max-retries', type=int, default=3, help='Número máximo de tentativas de tradução')
    parser.add_argument('--delay', type=float, default=2.0, help='Delay base entre tentativas de tradução (segundos)')
    parser.add_argument('--limit', type=int, help='Limitar o número total de sinopses a processar')
    parser.add_argument('--start-id', type=int, default=1, help='ID inicial para tradução')
    parser.add_argument('--dry-run', action='store_true', help='Modo teste: não atualiza o DB')
    parser.add_argument('--reset-progress', action='store_true', help='Reseta o arquivo de progresso')
    parser.add_argument('--max-synopsis-length', type=int, default=2000, help='Tamanho máximo da sinopse antes de truncar')
    parser.add_argument('--split-long-synopses', action='store_true', help='Dividir sinopses longas em partes para tradução')
    parser.add_argument('--min-similarity-ratio', type=float, default=0.0, help='Limite mínimo de similaridade para validação')
    parser.add_argument('--min-length-ratio', type=float, default=0.3, help='Razão mínima de comprimento para validação')
    parser.add_argument('--max-length-ratio', type=float, default=3.0, help='Razão máxima de comprimento para validação')
    parser.add_argument('--failed-file', type=str, default='failed_translations.json', 
                        help='Arquivo para salvar IDs de traduções falhadas')
    parser.add_argument('--min-connections', type=int, default=1, help='Número mínimo de conexões no pool')
    parser.add_argument('--max-connections', type=int, default=5, help='Número máximo de conexões no pool')
    parser.add_argument('--skip-empty', action='store_true', help='Pular sinopses nulas ou vazias')
    args = parser.parse_args()

    load_dotenv()
    logger.info(f"Diretório atual: {os.getcwd()}")
    logger.info(f"DATABASE_URL: {os.getenv('DATABASE_URL')[:20]}..." if os.getenv('DATABASE_URL') else "DATABASE_URL não encontrada")

    database_url = args.database_url or os.getenv('DATABASE_URL')
    if not database_url:
        raise ValueError("DATABASE_URL não encontrado. Configure no arquivo .env ou use --database-url")

    # Validações de configurações
    if args.batch_size <= 0:
        raise ValueError("BATCH_SIZE deve ser maior que 0")
    if args.max_workers < 1 or args.max_workers > 10:
        raise ValueError("MAX_WORKERS deve estar entre 1 e 10")
    if args.max_retries < 0:
        raise ValueError("MAX_RETRIES não pode ser negativo")
    if args.min_connections < 1 or args.max_connections < args.min_connections:
        raise ValueError("MIN_CONNECTIONS deve ser >= 1 e MAX_CONNECTIONS >= MIN_CONNECTIONS")

    config = {
        'DATABASE_URL': database_url,
        'LANGUAGE': 'pt',
        'BATCH_SIZE': min(args.batch_size, 500),
        'MAX_WORKERS': args.max_workers,
        'MAX_RETRIES': args.max_retries,
        'DELAY': args.delay,
        'LIMIT': args.limit,
        'START_ID': args.start_id,
        'DRY_RUN': args.dry_run,
        'RESET_PROGRESS': args.reset_progress,
        'PROGRESS_FILE': 'synopsis_progress_efficient.json',
        'MAX_SYNOPSIS_LENGTH': args.max_synopsis_length,
        'SPLIT_LONG_SYNOPSES': args.split_long_synopses,
        'MIN_SIMILARITY_RATIO': args.min_similarity_ratio,
        'MIN_LENGTH_RATIO': args.min_length_ratio,
        'MAX_LENGTH_RATIO': args.max_length_ratio,
        'FAILED_FILE': args.failed_file,
        'MIN_CONNECTIONS': args.min_connections,
        'MAX_CONNECTIONS': args.max_connections,
        'SKIP_EMPTY': args.skip_empty
    }
    return config

def init_translator(config):
    """Inicializa os tradutores (GoogleTranslator e ArgosTranslate, se disponível)"""
    translators = [GoogleTranslator(source='auto', target='pt')]
    try:
        from argostranslate import package, translate
        package.install_from_path('en_pt.translate')  # Baixe o pacote de tradução en->pt
        translators.append(translate.Translator('en', 'pt'))
        logger.info("ArgosTranslate adicionado como tradutor secundário")
    except ImportError:
        logger.warning("ArgosTranslate não instalado. Usando apenas GoogleTranslator")
    return translators

def init_database_pool(config):
    """Inicializa um pool de conexões com o banco de dados"""
    try:
        pool = psycopg2.pool.ThreadedConnectionPool(
            config['MIN_CONNECTIONS'], config['MAX_CONNECTIONS'], config['DATABASE_URL']
        )
        logger.info(f"Pool de conexões criado: min={config['MIN_CONNECTIONS']}, max={config['MAX_CONNECTIONS']}.")
        return pool
    except Exception as e:
        logger.error(f"Erro ao criar pool de conexões: {e}")
        raise

def get_db_connection(pool):
    """Obtém uma conexão do pool"""
    try:
        conn = pool.getconn()
        cursor = conn.cursor()
        return conn, cursor
    except Exception as e:
        logger.error(f"Erro ao obter conexão do pool: {e}")
        raise

def release_db_connection(pool, conn, cursor):
    """Libera a conexão de volta ao pool"""
    try:
        cursor.close()
        pool.putconn(conn)
    except Exception as e:
        logger.error(f"Erro ao liberar conexão: {e}")

def fetch_batch(cursor, batch_size, last_id, skip_empty=False):
    """Busca um lote de sinopses com synopsis_pt IS NULL usando keyset pagination"""
    query = """
    SELECT mal_id, synopsis 
    FROM animes 
    WHERE mal_id > %s 
    AND synopsis_pt IS NULL
    """
    if skip_empty:
        query += " AND synopsis IS NOT NULL AND synopsis != ''"
    query += " ORDER BY mal_id LIMIT %s"
    
    try:
        cursor.execute(query, [last_id, batch_size])
        results = cursor.fetchall()
        logger.debug(f"Buscado lote: {len(results)} sinopses (a partir de ID > {last_id})")
        return results
    except Exception as e:
        logger.error(f"Erro ao buscar lote de sinopses: {e}")
        return []

def load_progress(config, cursor):
    """Carrega progresso anterior de tradução, recalculando contadores se necessário"""
    progress_file = config['PROGRESS_FILE']
    
    if config['RESET_PROGRESS']:
        if os.path.exists(progress_file):
            os.remove(progress_file)
            logger.info("Progresso resetado.")
        
        try:
            cursor.execute(
                "SELECT COUNT(*) FROM animes WHERE synopsis_pt IS NOT NULL AND mal_id < %s",
                [config['START_ID']]
            )
            total_translated = cursor.fetchone()[0]
            
            query = "SELECT COUNT(*) FROM animes WHERE synopsis_pt IS NULL AND mal_id < %s"
            if config['SKIP_EMPTY']:
                query += " AND synopsis IS NOT NULL AND synopsis != ''"
            cursor.execute(query, [config['START_ID']])
            total_processed = cursor.fetchone()[0]
            
            logger.info(f"Progresso inicial calculado: total_translated={total_translated}, total_processed={total_processed}")
            return {
                'last_id': config['START_ID'] - 1,
                'total_translated': total_translated,
                'total_processed': total_processed
            }
        except Exception as e:
            logger.error(f"Erro ao recalcular progresso: {e}")
            return {'last_id': config['START_ID'] - 1, 'total_translated': 0, 'total_processed': 0}
    
    if os.path.exists(progress_file):
        try:
            with open(progress_file, 'r', encoding='utf-8') as f:
                progress = json.load(f)
                last_id = max(progress.get('last_id', config['START_ID'] - 1), config['START_ID'] - 1)
                total_translated = progress.get('total_translated', 0)
                total_processed = progress.get('total_processed', 0)
                logger.info(f"Progresso carregado: último ID {last_id}, "
                           f"total traduzidos {total_translated}, total processados {total_processed}")
                return {
                    'last_id': last_id,
                    'total_translated': total_translated,
                    'total_processed': total_processed
                }
        except Exception as e:
            logger.error(f"Erro ao carregar progresso: {e}")
    
    try:
        cursor.execute(
            "SELECT COUNT(*) FROM animes WHERE synopsis_pt IS NOT NULL AND mal_id < %s",
            [config['START_ID']]
        )
        total_translated = cursor.fetchone()[0]
        
        query = "SELECT COUNT(*) FROM animes WHERE synopsis_pt IS NULL AND mal_id < %s"
        if config['SKIP_EMPTY']:
            query += " AND synopsis IS NOT NULL AND synopsis != ''"
        cursor.execute(query, [config['START_ID']])
        total_processed = cursor.fetchone()[0]
        
        logger.info(f"Progresso inicial calculado: total_translated={total_translated}, total_processed={total_processed}")
        return {
            'last_id': config['START_ID'] - 1,
            'total_translated': total_translated,
            'total_processed': total_processed
        }
    except Exception as e:
        logger.error(f"Erro ao inicializar progresso: {e}")
        return {'last_id': config['START_ID'] - 1, 'total_translated': 0, 'total_processed': 0}

def save_progress(last_id, total_translated, total_processed, config):
    """Salva progresso de tradução"""
    progress = {
        'last_id': last_id,
        'total_translated': total_translated,
        'total_processed': total_processed,
        'timestamp': datetime.now().isoformat()
    }
    try:
        with open(config['PROGRESS_FILE'], 'w', encoding='utf-8') as f:
            json.dump(progress, f, ensure_ascii=False, indent=2)
        logger.info(f"Progresso salvo: último ID {last_id}, total traduzidos {total_translated}")
    except Exception as e:
        logger.error(f"Erro ao salvar progresso: {e}")

def save_failed_ids(failed_ids, config):
    """Salva IDs de traduções que falharam para reprocessamento futuro."""
    try:
        failed_file = config.get('FAILED_FILE', 'failed_translations.json')
        existing = []
        if os.path.exists(failed_file):
            with open(failed_file, 'r', encoding='utf-8') as f:
                existing = json.load(f)
        existing.extend(failed_ids)
        with open(failed_file, 'w', encoding='utf-8') as f:
            json.dump(existing, f, ensure_ascii=False, indent=2)
        logger.info(f"Salvos {len(failed_ids)} IDs em {failed_file} para reprocessamento")
    except Exception as e:
        logger.error(f"Erro ao salvar IDs falhados: {e}")

def save_failed_translations(failed_translations, config):
    """Salva traduções rejeitadas para análise posterior."""
    try:
        failed_file = 'rejected_translations.json'
        existing = []
        if os.path.exists(failed_file):
            with open(failed_file, 'r', encoding='utf-8') as f:
                existing = json.load(f)
        existing.extend(failed_translations)
        with open(failed_file, 'w', encoding='utf-8') as f:
            json.dump(existing, f, ensure_ascii=False, indent=2)
        logger.info(f"Salvas {len(failed_translations)} traduções rejeitadas em {failed_file}")
    except Exception as e:
        logger.error(f"Erro ao salvar traduções rejeitadas: {e}")

def load_failed_entries(config, cursor):
    """Carrega entradas de traduções falhadas do arquivo rejected_translations.json"""
    try:
        failed_file = 'rejected_translations.json'
        if not os.path.exists(failed_file):
            logger.info(f"Arquivo {failed_file} não encontrado. Pulando reprocessamento de traduções falhadas.")
            return []
        
        with open(failed_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        entries = []
        mal_ids = [item['mal_id'] for item in data if 'mal_id' in item and 'original' in item]
        
        query = "SELECT mal_id FROM animes WHERE mal_id = ANY(%s) AND synopsis_pt IS NULL"
        if config['SKIP_EMPTY']:
            query += " AND synopsis IS NOT NULL AND synopsis != ''"
        cursor.execute(query, (mal_ids,))
        valid_ids = {row[0] for row in cursor.fetchall()}
        
        for item in data:
            if 'mal_id' not in item or 'original' not in item:
                logger.warning(f"Entrada inválida no JSON: {item}")
                continue
            mal_id = item['mal_id']
            if mal_id not in valid_ids:
                logger.info(f"Sinopse ID {mal_id} já traduzida no banco, pulando.")
                continue
            entries.append((mal_id, clean_text(item['original'])))
        
        logger.info(f"Carregadas {len(entries)} entradas válidas de {failed_file} para reprocessamento")
        return entries
    except Exception as e:
        logger.error(f"Erro ao carregar {failed_file}: {e}")
        return []

def validate_translation(original, translated, config):
    """Valida a qualidade da tradução com base em comprimento e similaridade"""
    if not translated or not translated.strip() or len(translated.strip()) <= 5:
        logger.debug(f"Validação falhou: Tradução vazia ou muito curta (len={len(translated.strip()) if translated else 0})")
        return False
    
    if not original or not original.strip():
        return True
    
    length_ratio = len(translated) / len(original) if original else 1.0
    if not (config['MIN_LENGTH_RATIO'] <= length_ratio <= config['MAX_LENGTH_RATIO']):
        logger.debug(f"Validação falhou: Razão de comprimento {length_ratio:.2f} fora do intervalo [{config['MIN_LENGTH_RATIO']}, {config['MAX_LENGTH_RATIO']}]")
        return False
    
    seq_matcher = difflib.SequenceMatcher(None, original.lower(), translated.lower())
    similarity_ratio = seq_matcher.ratio()
    if similarity_ratio < config['MIN_SIMILARITY_RATIO']:
        logger.debug(f"Validação falhou: Similaridade {similarity_ratio:.2f} < {config['MIN_SIMILARITY_RATIO']}. "
                    f"Original: {original[:100]}... Traduzido: {translated[:100] if translated else 'N/A'}...")
        return False
    
    logger.debug(f"Validação bem-sucedida: Razão={length_ratio:.2f}, Similaridade={similarity_ratio:.2f}")
    return True

def clean_text(text):
    """Remove caracteres problemáticos do texto."""
    if not text:
        return text
    text = re.sub(r'[\u200b-\u200f\u202a-\u202e]', '', text)
    text = text.encode('utf-8', errors='ignore').decode('utf-8')
    return text

def safe_translate_single(mal_id, original_text, translators, max_retries=3, delay=2, config=None):
    """Traduz um texto individual com retry"""
    if original_text is None or not original_text.strip():
        if config['SKIP_EMPTY']:
            logger.warning(f"Sinopse ID {mal_id} nula ou vazia, pulando (skip-empty ativado)")
            return None
        logger.warning(f"Sinopse ID {mal_id} nula ou vazia, definindo synopsis_pt como vazio")
        return ""
    
    original_clean = clean_text(original_text.strip())
    if len(original_clean) > config['MAX_SYNOPSIS_LENGTH']:
        if config['SPLIT_LONG_SYNOPSES']:
            logger.info(f"Sinopse ID {mal_id} longa ({len(original_clean)} chars). Dividindo...")
            parts = [original_clean[i:i + config['MAX_SYNOPSIS_LENGTH']] 
                     for i in range(0, len(original_clean), config['MAX_SYNOPSIS_LENGTH'])]
            translated_parts = []
            for i, part in enumerate(parts):
                translated = None
                for translator in translators:
                    for attempt in range(max_retries):
                        try:
                            translated = translator.translate(part)
                            if translated and validate_translation(part, translated, config):
                                translated_parts.append(clean_text(translated.strip()))
                                break
                        except TooManyRequests as e:
                            retry_after = getattr(e, 'retry_after', 2 ** attempt * delay)
                            logger.warning(f"Rate limit atingido para ID {mal_id}, parte {i+1}. Aguardando {retry_after}s")
                            time.sleep(retry_after)
                        except Exception as e:
                            logger.warning(f"Erro na tradução para ID {mal_id}, parte {i+1}: {e}")
                    if translated:
                        break
                    else:
                        logger.error(f"Falha na tradução da parte {i+1} do ID {mal_id}")
                        return None
            translated = " ".join(translated_parts)
        else:
            logger.warning(f"Sinopse ID {mal_id} truncada (tamanho: {len(original_clean)}). Considere revisão manual.")
            original_clean = original_clean[:config['MAX_SYNOPSIS_LENGTH']]
    else:
        for translator in translators:
            for attempt in range(max_retries):
                try:
                    original_clean = original_clean.replace('\u200b', '')
                    translated = translator.translate(original_clean)
                    if translated:
                        translated = translated.replace('\u200b', '')
                        if validate_translation(original_clean, translated, config):
                            logger.debug(f"Tradução bem-sucedida para ID {mal_id} com {translator.__class__.__name__}")
                            return translated.strip()
                        else:
                            logger.warning(f"Tradução inválida para ID {mal_id} com {translator.__class__.__name__}. "
                                          f"Original: {original_clean[:100]}... Traduzido: {translated[:100] if translated else 'N/A'}...")
                            save_failed_translations([{
                                'mal_id': mal_id,
                                'original': original_clean,
                                'translated': translated,
                                'reason': f'Falha na validação (similaridade={difflib.SequenceMatcher(None, original_clean.lower(), translated.lower()).ratio():.2f}, '
                                          f'razão de comprimento={len(translated)/len(original_clean) if original_clean else 0:.2f})'
                            }], config)
                            return None
                    else:
                        logger.warning(f"Tradução vazia para ID {mal_id} com {translator.__class__.__name__}")
                        save_failed_translations([{
                            'mal_id': mal_id,
                            'original': original_clean,
                            'translated': None,
                            'reason': 'Tradução vazia'
                        }], config)
                        return None
                except TooManyRequests as e:
                    retry_after = getattr(e, 'retry_after', 2 ** attempt * delay)
                    logger.warning(f"Rate limit atingido para ID {mal_id}. Aguardando {retry_after}s")
                    time.sleep(retry_after)
                except Exception as e:
                    logger.warning(f"Erro na tradução para ID {mal_id}: {e}")
                    if attempt < max_retries - 1:
                        time.sleep(2 ** attempt * delay)
    
    logger.error(f"Falha total na tradução para ID {mal_id}. Pulando update.")
    save_failed_translations([{
        'mal_id': mal_id,
        'original': original_clean,
        'translated': None,
        'reason': 'Falha total após retries'
    }], config)
    return None

def translate_batch_parallel(synopses_batch, translators, max_workers, config):
    """Traduz um lote em paralelo com controle dinâmico de taxa"""
    translated_batch = []
    failed_ids = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_item = {
            executor.submit(safe_translate_single, mal_id, synopsis, translators, 
                           config['MAX_RETRIES'], config['DELAY'], config): (mal_id, synopsis)
            for mal_id, synopsis in synopses_batch
        }
        
        with tqdm(total=len(synopses_batch), desc="Traduzindo lote") as pbar:
            for future in as_completed(future_to_item):
                mal_id, _ = future_to_item[future]
                try:
                    translated = future.result()
                    if translated is not None:
                        translated = clean_text(translated)[:2000].encode('utf-8')[:2000].decode('utf-8', errors='ignore')
                        translated_batch.append((mal_id, translated))
                    else:
                        failed_ids.append(mal_id)
                        logger.debug(f"Pulando update para ID {mal_id}: falha na tradução")
                except Exception as e:
                    failed_ids.append(mal_id)
                    logger.error(f"Erro no worker para ID {mal_id}: {e}")
                pbar.update(1)
    
    if failed_ids:
        save_failed_ids(failed_ids, config)
    return translated_batch

def update_synopsis_pt(cursor, conn, translated_batch, dry_run=False):
    """Atualiza o campo synopsis_pt no banco de dados"""
    if not translated_batch:
        return 0
    
    updated_count = 0
    query = """
    UPDATE animes 
    SET synopsis_pt = %s, 
        updated_at = NOW()
    WHERE mal_id = %s AND synopsis_pt IS NULL
    """
    
    try:
        if dry_run:
            logger.info(f"[DRY-RUN] Simulando update de {len(translated_batch)} registros")
            updated_count = len(translated_batch)
        else:
            cursor.executemany(query, [(trans, mal_id) for mal_id, trans in translated_batch])
            conn.commit()
            updated_count = cursor.rowcount
        logger.info(f"Atualizadas {updated_count} sinopses no banco de dados")
        return updated_count
    except Exception as e:
        conn.rollback()
        logger.error(f"Erro ao atualizar sinopses: {e}")
        return 0

def get_total_animes(cursor, skip_empty=False):
    """Conta o total exato de animes com synopsis_pt IS NULL"""
    try:
        query = "SELECT COUNT(*) FROM animes WHERE synopsis_pt IS NULL"
        if skip_empty:
            query += " AND synopsis IS NOT NULL AND synopsis != ''"
        cursor.execute(query)
        total = cursor.fetchone()[0]
        logger.info(f"Total de animes sem tradução: {total}")
        return total
    except Exception as e:
        logger.error(f"Erro ao contar total de animes: {e}")
        return 28941

def adjust_batch_size(total_animes, base_batch_size):
    """Ajusta o tamanho do lote com base no total de animes"""
    if total_animes > 100000:
        return min(base_batch_size * 2, 1000)
    elif total_animes < 1000:
        return max(base_batch_size // 2, 10)
    return base_batch_size

def check_resource_usage():
    """Verifica o uso de memória e CPU, emitindo alertas se necessário."""
    mem = psutil.virtual_memory()
    cpu = psutil.cpu_percent(interval=1)
    if mem.percent > 90:
        logger.warning(f"Uso de memória alto: {mem.percent}%")
    if cpu > 90:
        logger.warning(f"Uso de CPU alto: {cpu}%")

def check_db_permissions(cursor):
    """Verifica permissões mínimas no banco de dados"""
    try:
        cursor.execute("SELECT has_table_privilege('animes', 'SELECT') AND has_table_privilege('animes', 'UPDATE')")
        has_privileges = cursor.fetchone()[0]
        if not has_privileges:
            raise ValueError("Usuário do banco não tem permissões de SELECT e UPDATE na tabela 'animes'")
        logger.info("Permissões do banco verificadas com sucesso")
    except Exception as e:
        logger.error(f"Erro ao verificar permissões do banco: {e}")
        raise

def main():
    """Função principal do script de tradução eficiente"""
    check_dependencies()
    config = load_config()
    translators = init_translator(config)
    pool = init_database_pool(config)
    
    try:
        conn, cursor = get_db_connection(pool)
        check_db_permissions(cursor)
        total_animes = get_total_animes(cursor, config['SKIP_EMPTY'])
        config['BATCH_SIZE'] = adjust_batch_size(total_animes, config['BATCH_SIZE'])
        
        progress = load_progress(config, cursor)
        last_id = progress.get('last_id', config['START_ID'] - 1)
        total_translated = progress.get('total_translated', 0)
        total_processed = progress.get('total_processed', 0)
        batch_num = 0
        total_limit = config.get('LIMIT')
        
        logger.info(f"Iniciando tradução com last_id={last_id + 1}, batch_size={config['BATCH_SIZE']}, "
                   f"workers={config['MAX_WORKERS']}, dry_run={config['DRY_RUN']}, skip_empty={config['SKIP_EMPTY']}")
        logger.info(f"Total estimado: {total_animes} animes sem tradução")
        
        with tqdm(total=total_animes, desc="Progresso geral (banco)", initial=total_processed) as pbar_global:
            while True:
                batch_num += 1
                check_resource_usage()
                synopses_batch = fetch_batch(
                    cursor, config['BATCH_SIZE'], last_id, config['SKIP_EMPTY']
                )
                
                if not synopses_batch:
                    logger.info("Nenhum lote mais para processar no banco. Concluído!")
                    break
                
                if total_limit and total_processed >= total_limit:
                    logger.info(f"Limite de {total_limit} processados atingido.")
                    break
                
                first_id = synopses_batch[0][0]
                logger.info(f"Lote {batch_num}: Processando {len(synopses_batch)} sinopses (IDs {first_id} a {synopses_batch[-1][0]})")
                
                start_time = time.time()
                translated_batch = translate_batch_parallel(synopses_batch, translators, config['MAX_WORKERS'], config)
                batch_time = time.time() - start_time
                logger.info(f"Tradução do lote concluída em {batch_time:.2f}s (sucessos: {len(translated_batch)}/{len(synopses_batch)})")
                
                updated = update_synopsis_pt(cursor, conn, translated_batch, config['DRY_RUN'])
                
                total_translated += updated
                total_processed += len(synopses_batch)
                
                new_last_id = synopses_batch[-1][0] if synopses_batch else last_id
                save_progress(new_last_id, total_translated, total_processed, config)
                
                last_id = new_last_id
                pbar_global.update(len(synopses_batch))
                
                if config['DELAY'] > 0:
                    logger.info(f"Aguardando {config['DELAY']}s antes do próximo lote...")
                    time.sleep(config['DELAY'])
        
        logger.info("Iniciando reprocessamento de traduções falhadas de rejected_translations.json")
        failed_entries = load_failed_entries(config, cursor)
        if failed_entries:
            logger.info(f"Processando {len(failed_entries)} traduções falhadas")
            translated_batch = translate_batch_parallel(failed_entries, translators, config['MAX_WORKERS'], config)
            updated = update_synopsis_pt(cursor, conn, translated_batch, config['DRY_RUN'])
            total_translated += updated
            total_processed += len(failed_entries)
            save_progress(last_id, total_translated, total_processed, config)
            logger.info(f"Reprocessamento concluído: {updated} sinopses atualizadas, {len(failed_entries)} processadas")
        else:
            logger.info("Nenhuma tradução falhada para reprocessar.")
        
        logger.info(f"\n=== TRADUÇÃO CONCLUÍDA ===")
        logger.info(f"Total de sinopses atualizadas: {total_translated}")
        logger.info(f"Total processadas: {total_processed}")
        logger.info(f"Tempo médio por lote: ~{batch_time:.2f}s (último)")
        
    except KeyboardInterrupt:
        logger.info("\nProcesso interrompido pelo usuário.")
        save_progress(last_id, total_translated, total_processed, config)
    except Exception as e:
        logger.error(f"Erro durante a tradução: {e}")
        save_progress(last_id, total_translated, total_processed, config)
    finally:
        release_db_connection(pool, conn, cursor)
        pool.closeall()
        logger.info("Pool de conexões fechado.")

if __name__ == "__main__":
    main()