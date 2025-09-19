import psycopg2
import requests
import json
from datetime import datetime, timezone
import time
import os
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor
import threading
import logging
from logging.handlers import RotatingFileHandler

# Carrega as variáveis de ambiente do arquivo .env
load_dotenv()

# Configuração do logging
def setup_logging():
    # Cria diretório de logs se não existir
    log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')
    os.makedirs(log_dir, exist_ok=True)
    
    # Configuração básica do logging
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    log_level = os.getenv('LOG_LEVEL', 'INFO').upper()
    
    # Configura o root logger
    logger = logging.getLogger()
    logger.setLevel(log_level)
    
    # Remove handlers existentes
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    
    # Handler para arquivo com rotação
    log_file = os.path.join(log_dir, 'anime_updater.log')
    file_handler = RotatingFileHandler(
        log_file, 
        maxBytes=10*1024*1024,  # 10MB por arquivo
        backupCount=5,           # Mantém até 5 arquivos de log
        encoding='utf-8'
    )
    file_handler.setFormatter(logging.Formatter(log_format))
    
    # Handler para console
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter(log_format))
    
    # Adiciona os handlers
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger

# Inicializa o logger
logger = setup_logging()

# Dicionários de tradução
SEASON_TRANSLATIONS = {
    'winter': 'Inverno',
    'spring': 'Primavera',
    'summer': 'Verão',
    'fall': 'Outono'
}

STATUS_TRANSLATIONS = {
    'Currently Airing': 'Em Exibição',
    'Finished Airing': 'Concluído',
    'Not yet aired': 'Ainda Não Exibido',
    'Not yet released': 'Ainda Não Lançado',
    'Complete': 'Completo',
    'Hiatus': 'Em Hiato',
    'Discontinued': 'Descontinuado',
    'TBA': 'A Definir',
    'UPCOMING': 'Em Breve'
}

# Rate limiting configuration for Jikan API
REQUESTS_PER_SECOND = 3
REQUESTS_PER_MINUTE = 60
DELAY_BETWEEN_REQUESTS = 1.0 / REQUESTS_PER_SECOND  # ~0.33 seconds

# Semaphore to limit concurrent requests
semaphore = threading.Semaphore(REQUESTS_PER_SECOND)

# Track request timestamps for rate limiting
request_timestamps = []
request_lock = threading.Lock()

def wait_for_rate_limit():
    """Wait if we've hit the rate limit"""
    global request_timestamps
    
    now = time.time()
    
    with request_lock:
        # Remove timestamps older than 1 minute
        request_timestamps = [t for t in request_timestamps if now - t < 60]
        
        # If we've hit the minute limit, wait until the oldest request is more than 1 minute old
        if len(request_timestamps) >= REQUESTS_PER_MINUTE:
            oldest = request_timestamps[0]
            sleep_time = 60 - (now - oldest)
            if sleep_time > 0:
                logger.info(f"Rate limit reached. Waiting {sleep_time:.2f} seconds...")
                time.sleep(sleep_time)
        
        # Add current timestamp
        request_timestamps.append(time.time())
    
    # Respect requests per second limit
    time.sleep(DELAY_BETWEEN_REQUESTS)

def update_anime(mal_id, conn, error_ids):
    try:
        with semaphore:
            # Wait to respect rate limits
            wait_for_rate_limit()
            
            # Make the request with retry logic
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    response = requests.get(f"https://api.jikan.moe/v4/anime/{mal_id}/full", timeout=10)
                    response.raise_for_status()
                except requests.exceptions.RequestException as e:
                    if attempt == max_retries - 1:  # Last attempt
                        raise
                    wait_time = (attempt + 1) * 5  # Exponential backoff
                    logger.warning(f"Request failed for mal_id {mal_id}, attempt {attempt + 1}/{max_retries}. Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
            data = response.json()['data']

            # Parse fields from API response
            title = data.get('title')
            title_english = data.get('title_english')
            title_japanese = data.get('title_japanese')
            title_synonyms = data.get('title_synonyms')  # Already a list
            anime_type = data.get('type')
            source = data.get('source')
            episodes = data.get('episodes')
            
            # Status em inglês e português
            status = data.get('status')
            status_pt = STATUS_TRANSLATIONS.get(status, status)
            
            airing = data.get('airing')
            aired_from_str = data['aired'].get('from')
            aired_from = datetime.fromisoformat(aired_from_str.rstrip('Z')).date() if aired_from_str else None
            aired_to_str = data['aired'].get('to')
            aired_to = datetime.fromisoformat(aired_to_str.rstrip('Z')).date() if aired_to_str else None
            duration = data.get('duration')
            rating = data.get('rating')
            score = data.get('score')
            scored_by = data.get('scored_by')
            rank = data.get('rank')
            popularity = data.get('popularity')
            members = data.get('members')
            favorites = data.get('favorites')
            synopsis = data.get('synopsis')
            background = data.get('background')
            
            # Temporada em inglês e português
            season = data.get('season')
            season_pt = SEASON_TRANSLATIONS.get(season.lower(), season) if season else None
            year = data.get('year')
            
            # Se year for NULL, tenta obter do campo aired_from
            if year is None and aired_from:
                year = aired_from.year
            
            # Monta o campo premiered (ex: "Spring 2023")
            premiered = None
            if season and year and str(year).isdigit():
                try:
                    # Garante que a primeira letra da estação seja maiúscula e o resto minúsculo
                    season_formatted = season.capitalize()
                    year_int = int(year)
                    # Formata o ano para garantir que tenha 4 dígitos
                    year_formatted = f"{year_int:04d}"
                    premiered = f"{season_formatted} {year_formatted}"
                except (ValueError, AttributeError) as e:
                    logger.warning(f"Erro ao formatar premiered para mal_id {mal_id}: {e}")
                    premiered = None
                
            broadcast = data['broadcast'].get('string') if data.get('broadcast') else None
            url = data.get('url')
            images = json.dumps(data.get('images')) if data.get('images') else None
            trailer = json.dumps(data.get('trailer')) if data.get('trailer') else None
            producers = json.dumps(data.get('producers')) if data.get('producers') else None
            licensors = json.dumps(data.get('licensors')) if data.get('licensors') else None
            studios = json.dumps(data.get('studios')) if data.get('studios') else None
            genres = json.dumps(data.get('genres')) if data.get('genres') else None
            explicit_genres = json.dumps(data.get('explicit_genres', []))
            themes = json.dumps(data.get('themes')) if data.get('themes') else None
            demographics = json.dumps(data.get('demographics')) if data.get('demographics') else None
            relations = json.dumps(data.get('relations')) if data.get('relations') else None
            approved = data.get('approved')
            image_url = data['images']['jpg'].get('image_url') if data.get('images', {}).get('jpg') else None
            external_links = json.dumps(data.get('external', []))

            # Execute UPDATE query
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE public.animes SET
                        title = %s,
                        title_english = %s,
                        title_japanese = %s,
                        title_synonyms = %s,
                        type = %s,
                        source = %s,
                        episodes = %s,
                        status = %s,
                        status_pt = %s,
                        airing = %s,
                        aired_from = %s,
                        aired_to = %s,
                        duration = %s,
                        rating = %s,
                        score = %s,
                        scored_by = %s,
                        rank = %s,
                        popularity = %s,
                        members = %s,
                        favorites = %s,
                        synopsis = %s,
                        background = %s,
                        premiered = %s,
                        season_pt = %s,
                        broadcast = %s,
                        url = %s,
                        images = %s::jsonb,
                        trailer = %s::jsonb,
                        producers = %s::jsonb,
                        licensors = %s::jsonb,
                        studios = %s::jsonb,
                        genres = %s::jsonb,
                        explicit_genres = %s::jsonb,
                        themes = %s::jsonb,
                        demographics = %s::jsonb,
                        relations = %s::jsonb,
                        approved = %s,
                        season = %s,
                        year = %s,
                        image_url = %s,
                        updated_at = NOW(),
                        external_links = %s::jsonb
                    WHERE mal_id = %s
                """, (
                    title, title_english, title_japanese, title_synonyms, anime_type, source, episodes, status,
                    status_pt, airing, aired_from, aired_to, duration, rating, score, scored_by, rank, popularity,
                    members, favorites, synopsis, background, premiered, season_pt, broadcast, url,
                    images, trailer, producers, licensors, studios, genres, explicit_genres, themes, demographics,
                    relations, approved, season, year, image_url, external_links, mal_id
                ))
            conn.commit()
            logger.info(f"Successfully updated anime with mal_id: {mal_id}")
            return True  # Indica sucesso APÓS atualização no banco
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 429:  # Too Many Requests
            retry_after = int(e.response.headers.get('Retry-After', 60))
            logger.warning(f"Rate limited. Waiting {retry_after} seconds...")
            time.sleep(retry_after)
            return update_anime(mal_id, conn, error_ids)  # Retry the request
        logger.error(f"HTTP error for mal_id {mal_id}: {e}", exc_info=True)
    except requests.exceptions.RequestException as e:
        logger.error(f"API request error for mal_id {mal_id}: {e}", exc_info=True)
    except psycopg2.Error as e:
        logger.error(f"Database error for mal_id {mal_id}: {e}", exc_info=True)
        conn.rollback()
    except Exception as e:
        logger.critical(f"Unexpected error for mal_id {mal_id}", exc_info=True)
        error_ids.append(mal_id)
    finally:
        # Small additional delay to be extra safe with rate limits
        time.sleep(0.1)

def send_startup_notification():
    """Envia notificação de inicialização para o webhook do Discord"""
    webhook_id = os.getenv('DISCORD_WEBHOOK_ID')
    webhook_token = os.getenv('DISCORD_WEBHOOK_TOKEN')
    
    if not webhook_id or not webhook_token:
        logger.warning("Webhook do Discord não configurado corretamente")
        return
    
    webhook_url = f"https://discord.com/api/webhooks/{webhook_id}/{webhook_token}"
    
    payload = {
        "embeds": [{
            "color": 512,
            "title": "Script Worker Railway - Iniciado com Sucesso!!",
            "footer": {
                "text": "Emitodo Em"
            },
            "timestamp": datetime.now(timezone.utc).isoformat()
        }]
    }
    
    try:
        response = requests.post(
            webhook_url,
            json=payload,
            headers={"Content-Type": "application/json"}
        )
        response.raise_for_status()
    except Exception as e:
        logger.error("Erro ao enviar notificação de inicialização para o Discord", exc_info=True)

def send_discord_notification(updated_count, error_ids):
    """Envia notificação de atualização para o webhook do Discord"""
    webhook_id = os.getenv('DISCORD_WEBHOOK_ID')
    webhook_token = os.getenv('DISCORD_WEBHOOK_TOKEN')
    
    if not webhook_id or not webhook_token:
        logger.warning("Webhook do Discord não configurado corretamente")
        return
    
    webhook_url = f"https://discord.com/api/webhooks/{webhook_id}/{webhook_token}"
    
    # Formata a lista de IDs com erro
    error_ids_str = ", ".join(map(str, error_ids)) if error_ids else "Nenhum"
    
    # Determina a cor do embed (vermelho se houver erros, verde se não)
    color = 15158332 if error_ids else 2192415
    
    payload = {
        "embeds": [{
            "color": color,
            "title": "Script Worker Railway - Realizado com Sucesso!!",
            "description": (
                f"Anime(s) atualizados: **{updated_count}**\n\n"
                f"Anime(s) com Erro: **{len(error_ids)}**\n\n"
                f"IDs com erro: {error_ids_str if error_ids else 'Nenhum'}"
            ),
            "footer": {
                "text": "Relatório Completo Em"
            },
            "timestamp": datetime.now(timezone.utc).isoformat()
        }]
    }
    
    try:
        response = requests.post(
            webhook_url,
            json=payload,
            headers={"Content-Type": "application/json"}
        )
        response.raise_for_status()
    except Exception as e:
        logger.error("Erro ao enviar notificação para o Discord", exc_info=True)

def get_db_connection():
    """Estabelece uma nova conexão com o banco de dados"""
    database_url = os.getenv('DATABASE_URL')
    if not database_url:
        raise ValueError("A variável de ambiente DATABASE_URL não está definida no arquivo .env")
    try:
        conn = psycopg2.connect(database_url)
        logger.info("Nova conexão com o banco de dados estabelecida com sucesso")
        return conn
    except psycopg2.Error as e:
        logger.error(f"Erro ao conectar ao banco de dados: {e}", exc_info=True)
        raise

def main():
    # Envia notificação de inicialização
    send_startup_notification()
    
    # Lista para armazenar IDs com erro
    error_ids = []
    updated_count = 0

    try:
        while True:
            # Estabelece nova conexão para o ciclo
            conn = get_db_connection()
            
            try:
                # Fetch all mal_ids from the table
                with conn.cursor() as cur:
                    cur.execute("SELECT mal_id FROM public.animes")
                    mal_ids = [row[0] for row in cur.fetchall()]

                if not mal_ids:
                    logger.info("No animes found in the database. Sleeping for 30 minutes.")
                else:
                    logger.info(f"Starting update for {len(mal_ids)} animes...")
                    # Use ThreadPoolExecutor for parallel updates (I/O-bound, so threading is fine)
                    # Process in smaller batches to better control rate limiting
                    batch_size = 10  # Process 10 at a time
                    total_animes = len(mal_ids)
                    total_batches = (total_animes + batch_size - 1) // batch_size  # Arredonda para cima
                    
                    for batch_num, i in enumerate(range(0, total_animes, batch_size), 1):
                        batch = mal_ids[i:i + batch_size]
                        processed = min(i + batch_size, total_animes)
                        progress = (i / total_animes) * 100
                        
                        logger.info(f"Processando lote {batch_num}/{total_batches} - "
                                    f"Animes: {processed}/{total_animes} ({progress:.1f}%)")
                        
                        # Use ThreadPoolExecutor with a single worker to avoid overwhelming the API
                        with ThreadPoolExecutor(max_workers=1) as executor:
                            results = list(executor.map(lambda mid: update_anime(mid, conn, error_ids), batch))
                            updated_count += len([r for r in results if r is True])

                logger.info(f"Update cycle completed. Updated {updated_count} animes. "
                            f"Errors: {len(error_ids)}")
                if error_ids:
                    logger.warning(f"IDs com erro: {error_ids}")
                
                # Envia notificação para o Discord
                send_discord_notification(updated_count, error_ids)
                
                # Reseta os contadores para o próximo ciclo
                updated_count = 0
                error_ids = []
                
            finally:
                # Fecha a conexão ao final do ciclo
                if conn:
                    conn.close()
                    logger.info("Conexão com o banco de dados fechada")
            
            time.sleep(30 * 60)  # 30 minutes in seconds
    except KeyboardInterrupt:
        logger.info("Script interrupted. Sending final notification...")
        send_discord_notification(updated_count, error_ids)
        logger.info("Exiting.")
    except Exception as e:
        logger.error(f"Error in main loop: {e}", exc_info=True)
        send_discord_notification(updated_count, error_ids)
        raise

if __name__ == "__main__":
    main()