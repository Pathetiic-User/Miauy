import time
import requests
import psycopg2
import json
import logging
import argparse
import os
from datetime import datetime
from deep_translator import GoogleTranslator
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from jsonschema import validate, ValidationError
from dotenv import load_dotenv

# Carregar variáveis de ambiente do arquivo .env
load_dotenv()

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('anime_scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Carregar configurações de arquivo ou argumentos
def load_config():
    parser = argparse.ArgumentParser(description='Anime Scraper Configuration')
    parser.add_argument('--max-workers', type=int, default=3, help='Número máximo de workers para requisições paralelas')
    parser.add_argument('--request-delay', type=float, default=0.35, help='Delay entre requisições em segundos')
    parser.add_argument('--config-file', type=str, default='config.json', help='Arquivo de configuração')
    args = parser.parse_args()

    # Carregar variáveis de ambiente necessárias
    database_url = os.getenv('DATABASE_URL')
    mal_client_id = os.getenv('MAL_CLIENT_ID')
    
    if not database_url:
        raise ValueError("DATABASE_URL não encontrado no arquivo .env. Certifique-se de que o arquivo .env existe e contém DATABASE_URL.")
    
    if not mal_client_id:
        logger.warning("MAL_CLIENT_ID não encontrado no arquivo .env. As requisições serão feitas sem autenticação, o que pode resultar em limites de taxa mais baixos.")

    config = {
        'MAX_WORKERS': args.max_workers,
        'REQUEST_DELAY': args.request_delay,
        'DATABASE_URL': database_url,
        'MAL_CLIENT_ID': mal_client_id,  # Adiciona o MAL_CLIENT_ID às configurações
        'LANGUAGE': "pt",
        'MAX_RETRIES': 3,
        'PROGRESS_FILE': "progress.json",
        'TIMEOUT': 15
    }

    if os.path.exists(args.config_file):
        try:
            with open(args.config_file, 'r') as f:
                file_config = json.load(f)
                config.update(file_config)
        except Exception as e:
            logger.error(f"Erro ao carregar arquivo de configuração {args.config_file}: {e}")

    # Atualizar com argumentos da linha de comando, se fornecidos
    config['MAX_WORKERS'] = args.max_workers if args.max_workers != 3 else config['MAX_WORKERS']
    config['REQUEST_DELAY'] = args.request_delay if args.request_delay != 0.35 else config['REQUEST_DELAY']
    
    return config

CONFIG = load_config()
DATABASE_URL = CONFIG['DATABASE_URL']
LANGUAGE = CONFIG['LANGUAGE']
MAX_RETRIES = CONFIG['MAX_RETRIES']
REQUEST_DELAY = CONFIG['REQUEST_DELAY']
PROGRESS_FILE = CONFIG['PROGRESS_FILE']
MAX_WORKERS = CONFIG['MAX_WORKERS']
TIMEOUT = CONFIG['TIMEOUT']

translator = GoogleTranslator(source='auto', target=LANGUAGE)

# Tradução para status
STATUS_TRANSLATIONS = {
    'Finished Airing': 'Finalizado',
    'Currently Airing': 'Em Exibição',
    'Not yet aired': 'Não Lançado',
    'Not yet released': 'Não Lançado',
    'Publishing': 'Em Publicação',
    'Complete': 'Completo',
    'Discontinued': 'Descontinuado',
    'Hiatus': 'Em Hiato'
}

# Tradução para season
SEASON_TRANSLATIONS = {
    'Winter': 'Inverno',
    'Spring': 'Primavera',
    'Summer': 'Verão',
    'Fall': 'Outono'
}

# Schema para validação de dados da API
ANIME_SCHEMA = {
    "type": "object",
    "properties": {
        "mal_id": {"type": ["integer", "null"]},
        "title": {"type": ["string", "null"]},
        "title_english": {"type": ["string", "null"]},
        "title_japanese": {"type": ["string", "null"]},
        "title_synonyms": {"type": ["array", "null"], "items": {"type": "string"}},
        "type": {"type": ["string", "null"]},
        "source": {"type": ["string", "null"]},
        "episodes": {"type": ["integer", "null"]},
        "status": {"type": ["string", "null"]},
        "airing": {"type": ["boolean", "null"]},
        "aired": {
            "type": ["object", "null"],
            "properties": {
                "from": {
                    "type": ["object", "string", "null"],
                    "properties": {
                        "day": {"type": ["integer", "null"]},
                        "month": {"type": ["integer", "null"]},
                        "year": {"type": ["integer", "null"]}
                    }
                },
                "to": {
                    "type": ["object", "string", "null"],
                    "properties": {
                        "day": {"type": ["integer", "null"]},
                        "month": {"type": ["integer", "null"]},
                        "year": {"type": ["integer", "null"]}
                    }
                }
            }
        },
        "duration": {"type": ["string", "null"]},
        "rating": {"type": ["string", "null"]},
        "score": {"type": ["number", "null"]},
        "scored_by": {"type": ["integer", "null"]},
        "rank": {"type": ["integer", "null"]},
        "popularity": {"type": ["integer", "null"]},
        "members": {"type": ["integer", "null"]},
        "favorites": {"type": ["integer", "null"]},
        "synopsis": {"type": ["string", "null"]},
        "background": {"type": ["string", "null"]},
        "premiered": {"type": ["string", "null"]},
        "broadcast": {"type": ["object", "null"], "properties": {
            "string": {"type": ["string", "null"]}
        }},
        "season": {"type": ["string", "null"]},
        "year": {"type": ["integer", "null"]},
        "images": {"type": ["object", "null"], "properties": {
            "jpg": {"type": ["object", "null"], "properties": {
                "image_url": {"type": ["string", "null"]}
            }}
        }},
        "trailer": {"type": ["object", "null"]},
        "producers": {"type": ["array", "null"], "items": {"type": "object"}},
        "licensors": {"type": ["array", "null"], "items": {"type": "object"}},
        "studios": {"type": ["array", "null"], "items": {"type": "object"}},
        "genres": {"type": ["array", "null"], "items": {"type": "object"}},
        "explicit_genres": {"type": ["array", "null"], "items": {"type": "object"}},
        "themes": {"type": ["array", "null"], "items": {"type": "object"}},
        "demographics": {"type": ["array", "null"], "items": {"type": "object"}},
        "relations": {"type": ["array", "null"], "items": {"type": "object"}},
        "approved": {"type": ["boolean", "null"]}
    },
    "required": ["mal_id"]
}

def init_database():
    """Inicializa o banco de dados e cria a tabela se não existir"""
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()
        
        # Verificar se a tabela já existe
        cursor.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_name = 'animes'
        );
        """)
        table_exists = cursor.fetchone()[0]
        
        if table_exists:
            # Verificar se a coluna season_pt existe
            cursor.execute("""
            SELECT column_name FROM information_schema.columns 
            WHERE table_name = 'animes' AND column_name = 'season_pt';
            """)
            if not cursor.fetchone():
                logger.info("Adicionando coluna season_pt à tabela animes...")
                cursor.execute("ALTER TABLE animes ADD COLUMN season_pt TEXT;")
                conn.commit()
            return conn, cursor
        
        # Criar tabela animes
        cursor.execute("""
        CREATE TABLE animes (
            mal_id INTEGER PRIMARY KEY,
            title TEXT,
            title_english TEXT,
            title_japanese TEXT,
            title_synonyms TEXT[],
            type TEXT,
            source TEXT,
            episodes INTEGER,
            status TEXT,
            status_pt TEXT,
            airing BOOLEAN,
            aired_from DATE,
            aired_to DATE,
            duration TEXT,
            rating TEXT,
            score REAL,
            scored_by INTEGER,
            rank INTEGER,
            popularity INTEGER,
            members INTEGER,
            favorites INTEGER,
            synopsis TEXT,
            synopsis_pt TEXT,
            background TEXT,
            premiered TEXT,
            broadcast TEXT,
            url TEXT,
            images JSONB,
            trailer JSONB,
            producers JSONB,
            licensors JSONB,
            studios JSONB,
            genres JSONB,
            explicit_genres JSONB,
            themes JSONB,
            demographics JSONB,
            relations JSONB,
            approved BOOLEAN,
            season TEXT,
            season_pt TEXT,
            year INTEGER,
            image_url TEXT,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        )
        """)
        
        cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_animes_title ON animes (title);
        CREATE INDEX IF NOT EXISTS idx_animes_genres ON animes USING GIN (genres);
        CREATE INDEX IF NOT EXISTS idx_animes_score ON animes (score) WHERE score IS NOT NULL;
        """)
        
        conn.commit()
        logger.info("Tabela animes criada com sucesso.")
        return conn, cursor
    except Exception as e:
        logger.error(f"Erro ao inicializar o banco de dados: {e}")
        raise

conn, cursor = init_database()

# Funções de cache de progresso
def load_progress():
    if os.path.exists(PROGRESS_FILE):
        try:
            with open(PROGRESS_FILE, 'r') as f:
                return json.load(f).get('last_page', 1)
        except Exception as e:
            logger.error(f"Erro ao carregar progresso: {e}")
    return 1

def save_progress(page):
    try:
        with open(PROGRESS_FILE, 'w') as f:
            json.dump({"last_page": page}, f)
        logger.info(f"Progresso salvo: página {page}")
    except Exception as e:
        logger.error(f"Erro ao salvar progresso: {e}")

def safe_translate(text, retries=MAX_RETRIES, delay=5):
    if not text:
        return None
    for attempt in range(retries):
        try:
            return translator.translate(text)
        except Exception as e:
            logger.error(f"Erro na tradução (tentativa {attempt+1}/{retries}): {e}")
            time.sleep(delay)
    return None

def fetch_anime_page(page, retries=MAX_RETRIES, timeout=TIMEOUT):
    """
    Busca uma página de animes da API Jikan v4.
    
    Args:
        page (int): Número da página a ser buscada
        retries (int): Número máximo de tentativas
        timeout (int): Timeout em segundos para a requisição
        
    Returns:
        tuple: (lista de animes, dados de paginação, novo delay)
    """
    url = f"https://api.jikan.moe/v4/anime?page={page}"
    dynamic_delay = REQUEST_DELAY
    
    # Configura os headers com User-Agent e, se disponível, o MAL_CLIENT_ID
    headers = {'User-Agent': 'Mozilla/5.0'}
    if 'MAL_CLIENT_ID' in globals() and MAL_CLIENT_ID:
        headers['X-MAL-CLIENT-ID'] = MAL_CLIENT_ID
    
    for attempt in range(retries):
        try:
            response = requests.get(url, headers=headers, timeout=timeout)
            response.raise_for_status()
            
            # Log de rate limit
            remaining = response.headers.get('X-RateLimit-Remaining', 'N/A')
            limit = response.headers.get('X-RateLimit-Limit', 'N/A')
            
            if remaining != 'N/A':
                remaining = int(remaining)
                logger.info(f"Rate limit: {remaining}/{limit} requisições restantes")
                
                # Ajusta o delay dinamicamente com base no rate limit
                if remaining < 5:
                    dynamic_delay = min(dynamic_delay * 2, 10)  # Aumenta o delay, máximo 10s
                    logger.warning(f"Rate limit baixo ({remaining}). Aumentando delay para {dynamic_delay}s")
                elif remaining > 30:
                    dynamic_delay = max(dynamic_delay * 0.75, 0.1)  # Reduz o delay, mínimo 0.1s
                    logger.info(f"Rate limit alto ({remaining}). Reduzindo delay para {dynamic_delay}s")
            
            # Processa a resposta
            data = response.json()
            return data.get('data', []), data.get('pagination', {}), dynamic_delay
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Erro na requisição (tentativa {attempt+1}/{retries}): {e}")
            if attempt < retries - 1:  # Não dormir após a última tentativa
                sleep_time = 5 * (attempt + 1)
                logger.info(f"Aguardando {sleep_time} segundos antes de tentar novamente...")
                time.sleep(sleep_time)
    return None, None, dynamic_delay

def validate_anime_data(anime):
    """Valida os dados do anime contra o schema"""
    try:
        validate(instance=anime, schema=ANIME_SCHEMA)
        return True
    except ValidationError as e:
        logger.error(f"Erro de validação para anime {anime.get('mal_id', 'unknown')}: {str(e)}")
        return False

def extract_date(date_input):
    """Extrai data do dicionário ou string retornado pela API"""
    if not date_input:
        return None
    try:
        if isinstance(date_input, str):
            # Parse ISO 8601 date string (e.g., '2013-12-31T00:00:00+00:00')
            parsed_date = datetime.fromisoformat(date_input.replace('Z', '+00:00'))
            return parsed_date.strftime('%Y-%m-%d')
        elif isinstance(date_input, dict):
            # Handle object format (e.g., {"day": 31, "month": 12, "year": 2013})
            return f"{date_input.get('year')}-{date_input.get('month'):02d}-{date_input.get('day'):02d}" if date_input.get('year') else None
        else:
            logger.warning(f"Formato de data inesperado: {date_input}")
            return None
    except Exception as e:
        logger.error(f"Erro ao extrair data: {e}")
        return None

def extract_entities(entities):
    """Extrai lista de entidades (gêneros, estúdios, etc)"""
    if not entities:
        return []
    return [{"id": e.get('mal_id'), "name": e.get('name'), "type": e.get('type')} for e in entities]

def process_anime(anime):
    try:
        if not validate_anime_data(anime):
            logger.warning(f"Dados inválidos para anime {anime.get('mal_id', 'unknown')}. Pulando...")
            return None
        
        mal_id = anime.get('mal_id')
        title = anime.get('title')
        title_english = anime.get('title_english')
        title_japanese = anime.get('title_japanese')
        title_synonyms = anime.get('title_synonyms', [])
        type_ = anime.get('type')
        source = anime.get('source')
        episodes = anime.get('episodes')
        status_en = anime.get('status')
        status_pt = STATUS_TRANSLATIONS.get(status_en, status_en) if status_en else None
        status = status_en
        airing = anime.get('airing', False)
        aired = anime.get('aired', {})
        aired_from = extract_date(aired.get('from'))
        aired_to = extract_date(aired.get('to'))
        duration = anime.get('duration')
        rating = anime.get('rating')
        score = anime.get('score')
        scored_by = anime.get('scored_by')
        rank = anime.get('rank')
        popularity = anime.get('popularity')
        members = anime.get('members')
        favorites = anime.get('favorites')
        synopsis = anime.get('synopsis')
        synopsis_pt = None  # Tradução de synopsis desativada
        background = anime.get('background')
        premiered = anime.get('premiered')
        broadcast = anime.get('broadcast', {}).get('string')
        season = anime.get('season')
        season_normalized = season.capitalize() if season else None
        season_pt = SEASON_TRANSLATIONS.get(season_normalized, None) if season_normalized else None
        if season and season_pt is None:
            logger.warning(f"Valor de season não mapeado para anime {mal_id}: {season} (normalizado: {season_normalized})")
        url = anime.get('url')
        images = json.dumps(anime.get('images', {}))
        trailer = json.dumps(anime.get('trailer', {}))
        producers = json.dumps(extract_entities(anime.get('producers', [])))
        licensors = json.dumps(extract_entities(anime.get('licensors', [])))
        studios = json.dumps(extract_entities(anime.get('studios', [])))
        genres = json.dumps(extract_entities(anime.get('genres', [])))
        explicit_genres = json.dumps(extract_entities(anime.get('explicit_genres', [])))
        themes = json.dumps(extract_entities(anime.get('themes', [])))
        demographics = json.dumps(extract_entities(anime.get('demographics', [])))
        relations = json.dumps(anime.get('relations', []))
        approved = anime.get('approved', False)
        year = anime.get('year')
        if not year:
            # Tentar extrair de aired.from
            aired_from_data = anime.get('aired', {}).get('from')
            if aired_from_data:
                try:
                    if isinstance(aired_from_data, str):
                        year = int(datetime.fromisoformat(aired_from_data.replace('Z', '+00:00')).year)
                    elif isinstance(aired_from_data, dict):
                        year = aired_from_data.get('year')
                except Exception as e:
                    logger.warning(f"Erro ao extrair ano de aired.from para anime {mal_id}: {e}")
            # Tentar extrair de premiered (ex.: "Spring 2023")
            if not year and anime.get('premiered'):
                try:
                    year = int(anime.get('premiered').split()[-1])
                except Exception as e:
                    logger.warning(f"Erro ao extrair ano de premiered para anime {mal_id}: {e}")
        image_url = anime.get('images', {}).get('jpg', {}).get('image_url')
        
        params = (
            mal_id, title, title_english, title_japanese, title_synonyms,
            type_, source, episodes, status, status_pt, airing,
            aired_from, aired_to, duration, rating,
            score, scored_by, rank, popularity, members, favorites,
            synopsis, synopsis_pt, background,
            premiered, broadcast,
            url, images, trailer,
            producers, licensors, studios, genres, explicit_genres, themes, demographics,
            relations,
            approved, season, season_pt, year,
            image_url
        )
        return params
    except Exception as e:
        logger.error(f"Erro ao processar anime {anime.get('mal_id', 'unknown')}: {e}")
        return None

def process_anime_batch(animes_batch):
    """Processa um lote de animes e retorna os parâmetros para inserção em lote"""
    params_batch = []
    for anime in animes_batch:
        params = process_anime(anime)
        if params:
            params_batch.append(params)
    return params_batch

def insert_anime_batch(params_batch):
    """Insere um lote de animes no banco de dados"""
    if not params_batch:
        return 0
        
    query = """
    INSERT INTO animes (
        mal_id, title, title_english, title_japanese, title_synonyms,
        type, source, episodes, status, status_pt, airing,
        aired_from, aired_to, duration, rating,
        score, scored_by, rank, popularity, members, favorites,
        synopsis, synopsis_pt, background,
        premiered, broadcast,
        url, images, trailer,
        producers, licensors, studios, genres, explicit_genres, themes, demographics,
        relations,
        approved, season, season_pt, year,
        image_url
    ) VALUES (
        %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s,
        %s, %s, %s,
        %s, %s,
        %s, %s, %s,
        %s, %s, %s, %s, %s, %s, %s,
        %s,
        %s, %s, %s, %s,
        %s
    )
    ON CONFLICT (mal_id) DO UPDATE SET
        title = EXCLUDED.title,
        title_english = EXCLUDED.title_english,
        title_japanese = EXCLUDED.title_japanese,
        title_synonyms = EXCLUDED.title_synonyms,
        type = EXCLUDED.type,
        source = EXCLUDED.source,
        episodes = EXCLUDED.episodes,
        status = EXCLUDED.status,
        status_pt = EXCLUDED.status_pt,
        airing = EXCLUDED.airing,
        aired_from = EXCLUDED.aired_from,
        aired_to = EXCLUDED.aired_to,
        duration = EXCLUDED.duration,
        rating = EXCLUDED.rating,
        score = EXCLUDED.score,
        scored_by = EXCLUDED.scored_by,
        rank = EXCLUDED.rank,
        popularity = EXCLUDED.popularity,
        members = EXCLUDED.members,
        favorites = EXCLUDED.favorites,
        synopsis = EXCLUDED.synopsis,
        synopsis_pt = EXCLUDED.synopsis_pt,
        background = EXCLUDED.background,
        premiered = EXCLUDED.premiered,
        broadcast = EXCLUDED.broadcast,
        url = EXCLUDED.url,
        images = EXCLUDED.images,
        trailer = EXCLUDED.trailer,
        producers = EXCLUDED.producers,
        licensors = EXCLUDED.licensors,
        studios = EXCLUDED.studios,
        genres = EXCLUDED.genres,
        explicit_genres = EXCLUDED.explicit_genres,
        themes = EXCLUDED.themes,
        demographics = EXCLUDED.demographics,
        relations = EXCLUDED.relations,
        approved = EXCLUDED.approved,
        season = EXCLUDED.season,
        season_pt = EXCLUDED.season_pt,
        year = EXCLUDED.year,
        image_url = EXCLUDED.image_url,
        updated_at = NOW()
    """
    
    try:
        cursor.executemany(query, params_batch)
        conn.commit()
        logger.info(f"Inseridos {len(params_batch)} animes com sucesso.")
        return len(params_batch)
    except Exception as e:
        conn.rollback()
        logger.error(f"Erro ao inserir lote de {len(params_batch)} animes: {e}")
        return 0

def fetch_pages_parallel(pages):
    """Fetch múltiplas páginas em paralelo com limite de workers"""
    all_animes = []
    all_pagination = None
    dynamic_delay = REQUEST_DELAY
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_page = {executor.submit(fetch_anime_page, page): page for page in pages}
        for future in as_completed(future_to_page):
            page = future_to_page[future]
            try:
                animes, pagination, new_delay = future.result()
                dynamic_delay = new_delay
                if animes:
                    all_animes.extend(animes)
                    if not all_pagination:
                        all_pagination = pagination
                logger.info(f"Página {page} fetchada: {len(animes) if animes else 0} animes")
            except Exception as e:
                logger.error(f"Erro no fetch da página {page}: {e}")
            time.sleep(dynamic_delay)
    return all_animes, all_pagination, dynamic_delay

def main():
    pagina_atual = load_progress()
    total_animes_processados = 0
    dynamic_delay = REQUEST_DELAY
    
    logger.info(f"Iniciando a partir da página {pagina_atual}")
    
    try:
        while True:
            logger.info(f"\n=== Processando a partir da Página {pagina_atual} ===")
            
            batch_pages = list(range(pagina_atual, pagina_atual + MAX_WORKERS))
            animes_batch_all, pagination, dynamic_delay = fetch_pages_parallel(batch_pages)
            if not animes_batch_all:
                logger.info("Nenhum anime encontrado. Fim da lista.")
                break
                
            total_animes = len(animes_batch_all)
            logger.info(f"Processando {total_animes} animes do batch...")
            
            params_batch = process_anime_batch(animes_batch_all)
            salvos = insert_anime_batch(params_batch)
            total_animes_processados += salvos
            
            save_progress(max(batch_pages))
            pagina_atual = max(batch_pages) + 1
            
            logger.info(f"Batch concluído (páginas {min(batch_pages)}-{max(batch_pages)}): {salvos}/{total_animes} animes processados")
            
            if not pagination or not pagination.get('has_next_page', True):
                logger.info("\nÚltima página atingida.")
                break
                
            time.sleep(dynamic_delay * MAX_WORKERS)
            
    except KeyboardInterrupt:
        logger.info("\nProcesso interrompido pelo usuário.")
    except Exception as e:
        logger.error(f"\nErro inesperado: {e}")
    finally:
        logger.info(f"\nTotal de animes processados: {total_animes_processados}")
        cursor.close()
        conn.close()
    
    logger.info(f"\nProcesso finalizado! Total de animes processados: {total_animes_processados}")

if __name__ == "__main__":
    main()