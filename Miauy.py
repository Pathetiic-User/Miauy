import discord
from discord import app_commands
from dotenv import load_dotenv
import os
import psycopg2
import logging

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('discord_bot.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Carrega variáveis de ambiente
load_dotenv()
DISCORD_TOKEN = os.getenv('DISCORD_TOKEN')
DATABASE_URL = os.getenv('DATABASE_URL')

# Verifica conexão com o banco
def check_db_connection():
    try:
        conn = psycopg2.connect(DATABASE_URL)
        conn.close()
        return True
    except Exception as e:
        logger.error(f"Erro ao conectar ao banco de dados: {e}")
        return False

# Intents do Discord
intents = discord.Intents.default()
intents.message_content = True

client = discord.Client(intents=intents)
tree = app_commands.CommandTree(client)

# Evento: Bot pronto
@client.event
async def on_ready():
    logger.info(f'Bot logado como {client.user}')
    db_status = "conectado" if check_db_connection() else "desconectado"
    logger.info(f'Banco de dados: {db_status}')
    try:
        synced = await tree.sync()
        logger.info(f'Sincronizados {len(synced)} comandos.')
    except Exception as e:
        logger.error(f'Erro ao sincronizar comandos: {e}')

# Comando /start
@tree.command(name='start', description='Inicia o bot e verifica o status')
async def start(interaction: discord.Interaction):
    db_status = "conectado" if check_db_connection() else "desconectado"
    await interaction.response.send_message(f'Olá! O bot está online. Banco de animes: {db_status}. 🚀')

# Roda o bot
client.run(DISCORD_TOKEN)