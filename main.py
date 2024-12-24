from ftplib import FTP
import discord
import asyncio
import hashlib
import os
from dotenv import load_dotenv

load_dotenv()

# Configuración del servidor FTP
FTP_HOST = os.getenv('FTP_HOST')
FTP_USER = os.getenv('FTP_USER')
FTP_PASS = os.getenv('FTP_PASS')
LOG_PATH = os.getenv('LOG_PATH')
LOCAL_LOG_COPY = os.getenv('LOCAL_LOG_COPY')
PROCESSED_LOG_FILE = os.getenv('PROCESSED_LOG_FILE')

# Configuración del bot de Discord
DISCORD_TOKEN = os.getenv('DISCORD_TOKEN')
CHANNEL_ID = int(os.getenv('CHANNEL_ID'))  # Convertir a entero

# Configuración del cliente de Discord
intents = discord.Intents.default()
client = discord.Client(intents=intents)

def load_processed_hashes():
    """
    Cargar los hashes de líneas procesadas desde el archivo.
    """
    try:
        with open(PROCESSED_LOG_FILE, 'r') as file:
            return set(line.strip() for line in file.readlines())
    except FileNotFoundError:
        return set()

def save_processed_hash(line_hash):
    """
    Guardar un hash de línea procesada en el archivo.
    """
    with open(PROCESSED_LOG_FILE, 'a') as file:
        file.write(f"{line_hash}\n")

def calculate_line_hash(line):
    """
    Calcular un hash único para la línea.
    """
    return hashlib.sha256(line.encode('utf-8')).hexdigest()

def download_logs():
    """
    Conectarse al servidor FTP y descargar el archivo de logs.
    """
    try:
        print("Conectando al servidor FTP...")
        ftp = FTP(FTP_HOST)
        ftp.login(FTP_USER, FTP_PASS)
        print("Conexión exitosa. Descargando archivo de logs...")
        with open(LOCAL_LOG_COPY, 'wb') as f:
            ftp.retrbinary(f'RETR ' + LOG_PATH, f.write)
        ftp.quit()
        print("Archivo descargado correctamente.")
    except Exception as e:
        print(f"Error al descargar el archivo: {e}")

def read_last_lines(filepath, line_count=20):
    """
    Leer las últimas `line_count` líneas de un archivo.
    """
    try:
        with open(filepath, 'r', encoding='utf-8') as file:
            return file.readlines()[-line_count:]
    except FileNotFoundError:
        print("Archivo de logs no encontrado.")
        return []

async def send_discord_message(player_name, action):
    """
    Enviar un mensaje a Discord notificando la conexión o desconexión de un jugador.
    """
    channel = client.get_channel(CHANNEL_ID)
    if channel:
        if action == "connected":
            await channel.send(f"👋 ¡{player_name} se ha conectado al servidor!")
        elif action == "disconnected":
            await channel.send(f"👋 ¡{player_name} se ha desconectado del servidor!")

async def process_logs():
    """
    Procesar las últimas líneas del archivo de logs para detectar conexiones y desconexiones.
    """
    processed_hashes = load_processed_hashes()
    new_lines = read_last_lines(LOCAL_LOG_COPY)

    for line in new_lines:
        if "Connected new client" in line or "Disconnected player" in line:
            line_hash = calculate_line_hash(line)

            if line_hash not in processed_hashes:
                if "Connected new client" in line:
                    player_name = line.split("Connected new client ")[1].split(" ID")[0].strip()
                    print(f"Detectada conexión: {player_name}")
                    await send_discord_message(player_name, "connected")
                elif "Disconnected player" in line:
                    player_name = line.split("Disconnected player \"")[1].split("\"")[0].strip()
                    print(f"Detectada desconexión: {player_name}")
                    await send_discord_message(player_name, "disconnected")

                save_processed_hash(line_hash)

@client.event
async def on_ready():
    print(f"Bot de Discord conectado como {client.user}")
    while True:
        download_logs()
        await process_logs()
        await asyncio.sleep(10)  # Esperar 10 segundos antes de procesar nuevamente

client.run(DISCORD_TOKEN)
