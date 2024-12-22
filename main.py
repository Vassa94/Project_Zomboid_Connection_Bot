from ftplib import FTP
import discord
import asyncio
import time
import hashlib
from dotenv import load_dotenv
import os

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

def load_processed_lines():
    """
    Cargar líneas procesadas desde el archivo de registro.
    """
    try:
        with open(PROCESSED_LOG_FILE, 'r') as file:
            return set(line.strip() for line in file.readlines())
    except FileNotFoundError:
        return set()  # Si el archivo no existe, devolver un conjunto vacío

def save_processed_line(line):
    """
    Guardar una línea procesada en el archivo de registro.
    """
    with open(PROCESSED_LOG_FILE, 'a') as file:
        file.write(line + '\n')

def calculate_line_hash(line):
    """
    Calcular un hash único para la línea, asegurando que el registro sea compacto y consistente.
    """
    return hashlib.sha256(line.encode('utf-8')).hexdigest()

async def send_discord_message(player_name):
    """
    Enviar un mensaje a Discord notificando la conexión de un jugador.
    """
    channel = client.get_channel(CHANNEL_ID)
    if channel:
        await channel.send(f"👋 ¡{player_name} se ha conectado al servidor!")

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

async def process_logs(processed_lines):
    """
    Leer y procesar las nuevas líneas del archivo de logs descargado.
    """
    try:
        with open(LOCAL_LOG_COPY, 'r', encoding='utf-8') as file:  # Forzar UTF-8
            lines = file.readlines()
        new_lines = [line for line in lines if calculate_line_hash(line) not in processed_lines]
        for line in new_lines:
            # Detectar líneas con conexiones de jugadores
            if "PlayerConnectionMessage\tplayerConnected" in line:
                parts = line.split("\t")  # Dividir por tabulaciones
                player_name = parts[-1].strip()  # Extraer el último elemento (nombre del jugador)
                print(f"👋 ¡{player_name} se ha conectado al servidor!")
                line_hash = calculate_line_hash(line)
                processed_lines.add(line_hash)
                save_processed_line(line_hash)  # Guardar en el registro persistente
                # Enviar mensaje a Discord
                await send_discord_message(player_name)
    except FileNotFoundError:
        print("Archivo de logs no encontrado.")
    except UnicodeDecodeError as e:
        print(f"Error al decodificar el archivo: {e}")
    return processed_lines

async def main():
    """
    Bucle principal para descargar y procesar los logs periódicamente.
    """
    processed_lines = load_processed_lines()  # Cargar líneas procesadas desde el archivo
    while True:
        download_logs()
        await process_logs(processed_lines)
        await asyncio.sleep(10)  # Esperar 10 segundos antes de volver a verificar

@client.event
async def on_ready():
    print(f"Bot de Discord conectado como {client.user}")
    await main()

client.run(DISCORD_TOKEN)
