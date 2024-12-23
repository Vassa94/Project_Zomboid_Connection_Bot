from ftplib import FTP
import discord
import asyncio
import time
import hashlib
from dotenv import load_dotenv
import os
import datetime

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
    Devuelve un conjunto de hashes y un diccionario con sus marcas de tiempo.
    """
    processed_lines = set()
    timestamps = {}
    try:
        with open(PROCESSED_LOG_FILE, 'r') as file:
            for line in file.readlines():
                hash_, timestamp = line.strip().split(',')
                processed_lines.add(hash_)
                timestamps[hash_] = datetime.datetime.fromisoformat(timestamp)
    except FileNotFoundError:
        pass  # Si el archivo no existe, devolvemos un conjunto vacío
    return processed_lines, timestamps

def save_processed_line(line_hash, timestamps):
    """
    Guardar las líneas procesadas en el archivo de registro con sus marcas de tiempo.
    """
    with open(PROCESSED_LOG_FILE, 'w') as file:
        for hash_, timestamp in timestamps.items():
            file.write(f"{hash_},{timestamp.isoformat()}\n")



def clean_old_processed_lines(processed_lines, timestamps, hours=2):
    """
    Limpiar líneas procesadas que tienen más de `hours` horas.
    """
    cutoff_time = datetime.datetime.now() - datetime.timedelta(hours=hours)
    hashes_to_remove = {hash_ for hash_, timestamp in timestamps.items() if timestamp < cutoff_time}
    for hash_ in hashes_to_remove:
        processed_lines.remove(hash_)
        del timestamps[hash_]


def clean_old_logs(log_file, hours=2):
    """
    Eliminar líneas de logs más antiguas de `hours` horas.
    """
    cutoff_time = datetime.datetime.now() - datetime.timedelta(hours=hours)
    try:
        with open(log_file, 'r', encoding='utf-8') as file:
            lines = file.readlines()

        recent_lines = []
        for line in lines:
            # Filtrar líneas recientes basadas en marcas de tiempo en el log
            parts = line.split('>')
            if len(parts) > 1:
                try:
                    timestamp_str = parts[1].split('[')[1].split(']')[0]
                    log_time = datetime.datetime.strptime(timestamp_str, "%y-%m-%d %H:%M:%S.%f")
                    if log_time >= cutoff_time:
                        recent_lines.append(line)
                except (IndexError, ValueError):
                    recent_lines.append(line)  # Si no se puede parsear, asumimos que es reciente

        with open(log_file, 'w', encoding='utf-8') as file:
            file.writelines(recent_lines)
    except FileNotFoundError:
        print(f"{log_file} no encontrado. No se realizó ninguna limpieza.")


def calculate_line_hash(line):
    """
    Calcular un hash único para la línea, asegurando que el registro sea compacto y consistente.
    """
    return hashlib.sha256(line.encode('utf-8')).hexdigest()

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


async def process_logs(processed_lines, timestamps):
    """
    Leer y procesar las nuevas líneas del archivo de logs descargado.
    """
    try:
        # Limpia logs locales antes de procesar
        clean_old_logs(LOCAL_LOG_COPY)

        # Lee las líneas del archivo de logs descargado
        with open(LOCAL_LOG_COPY, 'r', encoding='utf-8') as file:
            lines = file.readlines()

        # Filtrar líneas no procesadas y descartar las que comienzan con "WARN : General"
        new_lines = []
        for line in lines:
            if line.startswith("WARN : General"):
                continue  # Ignorar estas líneas completamente
            line_hash = calculate_line_hash(line)
            if line_hash not in processed_lines:
                new_lines.append((line, line_hash))

        cutoff_time = datetime.datetime.now() - datetime.timedelta(minutes=10)

        # Procesar nuevas líneas
        for line, line_hash in new_lines:
            # Intentar extraer o calcular el timestamp
            log_time = None
            try:
                if "[" in line and "]" in line:
                    timestamp_str = line.split('[')[1].split(']')[0]
                    log_time = datetime.datetime.strptime(timestamp_str, "%y-%m-%d %H:%M:%S.%f")
                else:
                    # Si no hay timestamp explícito, calcular un intermedio
                    log_time = calculate_intermediate_timestamp(lines, line, cutoff_time)
            except (IndexError, ValueError):
                print(f"Error al analizar la marca de tiempo de la línea: {line.strip()}")
                continue

            # Ignorar logs más antiguos que 10 minutos
            if log_time < cutoff_time:
                print(f"Línea ignorada por antigüedad: {line.strip()}")
                continue

            # Procesar conexiones
            if "PlayerConnectionMessage\tplayerConnected" in line:
                player_name = line.split("\t")[-1].strip()
                print(f"👋 ¡{player_name} se ha conectado al servidor!")
                processed_lines.add(line_hash)
                timestamps[line_hash] = log_time
                save_processed_line(line_hash, timestamps)
                await send_discord_message(player_name, "connected")

            # Procesar desconexiones
            elif "Disconnected player" in line:
                player_name = line.split('"')[1].strip()
                print(f"👋 ¡{player_name} se ha desconectado del servidor!")
                processed_lines.add(line_hash)
                timestamps[line_hash] = log_time
                save_processed_line(line_hash, timestamps)
                await send_discord_message(player_name, "disconnected")
    except FileNotFoundError:
        print("Archivo de logs no encontrado.")
    except UnicodeDecodeError as e:
        print(f"Error al decodificar el archivo: {e}")
    return processed_lines, timestamps



def calculate_intermediate_timestamp(lines, line, cutoff_time):
    """
    Calcular un timestamp intermedio para una línea sin timestamp explícito.
    """
    previous_timestamp = None
    next_timestamp = None

    # Buscar el timestamp anterior
    for previous_line in reversed(lines[:lines.index(line)]):
        if "[" in previous_line and "]" in previous_line:
            try:
                timestamp_str = previous_line.split('[')[1].split(']')[0]
                previous_timestamp = datetime.datetime.strptime(timestamp_str, "%y-%m-%d %H:%M:%S.%f")
                break
            except (IndexError, ValueError):
                continue

    # Buscar el timestamp siguiente
    for next_line in lines[lines.index(line) + 1:]:
        if "[" in next_line and "]" in next_line:
            try:
                timestamp_str = next_line.split('[')[1].split(']')[0]
                next_timestamp = datetime.datetime.strptime(timestamp_str, "%y-%m-%d %H:%M:%S.%f")
                break
            except (IndexError, ValueError):
                continue

    # Calcular el promedio entre los timestamps anterior y siguiente
    if previous_timestamp and next_timestamp:
        return previous_timestamp + (next_timestamp - previous_timestamp) / 2
    elif previous_timestamp:
        return previous_timestamp
    elif next_timestamp:
        return next_timestamp
    else:
        # Si no hay timestamps válidos, usa la hora actual como fallback
        return datetime.datetime.now()




def initialize_processed_logs_with_timestamps():
    """
    Actualizar el archivo processed_logs.txt agregando marcas de tiempo actuales.
    """
    try:
        with open(PROCESSED_LOG_FILE, 'r') as file:
            lines = file.readlines()

        with open(PROCESSED_LOG_FILE, 'w') as file:
            current_time = datetime.datetime.now().isoformat()  # Tiempo actual
            for line in lines:
                hash_ = line.strip()
                file.write(f"{hash_},{current_time}\n")
        print("Archivo processed_logs.txt actualizado con marcas de tiempo.")
    except FileNotFoundError:
        print("El archivo processed_logs.txt no existe, creando uno vacío.")
        with open(PROCESSED_LOG_FILE, 'w') as file:
            pass  # Crear un archivo vacío si no existe

async def main():
    """
    Bucle principal para descargar y procesar los logs periódicamente.
    """
    initialize_processed_logs_with_timestamps()

    processed_lines, timestamps = load_processed_lines()  # Cargar líneas procesadas desde el archivo
    while True:
        download_logs()
        processed_lines, timestamps = await process_logs(processed_lines, timestamps)
        clean_old_processed_lines(processed_lines, timestamps)  # Limpiar procesados antiguos
        await asyncio.sleep(10)  # Esperar 10 segundos antes de volver a verificar

@client.event
async def on_ready():
    print(f"Bot de Discord conectado como {client.user}")
    await main()

client.run(DISCORD_TOKEN)
