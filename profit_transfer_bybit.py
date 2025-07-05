import json
import time
import logging
import mysql.connector
import ccxt
import uuid
from typing import List, Dict, Optional

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='transferencias.log'
)

# Cargar configuración del usuario desde JSON
def cargar_usuarios() -> List[Dict]:
    try:
        with open('user.json', 'r') as f:
            users = json.load(f)
        return [u for u in users if u.get("activo", False)]
    except Exception as e:
        logging.error(f"Error al cargar usuarios: {e}")
        raise

# Conectar a la base de datos
def conectar_db():
    try:
        conn = mysql.connector.connect(
            host='localhost',
            user='root',
            password='',
            database='profit_transfer'
        )
        return conn
    except mysql.connector.Error as err:
        logging.error(f"Error conectando a la base de datos: {err}")
        raise

# Realizar transferencia usando Bybit
def realizar_transferencia(usuario):
    try:
        exchange = ccxt.bybit({
            "apiKey": usuario["api_k"],
            "secret": usuario["api_s"],
            "enableRateLimit": True,
        })

        balance = exchange.fetch_balance()
        wallet_balance = round(balance["total"].get("USDT", 0), 4)
        kdt = usuario["KdT"]
        transfer = max(wallet_balance - kdt, 0)

        if transfer <= 0:
            logging.info(f"Saldo insuficiente para {usuario['user']}. Saldo: {wallet_balance} USDT")
            return

        # Generar UUID
        transfer_id = str(uuid.uuid4())

        # Ejecutar transferencia
        transfer_response = exchange.private_post_v5_asset_transfer_inter_transfer({
            'transferId': transfer_id,
            'coin': 'USDT',
            'amount': str(transfer),
            'fromAccountType': usuario["type"],
            'toAccountType': 'FUND',
            'fromMemberId': usuario["UID"],
            'toMemberId': 35671204,
        })

        logging.info(f"Transferencia realizada por {transfer} USDT para {usuario['user']}")

        # Guardar en BD
        guardar_registro(usuario, transfer)

    except Exception as e:
        logging.error(f"Error al procesar {usuario['user']}: {e}")

# Guardar registro en la base de datos
def guardar_registro(usuario, transfer):
    try:
        conn = conectar_db()
        cursor = conn.cursor()
        query = """
            INSERT INTO profits (user, uid, coin, profit, KdT)
            VALUES (%s, %s, %s, %s, %s)
        """
        values = (
            usuario["user"],
            usuario["UID"],
            usuario["coin"],
            transfer,
            usuario["KdT"]
        )
        cursor.execute(query, values)
        conn.commit()
        logging.info("Registro guardado en la base de datos.")
    except Exception as e:
        logging.error(f"Error al guardar en la base de datos: {e}")
    finally:
        if 'conn' in locals() and conn.is_connected():
            conn.close()

# Función principal
def main():
    usuarios = cargar_usuarios()
    for usuario in usuarios:
        realizar_transferencia(usuario)

# Programador de tareas
def programar_tarea():
    import schedule
    schedule.every().day.at("22:00").do(main)

    while True:
        schedule.run_pending()
        time.sleep(30)

if __name__ == "__main__":
    try:
        programar_tarea()
    except KeyboardInterrupt:
        logging.info("Script detenido manualmente.")
    except Exception as e:
        logging.critical(f"Error crítico: {e}")
