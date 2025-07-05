import json
import time
import logging
import mysql.connector
import ccxt
import uuid
import schedule

from datetime import datetime

# Configuración del logging para mostrar en consola Y escribir en archivo
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("/var/log/profit_transfer/out.log"),
        logging.StreamHandler()  # Esto hace que también se muestre en la consola
    ]
)

def cargar_usuarios():
    try:
        with open('user.json', 'r') as f:
            users = json.load(f)
        return [u for u in users if u.get("activo", False)]
    except Exception as e:
        logging.error(f"❌ Error al cargar usuarios: {e}")
        raise

def conectar_db():
    try:
        conn = mysql.connector.connect(
            host='localhost',
            user='root',
            password='Marinita1953***sql',
            database='profit_transfer'
        )
        return conn
    except mysql.connector.Error as err:
        logging.error(f"❌ Error conectando a la base de datos: {err}")
        raise

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
            logging.info(f"⚠️ Saldo insuficiente para {usuario['user']}. Saldo: {wallet_balance} USDT")
        else:
            # Generar UUID
            transfer_id = str(uuid.uuid4())

            # Realizar la transferencia
            transfer_response = exchange.private_post_v5_asset_transfer_inter_transfer({
                'transferId': transfer_id,
                'coin': 'USDT',
                'amount': str(round(transfer, 4)),
                'fromAccountType': usuario["type"],
                'toAccountType': 'FUND',
                'fromMemberId': usuario["UID"],
                'toMemberId': 35671204,
            })

            logging.info(f"✅ Transferencia realizada por {transfer} USDT para {usuario['user']}")

        # Guardar siempre, incluso si no hubo transferencia
        guardar_registro(usuario, transfer)

    except Exception as e:
        logging.error(f"❌ Error al procesar {usuario['user']}: {e}")

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
        logging.info("📌 Registro guardado en la base de datos.")
    except Exception as e:
        logging.error(f"❌ Error al guardar en la base de datos: {e}")
    finally:
        if 'conn' in locals() and conn.is_connected():
            conn.close()

def main():
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logging.info(f"🚀 Inicio de ejecución programada ({now})")

    try:
        usuarios = cargar_usuarios()
        if not usuarios:
            logging.warning("⚠️ No hay usuarios activos para procesar.")
            return

        for usuario in usuarios:
            logging.info(f"➡️ Procesando usuario: {usuario['user']}")
            realizar_transferencia(usuario)

        logging.info(f"🏁 Fin de ejecución programada ({datetime.now().strftime('%Y-%m-%d %H:%M:%S')})")

    except Exception as e:
        logging.critical(f"🔥 ERROR GRAVE EN LA EJECUCIÓN PROGRAMADA: {e}")

# Programar la tarea
schedule.every().day.at("21:00").do(main)  # Ajusta la hora según tu zona horaria

if __name__ == "__main__":
    logging.info("🕒 Script iniciado. Esperando próxima ejecución programada...")
    while True:
        schedule.run_pending()
        time.sleep(30)
