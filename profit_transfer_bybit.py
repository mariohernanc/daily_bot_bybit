import json
import time
import pandas as pd
import ccxt
import uuid
import mysql.connector
import schedule
import subprocess

def realizar_transferencias():
    # Leer el archivo 'user.json'
    with open('user.json', 'r') as file:
        lines = file.readlines()

    # Lista para almacenar los datos de los usuarios
    user_data = []

    # Procesar las líneas en bloques de usuario
    for i in range(0, len(lines), 7):
        user = lines[i].strip().split(': ')[1].strip('"')
        uid = lines[i + 1].strip().split(': ')[1].strip('"')
        coin = lines[i + 2].strip().split(': ')[1].strip('"')
        api_k = lines[i + 3].strip().split(': ')[1].strip('"')
        api_s = lines[i + 4].strip().split(': ')[1].strip('"')
        type = lines[i + 5].strip().split(': ')[1].strip('"')
        kdt = float(lines[i + 6].strip().split(': ')[1].strip('"'))

        # Agregar a la lista como un diccionario
        user_data.append({
            "user": user,
            "uid": uid,
            "coin": coin,
            "api_k": api_k,
            "api_s": api_s,
            "type": type,
            "KdT": kdt
        })

    # Conexión a la base de datos
    conexion = mysql.connector.connect(
        host='localhost',
        user='root',
        password='',
        database='profit_transfer'
    )

    cursor = conexion.cursor()

    # Iterar sobre cada usuario en user_data para obtener el saldo
    for data in user_data:
        user = data['user']
        uid = int(data['uid'])
        coin = data['coin']
        api_k_user = data['api_k']
        api_s_user = data['api_s']
        account_type = data['type']
        kdt = data['KdT']

        # Crear una instancia de Bybit en CCXT
        try:
            exchange = ccxt.bybit({
                "apiKey": api_k_user,
                "secret": api_s_user,
                "enableRateLimit": True,
            })

            # Obtener el saldo de la cuenta
            balance = exchange.fetch_balance()
            wallet_balance = round(balance["total"].get("USDT", 0), 4)
            transfer = round(max(wallet_balance - kdt, 0), 4)

            print(f"Usuario: {user}, Moneda: {coin}, Saldo en USDT: {wallet_balance}, Transferencia: {transfer}")

            if transfer > 0:
                # Generar un UUID para la transferencia
                transfer_id = str(uuid.uuid4())

                # Realizar la transferencia
                transfer_response = exchange.private_post_v5_asset_transfer_inter_transfer({
                    'transferId': transfer_id,
                    'coin': 'USDT',
                    'amount': str(round(transfer, 4)),
                    'fromAccountType': account_type,
                    'toAccountType': 'FUND',
                    'fromMemberId': uid,
                    'toMemberId': 35671204,
                })

                print(f"Transferencia realizada con éxito por: {transfer}")
            else:
                print(f"Saldo insuficiente para transferir. Saldo disponible: {wallet_balance} USDT")

            # Almacenar en la base de datos
            nuevo_registro = (user, uid, coin, transfer, kdt)
            consulta = 'INSERT INTO profits (user, uid, coin, profit, KdT) VALUES (%s, %s, %s, %s, %s)'
            cursor.execute(consulta, nuevo_registro)
            conexion.commit()
            print('Registro guardado')
            print()

        except Exception as e:
            print(f"Exception: {e}")
            print("Restarting bot...")
            subprocess.run(["python3", "profit_transfer.py"], cwd="/home/mac/profit_transfer")
            return  # Salir de la función después de reiniciar

    # Cerrar la conexión a la base de datos
    conexion.close()

# Programar la tarea para que se ejecute todos los días a una hora establecida
schedule.every().day.at("12:00").do(realizar_transferencias)

print("Programación establecida. El script se ejecutará todos los días a las 12:00 UCT - 07:00 COL.")

# Mantener el script en ejecución
while True:
    schedule.run_pending()
    time.sleep(30)  # Espera 30 segundos antes de verificar nuevamente
