#!/usr/bin/env python3
import logging
import os
import random

from cassandra.cluster import Cluster

import model

# Set logger
log = logging.getLogger()
log.setLevel('INFO')
handler = logging.FileHandler('investments.log')
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
log.addHandler(handler)

# Read env vars related to Cassandra App
CLUSTER_IPS = os.getenv('CASSANDRA_CLUSTER_IPS', 'localhost')
KEYSPACE = os.getenv('CASSANDRA_KEYSPACE', 'investments')
REPLICATION_FACTOR = os.getenv('CASSANDRA_REPLICATION_FACTOR', '1')


def print_menu():
    mm_options = {
        0: "Populate data",
        1: "Show accounts",
        2: "Show positions",
        3: "Show trade history",
        4: "Change username",
        5: "Exit",
    }
    for key in mm_options.keys():
        print(key, '--', mm_options[key])


def print_trade_history_menu():
    thm_options = {
        1: "All Trades. (Optional date range. Defaults to latest 30 days)",
        2: "Trades by type (Buy or Sell). (Optional date range. Defaults to latest 30 days)",
        3: "Transaction by type (Buy or Sell) with instrument symbol. (Optional date range. Defaults to latest 30 days)",
        4: "Trades by symbol. (Optional date range. Defaults to latest 30 days)",
    }
    for key in thm_options.keys():
        print('    ', key, '--', thm_options[key])


def set_username():
    username = input('**** Username to use app: ')
    log.info(f"Username set to {username}")
    return username


def get_instrument_value(instrument):
    instr_mock_sum = sum(bytearray(instrument, encoding='utf-8'))
    return random.uniform(1.0, instr_mock_sum)

def main():
    log.info("Connecting to Cluster")
    cluster = Cluster(CLUSTER_IPS.split(','))
    session = cluster.connect()

    model.create_keyspace(session, KEYSPACE, REPLICATION_FACTOR)
    session.set_keyspace(KEYSPACE)

    model.create_schema(session)

    username = set_username()

    while(True):
        print_menu()
        option = int(input('Enter your choice: '))
        # Opcion 0
        if option == 0:
            model.bulk_insert(session)
        # Opcion 1
        if option == 1:
            accounts = model.get_user_accounts(session, username)
            print(f"\nAccounts for {username}:")
            for acc in accounts:
                print(f"- Account: {acc.account_number}") # prints para mostrar en pantalla
                print(f"  Cash Balance: {acc.cash_balance}")
        # Opcion 2
        if option == 2:
            accounts = model.get_user_accounts(session, username)
            print(f"\nAccounts for {username}:")
            for acc in accounts:
                print(f"\n=== Account: {acc.account_number} ===")
                print(f"- Cash Balance: {acc.cash_balance}")  # Ahora se imprime aquí
                positions = model.get_account_positions(session, acc.account_number)
                print(f"Positions for Account {acc.account_number}:")
                for pos in positions:
                    print(f"- {pos.symbol}: {pos.quantity}")
        # Opcion 3
        if option == 3:
            print_trade_history_menu()
            tv_option = int(input('Enter your trade view choice: '))
            account = input("Enter account number: ")
            
            # Solicitar parámetros según sub-opción
            start_date = None
            end_date = None
            date_range = input("Enter date range (YYYY-MM-DD to YYYY-MM-DD, or leave empty): ")
            if date_range:
                start_str, end_str = date_range.split(" to ")
                start_date = datetime.datetime.strptime(start_str, "%Y-%m-%d")
                end_date = datetime.datetime.strptime(end_str, "%Y-%m-%d") + datetime.timedelta(days=1)
            
            trade_type = None
            symbol = None
            
            if tv_option == 2:
                trade_type = input("Enter trade type (buy/sell): ").lower()
            elif tv_option == 3:
                trade_type = input("Enter trade type (buy/sell): ").lower()
                symbol = input("Enter symbol: ").upper()
            elif tv_option == 4:
                symbol = input("Enter symbol: ").upper()
            
            # Obtener trades
            trades = model.get_trades(
                session, 
                account, 
                tv_option,
                symbol=symbol,
                trade_type=trade_type,
                start_date=start_date,
                end_date=end_date
            )
            
            # Imprimir resultados
            print(f"\nTrades for Account {account}:")
            for trade in trades:
                print(f"ID: {trade.trade_id} | {trade.type.upper()} {trade.shares} shares of {trade.symbol} @ ${trade.price:.2f} (Total: ${trade.amount:.2f})")

        # Opcion 4
        if option == 4:
            new_user = set_username()
            print(f"\n¡Username cambiado a '{new_user}'!")
            print("Nota: Si este usuario no tiene datos asociados, usa la opción 0 para generarlos.\n")
            username = new_user  # Actualiza la variable global de usuario
            
        elif option == 5:
            print("Programa cerrado. ¡Hasta luego!")
            exit(0)

if __name__ == '__main__':
    main()