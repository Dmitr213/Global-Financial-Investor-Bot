import psycopg2
from config import host, user, password, port, db_name, sslmode

DATABASE_URL = "postgres://" + user + ":" + password + "@" + host + ":" + port + "/" + db_name


def tables_create():
    connection = None

    try:
        connection = psycopg2.connect(DATABASE_URL, sslmode=sslmode)
        connection.autocommit = True

        # # deleting an invalid table for re-creation
        # with connection.cursor() as cursor:
        #     cursor.execute(
        #         """DROP TABLE users;"""
        #     )
        #
        #     print("[INFO] Table was deleted successfully")

        # create a new table users
        with connection.cursor() as cursor:
            cursor.execute(
                """CREATE TABLE users(
                id serial PRIMARY KEY,
                telegram_id bigint UNIQUE NOT NULL,
                user_name varchar(32),
                first_name varchar(64),
                last_name varchar(64),
                email varchar(254),
                telephone varchar(16),
                balance NUMERIC(12,2) DEFAULT 0,
                investment_income NUMERIC(15,2) DEFAULT 0,
                ref_balance NUMERIC(12,2) DEFAULT 0,
                ref_count int DEFAULT 0,
                turnover bigint DEFAULT 0,
                wallet_number varchar(64),
                invited_by bigint,
                language varchar(2));"""
            )
        print("[INFO] Table users created successfully")

        # create a new table investment
        with connection.cursor() as cursor:
            cursor.execute(
                """CREATE TABLE investment(
                id serial PRIMARY KEY,
                telegram_id bigint NOT NULL,
                tariff int NOT NULL,
                investment_amount int NOT NULL,
                investment_start_date int NOT NULL,
                investment_end_date int NOT NULL,
                deposit_is_active bool NOT NULL);"""
            )

            print("[INFO] Table investment created successfully")

        # create a new table money_withdrawal
        with connection.cursor() as cursor:
            cursor.execute(
                """CREATE TABLE money_withdrawal(
                id serial PRIMARY KEY,
                telegram_id bigint NOT NULL,
                withdrawal_amount int NOT NULL,
                request_date int NOT NULL,
                request_status varchar(32) NOT NULL);"""
            )

            print("[INFO] Table money_withdrawal created successfully")

        # create a new table balance_replenishment
        with connection.cursor() as cursor:
            cursor.execute(
                """CREATE TABLE balance_replenishment(
                id serial PRIMARY KEY,
                telegram_id bigint NOT NULL,
                user_name varchar(32),
                replenishment_amount int NOT NULL,
                transaction_hash varchar(64),
                request_date int NOT NULL,
                request_status varchar(32) NOT NULL);"""
            )

            print("[INFO] Table balance_replenishment created successfully")

        # with connection.cursor() as cursor:
        #     cursor.execute(
        #         """ALTER TABLE users DROP COLUMN language;"""
        #     )
        #
        # print("[INFO] Table column drop successfully")
        #
        # with connection.cursor() as cursor:
        #     cursor.execute(
        #         """ALTER TABLE users ADD COLUMN language varchar(2);"""
        #     )
        #
        # print("[INFO] Table column add successfully")

    except Exception as _ex:
        print("[INFO] Error wile with PostgreSQL", _ex)
    finally:
        if connection:
            connection.close()
            print("[INFO] PostgreSQL connection closed")


if __name__ == '__main__':
    tables_create()
