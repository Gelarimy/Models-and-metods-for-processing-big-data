#https://ssd-api.jpl.nasa.gov/doc/fireball.html

import sqlite3

def create_db(cursor):

    create_table = ''' CREATE TABLE fireballs_4 (
                                id INTEGER PRIMARY KEY,
                                data_time TEXT NOT NULL,
                                Latitude TEXT NOT NULL,
                                Longitude TEXT NOT NULL,
                                Altitude REAL NOT NULL,
                                Velocity_vx REAL NOT NULL,
                                Velocity_vy REAL NOT NULL,
                                Velocity_vz REAL NOT NULL,
                                Velocity_conponents TEXT NOT NULL,
                                Total_radiated_energy TEXT NOT NULL,
                                Calculated_total_impact_energy REAL NOT NULL);  '''

    cursor.execute(create_table)
    print("db was created_5")

def connect():
    try:
        sqllite_connection = sqlite3.connect('sqllite_python.db')
        cursor = sqllite_connection.cursor()
        print("База данных создана и успешно подключена к sqlite")

        sqllite_select_query = "select sqlite_version();"
        cursor.execute(sqllite_select_query)
        record = cursor.fetchall()
        print("Версия базы данных:", record)

    except sqlite3.Error as error:
        print("Ошибка подключния к бд", error)

    return sqllite_connection, cursor

def close_connection(connection, cursor):
    cursor.close()
    connection.close()

if __name__ == '__main__':
    connection, cursor = connect()
    create_db(cursor)
    close_connection(connection, cursor)