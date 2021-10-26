# https://ssd-api.jpl.nasa.gov/doc/fireball.html

import sqlite3
import requests
import json


# creating table for saving data
def create_table(cursor):
    create_table = ''' CREATE TABLE fireballs (
                                id INTEGER PRIMARY KEY,
                                date TEXT NOT NULL,
                                energy TEXT,
                                impact_e TEXT,
                                lat TEXT,
                                lat_dir TEXT,
                                lon TEXT,
                                lon_dir TEXT,
                                alt TEXT,
                                vel TEXT);  '''

    cursor.execute(create_table)
    print("db was created_5")


# creating connection and cursor
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


# closing connection
def close_connection(connection, cursor):
    cursor.close()
    connection.close()


def getting_data_with_api():
    res = requests.get("https://ssd-api.jpl.nasa.gov/fireball.api")
    todos = json.loads(res.text)
    print(todos)

    with open('data.json', 'w') as f:
        json.dump(todos, f)


# reading data from json-file
def parsing_data():
    with open('data.json', 'r') as f:
        jData = json.load(f)
        print(jData)
        d = jData["fields"]
        print(d)
        print(type(d))


# full db
def writting_to_db(cur):
    lst = []

    connection.commit()
    with open('data.json', 'r') as f:
        data = json.load(f)

        for d in data["data"]:
            lst = d
            cur.execute(
                "INSERT INTO fireballs(date, energy, impact_e, lat, lat_dir, lon, lon_dir, alt, vel) VALUES(?,?,?,?,?,?,?,?,?);",
                lst)

        connection.commit()


if __name__ == '__main__':
    connection, cursor = connect()
    create_table(cursor)

    getting_data_with_api()

    # parsing_data()

    writting_to_db(cursor)

    close_connection(connection, cursor)
