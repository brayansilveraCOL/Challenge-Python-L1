import sqlite3
from sqlite3 import Error


def create_database():
    try:
        sqlite3.connect('data_persistence/database.db')
    except Error:
        print( 'Error al conectarse o crear base de datos ' + Error)


def create_tables():
    try:

        con = sqlite3.connect('data_persistence/database.db')
        cursor_con = con.cursor()
        variable = cursor_con.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='metrics';")
        variable = len(variable.fetchall())
        if variable >= 1:
            return None
        else:
            cursor_con.execute(
                "CREATE TABLE metrics(id integer PRIMARY KEY, total_data_frame decimal, mean_data_frame decimal, max_data_frame decimal, min_data_frame decimal)")
            con.commit()
            con.close()
    except Error:
        print(Error)


def insert_data(data, connection):
    try:
        print(data)
        columns = ', '.join(data.keys())
        placeholders = ':' + ', :'.join(data.keys())
        query = 'INSERT INTO metrics (%s) VALUES (%s)' % (columns, placeholders)
        connection.execute(query, data)
        connection.commit()
        connection.close()
    except:
        print('Error en el Proceso de Insertar la Data')


def create_connection():
    try:
        connection = sqlite3.connect('data_persistence/database.db')
        return connection
    except Error:
        print(Error)
