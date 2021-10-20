# This is a sample Python script.
from functions.funtions import send_request_get, make_etl, create_data_frame, data_analytic, convert_data_frame_json
from functions.database_conection import create_database, create_tables, insert_data, create_connection


# Press Mayús+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.


def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.


# Press the green button in the gutter to run the script.
if __name__ == '__main__':

    response = send_request_get()
    etl_response = make_etl(response)
    data_frame_response = create_data_frame(etl_response)
    convert_data_frame_json(data_frame_response)
    analytic_response = data_analytic(data_frame_response)
    print(' Created database')
    create_database()
    print('Created table metrics')
    create_tables()
    print('Created connection')
    connection = create_connection()
    print('insert data')
    insert_data(analytic_response, connection)



# See PyCharm help at https://www.jetbrains.com/help/pycharm/
