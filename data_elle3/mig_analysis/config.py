import pyodbc
from configparser import ConfigParser
from turbodbc import connect

def get_params(type):
    parameters = {}
    parser = ConfigParser()
    parser.read("config.ini")
    if parser.has_section(type):
        parameters = {
            item[0] : item[1]
            for item in parser.items(type)
        }
    return parameters

def get_connection():
    connection_string = "Driver={};Server={};Port={};Database={};Uid={};Pwd={};".format("ODBC Driver 17 for SQL Server", get_params("mssql")["server"], get_params("mssql")["port"], get_params("mssql")["database"], get_params("mssql")["username"], get_params("mssql")["password"])
    connection = connect(connection_string=connection_string)
    #cursor = connection.cursor()
    #cursor.execute("Select * from [ELLE3].[dbo].[COMUNITA]")
    #table = cursor.fetchallarrow()   
    return connection

