import mysql.connector

class Database:
    def __init__(self, host, user, password, database, port):
        self.connection = mysql.connector.connect(
        host=host,
        user=user,
        password=password,
        database=database,
        port=port
        )
        self.cursor = self.connection.cursor()
        
    #Función para crear las tablas
    def create_table(self, create_table_query):
        self.cursor.execute(create_table_query)
        self.connection.commit()
        
    # Función para insertar en las tablas
    def insert_data(self, insert_query, data):
        self.cursor.execute(insert_query, data)
        self.connection.commit()
        
        
        
    def get_all_data(self, select_query):
        #select_all_query = "SELECT * FROM location"
        select_all_query = select_query
        self.cursor.execute(select_all_query)
        result = self.cursor.fetchall()
        return result