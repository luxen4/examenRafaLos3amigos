import csv
import json
import mysql.connector
from db_operationsMysql import Database
from db_neo4j_operations import Neo4jCRUD
from db_operationsMongoDB import MongoDBOperations
import pymongo


DB_HOST = "localhost"
DB_USER = "root"
DB_PASSWORD = "my-secret-pw"
DB_DATABASE = "cochesalquiler"
DB_PORT= "8889"


db = Database (DB_HOST, DB_USER, DB_PASSWORD, DB_DATABASE, DB_PORT)


def insercionMysql():
    create_table_queryCars = """
    CREATE TABLE IF NOT EXISTS Cars (
        id INT NOT NULL PRIMARY KEY,
        brand nVARCHAR (255),
        model nVARCHAR (255),
        year INT
    )
    """

    create_table_queryCustomers = """
    CREATE TABLE IF NOT EXISTS Customers (
        id INT NOT NULL PRIMARY KEY,
        name VARCHAR (255) not null,
        email varchar (255) not null
    )"""

    create_table_queryRentals = """
        CREATE TABLE IF NOT EXISTS Rentals (
            id INT NOT NULL PRIMARY KEY,
            car_id INT NOT null,
            customer_id INT NOT NULL,
            rental_date datetime,
            foreign key (car_id) references Cars(id) on update cascade on delete cascade,
            foreign key (customer_id) references Customers(id) on update cascade on delete cascade
            
    )"""    




    #db.create_table(create_table_queryCars)
    #db.create_table(create_table_queryCustomers)
    #db.create_table(create_table_queryRentals)




    # Paquete que inserta los registros de todas las tablas
    def read_csv_file(filename):
        data=[]
        with open(filename, 'r', encoding="utf-8") as file:
            reader = csv.reader(file)
            iterator = iter(reader)
            next(iterator)
            for row in reader:
                #print(row[0])
                data.append(row)
            return data

    for element in read_csv_file("MySQL/cars_mysql.csv"):
        insert_query = "INSERT INTO Cars(id, brand, model, year) VALUES (%s,%s,%s,%s)"
        data = (element[0], element[1], element[2], element[3])
        #db.insert_data(insert_query,data)
        

    for element in read_csv_file("MySQL/customers_mysql.csv"):
        insert_query = "INSERT INTO Customers (id, name, email) VALUES (%s,%s,%s)"
        data = (element[0], element[1], element[2])
        #db.insert_data(insert_query,data)
        
        
    for element in read_csv_file("MySQL/rentals_mysql.csv"):
        insert_query = "INSERT INTO Rentals ( id, car_id, customer_id, rental_date) VALUES (%s,%s,%s,%s)"
        data = (element[0], element[1], element[2], element[3])
        #db.insert_data(insert_query,data)
    ###########################################################################


def insercionMongoDB():

    # Conexión a MongoDB       ADRIÁN    
    DATABASE_MONGO="turismo"
    PORT_MONGO="8888"
    COLLECTION_MONGO="accommodations"

    client = pymongo.MongoClient("mongodb://localhost:8888/")  # Reemplaza <IP_del_contenedor>

    db = client["turismo"]
    collection = db["accommodations"]
    with open('./MongoDB/accommodations_mongo.json', 'r') as archivo_json:
        datos = json.load(archivo_json)
        
    for i in range(0, len(datos), 1):
        accommodation_id=   datos[i]['accommodation_id']
        destination_id=     datos[i]['destination_id']
        name=               datos[i]['name']
        city=               datos[i]['type']
        data_to_insert = { "accommodation_id": accommodation_id, "destination_id": destination_id, "name": name, "type": city}
        #collection.insert_one(data_to_insert)
        

    DATABASE_MONGO="turismo"
    PORT_MONGO="8888"
    COLLECTION_MONGO = 'bookings'
    client = pymongo.MongoClient("mongodb://localhost:" + PORT_MONGO + "/")  # Reemplaza <IP_del_contenedor>

    db = client[DATABASE_MONGO]
    collection = db[COLLECTION_MONGO]
    with open('./MongoDB/bookings_mongo.json', 'r') as archivo_json:
        datos = json.load(archivo_json)
        
    for i in range(0, len(datos), 1):
        booking_id = datos[i]['booking_id']
        customer_id = datos[i]['customer_id']
        accommodation_id = datos[i]['accommodation_id']
        check_in_date = datos[i]['check_in_date']
        data_to_insert = { "booking_id": booking_id, "customer_id": customer_id, "accommodation_id": accommodation_id, "check_in_date": check_in_date}
        #collection.insert_one(data_to_insert)
        
        
        
        
    
    DATABASE_MONGO="turismo"
    PORT_MONGO="8888"
    COLLECTION_MONGO = 'destinations'
    client = pymongo.MongoClient("mongodb://localhost:" + PORT_MONGO + "/")  # Reemplaza <IP_del_contenedor>

    db = client[DATABASE_MONGO]
    collection = db[COLLECTION_MONGO]
    with open('./MongoDB/destinations_mongo.json', 'r') as archivo_json:
        datos = json.load(archivo_json)
        
    for i in range(0, len(datos), 1):
        destination_id = datos[i]['destination_id']
        location = datos[i]['location']
        attractions = datos[i]['attractions']
        
        
        data_to_insert = { "destination_id": destination_id, "location": location, "atractions": attractions}
        collection.insert_one(data_to_insert)
        
        #"destination_id": "D1",
        #"location": "Ciudad A",
        #"attractions": [
            
            
        # FALTA 1
    ###############################

    # Conexión a Neo4j

# Conexión a neo4j          
uriNeo4j = "bolt://localhost:7687"  
userNeo4j = "neo4j"
passwordNeo4j = "alberite"
neo4j_crud = Neo4jCRUD(uriNeo4j, userNeo4j, passwordNeo4j)




def insercionNeo4j():


    print(" Inserción de Atracciones")
    # leer el csv para que empiece a meter
    filename = "./Neo4j/Atracciones.csv"
    with open(filename, 'r',  encoding="utf-8") as file:
        reader = csv.reader(file)
        for row in reader:
            node_properties = {"id":row[0], "atraccion_name": row[1], "category":row[2], "location": row[3] }
            #created_node = neo4j_crud.create_node("Atracciones", node_properties)
            #print(f"Created Node: {created_node}")


    print(" Inserción de Interacciones")
    # leer el csv para que empiece a meter
    filename = "./Neo4j/Interacciones.csv"
    with open(filename, 'r',  encoding="utf-8") as file:
        reader = csv.reader(file)
        for row in reader:
            node_properties = {"visitor_id":row[0], "attraction_id": row[1], "date":row[2], "time": row[3] }
            #created_node = neo4j_crud.create_node("Interacciones", node_properties)
            #print(f"Created Node: {created_node}")


    print(" Inserción de Visitantes")
    # leer el csv para que empiece a meter
    filename = "./Neo4j/Visitantes.csv"
    with open(filename, 'r',  encoding="utf-8") as file:
        reader = csv.reader(file)
        for row in reader:
            node_properties = {"id":row[0], "visitor_name": row[1], "age":row[2], "gender": row[3] }
            #created_node = neo4j_crud.create_node("Visitantes", node_properties)
            #print(f"Created Node: {created_node}")
            
            

    print(" Inserción de Relación interacciones")
    filename = "./Neo4j/Interacciones.csv"
    with open(filename, 'r') as file:                  
        reader = csv.reader(file)
        iterator = iter(reader)
        next(iterator)
        for row in iterator:
            node_properties = {"visitor_id":row[0], "attraction_id":row[1], "date":row[2], "time":row[3] }
            #neo4j_crud.create_relationshipAdrian3("Visitantes","Atracciones","Interacciones",node_properties) 
            # Consulta, personas que trabajen en varios sitios
            # Sitios donde trabajen varias personas



def main(): 
    while True:
        choice = int(input("Enter your choice: "))
        print("0. Carga de datos.")
        print("1. Obtener todas las atracciones visitadas por un visitante específico")
        print("2. Mostrar las 5 atracciones más visitadas.")
        print("3. Obtener todas las reservas para un cliente específico.")
        print("4. Obtener la información de todos los coches alquilados por un cliente específico, incluyendo detalles del coche y fecha de alquiler") 
        print("5. Mostrar todos los coches que fueron alquilados más de una vez:")
        print("6. --------Muestra los equipos con el número total de proyectos a los que están asociados.")
        print("7. Mostrar las atracciones más visitadas")
        print("8. EXit.")
        try:
            match choice:
                case 0:
                    #cargaDatos()
                    #insercionMysql()
                    insercionMongoDB()
                    #insercionNeo4j()
                    

                case 1: # OK 1. Obtener todas las atracciones visitadas por un visitante específico 
                    
                    query = "MATCH (v:Visitantes {visitor_name:'Fernanda'})-[i:Interacciones]-(a:Atracciones ) RETURN v.visitor_name, i.date, a.atraccion_name"
                    results = neo4j_crud.run_query(query)
                    print(results)
                    
                    for i in range (0 ,len(results), 1):
                        dic=results[i]
                        print(dic['v.visitor_name'] + " ha frecuentado en la fecha " + dic['i.date'], " la atracción '" + dic['a.atraccion_name'] + "'" )
                       
                case 2: # 2. Mostrar las 5 atracciones más visitadas.
                    
                    query = ( 
                    f" match (v:Visitantes)-[i:Interacciones]->(a:Atracciones) "
                    f" return *, count(i.person_id) as TotalVisitas, a.atraccion_name"
                    f" ORDER BY TotalVisitas asc "
                    f" limit 6 ")
                    
                    
                    query2 = ( 
                    f" MATCH (c:Companies)-[w2:Works_at]-(p:Persons),"
                    f"       (c1:Companies)-[w:Works_at]-(p2:Persons) "
                    f"       where w2.role = w.role and c.name <> c1.name and p.id = p2.id "
                    f"       return p.name, w.role, c.name" )
                        
                    results = neo4j_crud.run_query(query)
                    for i in range (0 ,len(results),1):
                        dic=results[i]
                        print( str(dic['TotalVisitas']) + " --- " + dic['a.atraccion_name']  )
 
                case 3: # OK 3. Obtener todas las reservas para un cliente específico." MONGODB
                    print()
                    
                    lookup={"from": 'accommodations', "localField": 'accommodation_id', "foreignField": 'accommodation_id', "as": 'info'}
  
                    pipeline2 = [  {"$match": {"customer_id": 10}},
                                   {"$lookup": lookup},
                                   {"$unwind":'$info'},
                                   {"$group": {"_id": "$info.name"}},
                                   {"$project": {"_id": 1 }} 
                                   ]            
                    
                    mongo_operations = MongoDBOperations('turismo', 'bookings', 8888)   # funciona OK run_aggregation
                    lista = mongo_operations.run_aggregation(pipeline2)
                    
                    # Mostrar los resultados
                    for resultado in lista:
                        print(resultado)

                case 4: # 4. Obtener la información de todos los coches alquilados por un cliente específico, incluyendo detalles del coche y fecha de alquiler   MYSQL
                    select_query="Select c.id, c.brand, c.model, c.year FROM Rentals r inner join Cars c on c.id=r.car_id inner join Customers cu on r.customer_id=cu.id where cu.name = 'Elena Lopez' "
                    obj = Database(DB_HOST,DB_USER,DB_PASSWORD,DB_DATABASE,DB_PORT)
                    results = obj.get_all_data(select_query)
                    for datos in results:
                        print("Marca: " + datos[1] + ", Modelo: "+ datos[2] + ", Año: " + str(datos[3]) )
                    
                case 5: # 5. Mostrar todos los coches que fueron alquilados más de una vez:
                    select_query="Select c.id, c.brand, c.model, c.year FROM Rentals r inner join Cars c on c.id=r.car_id group by r.car_id having count(car_id)>1 "
                    obj = Database(DB_HOST,DB_USER,DB_PASSWORD,DB_DATABASE,DB_PORT)
                    results = obj.get_all_data(select_query)
                    for datos in results:
                        print("Marca: " + datos[1] + ", Modelo: "+ datos[2] + ", Año: " + str(datos[3]))
                    
                case 6: # 6. Muestra los equipos con el número total de proyectos a los que están asociados.
                    print("Esta es de otro proyecto")
                        
                case 7: # 7. Mostrar las atracciones más visitadas
                    print("ES la misma que la 2")

                case 9:
                    print("")
                case 10:
                    print("Goodbye!")
                    break
                case _:
                    
                    print("Invalid choice. Please try again.")
        except ValueError:
            print("Invalid choice. Please try again.")
        
if __name__ == "__main__":
    main()


