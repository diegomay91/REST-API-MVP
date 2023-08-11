import psycopg2

# Configura tus credenciales y detalles de la base de datos
db_config = {
    'host': 'localhost',
    'dbname': 'test_SQL',
    'user': 'postgres',
    'password': 'Password1234$',
    'port': '5432',  # Puerto por defecto de PostgreSQL
}

try:
    # Intenta establecer una conexión a la base de datos
    connection = psycopg2.connect(**db_config)

    # Crea un cursor para ejecutar consultas
    cursor = connection.cursor()

    # Ejecuta una consulta simple para verificar la conexión
    cursor.execute("SELECT version();")
    version = cursor.fetchone()

    print("Conexión exitosa a la base de datos.")
    print(cursor)
    print("Versión del servidor PostgreSQL:", version[0])
    # Ejecuta un SELECT * de una tabla
    cursor.execute("SELECT name, datetime FROM empleados.hired LIMIT 10")
    rows = cursor.fetchall()

    for row in rows:
        print(row)

except (Exception, psycopg2.Error) as error:
    print("Error al conectar a la base de datos:", error)

finally:
    # Cierra el cursor y la conexión, independientemente del resultado
    if connection:
        cursor.close()
        connection.close()
        print("Conexión cerrada.")