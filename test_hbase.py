import happybase

try:
# Intentar establecer conexión
	connection = happybase.Connection('localhost')
# Listar las tablas existentes
	print("Tablas existentes:", connection.tables())
	print("Conexión exitosa a HBase")
	connection.close()
except Exception as e:
	print("Error al conectar:", str(e))
