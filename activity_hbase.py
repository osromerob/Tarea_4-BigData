import happybase

# Conexión al servidor HBase
connection = happybase.Connection('localhost') 
table = connection.table('reviews')

# Función para guardar resultados en un archivo .txt
def write_to_file(filename, data):
    with open(filename, 'w', encoding='utf-8') as file:
        for line in data:
            file.write(line + '\n')

# 1. Consulta de selección y filtrado: obtener reseñas de 2022
def select_filtered_data():
    filtered_data = []
    for key, row in table.scan():
        date = row.get(b'cf:date', b'').decode()
        comments = row.get(b'cf:comments', b'').decode()
        if date.startswith('2022'):  # Filtrar reseñas de 2022
            filtered_data.append(f"ID: {key.decode()}, Date: {date}, Comment: {comments}")
    
    write_to_file('reviews_2022.txt', filtered_data)
    print(f"Reseñas de 2022 guardadas en 'reviews_2022.txt'")

# 2. Consulta de selección y filtrado: obtener reseñas de un usuario específico
def select_reviews_by_user(user_name):
    user_reviews = []
    for key, row in table.scan():
        reviewer_name = row.get(b'cf:reviewer_name', b'').decode()
        comments = row.get(b'cf:comments', b'').decode()
        if reviewer_name.lower() == user_name.lower():  # Filtrar reseñas por el nombre de usuario
            user_reviews.append(f"ID: {key.decode()}, Reviewer: {reviewer_name}, Comment: {comments}")
    
    write_to_file(f'reviews_{user_name}.txt', user_reviews)
    print(f"Reseñas de {user_name} guardadas en 'reviews_{user_name}.txt'")

# 3. Consulta de selección: listar reseñas de un alojamiento (listing_id)
def select_reviews_by_listing(listing_id):
    listing_reviews = []
    for key, row in table.scan():
        current_listing_id = row.get(b'cf:listing_id', b'').decode()
        comments = row.get(b'cf:comments', b'').decode()
        if current_listing_id == str(listing_id):  # Filtrar reseñas por listing_id
            listing_reviews.append(f"ID: {key.decode()}, Listing ID: {current_listing_id}, Comment: {comments}")
    
    write_to_file(f'reviews_listing_{listing_id}.txt', listing_reviews)
    print(f"Reseñas del alojamiento {listing_id} guardadas en 'reviews_listing_{listing_id}.txt'")

# 4. Operación de inserción de nueva reseña
def insert_data(listing_id, reviewer_id, reviewer_name, date, comments):
    row_key = str(listing_id) + "_" + str(reviewer_id)
    data = {
        b'cf:listing_id': str(listing_id).encode(),
        b'cf:id': str(reviewer_id).encode(),
        b'cf:date': date.encode(),
        b'cf:reviewer_name': reviewer_name.encode(),
        b'cf:comments': comments.encode(),
    }
    
    table.put(row_key.encode(), data)
    print(f"Data inserted for {row_key}")

# 5. Operación de actualización de comentarios
def update_data(listing_id, reviewer_id, new_comments):
    row_key = str(listing_id) + "_" + str(reviewer_id)
    updated_data = {
        b'cf:comments': new_comments.encode(),
    }
    
    table.put(row_key.encode(), updated_data)
    print(f"Data updated for {row_key}")

# 6. Operación de eliminación de reseña
def delete_data(listing_id, reviewer_id):
    row_key = str(listing_id) + "_" + str(reviewer_id)
    table.delete(row_key.encode())
    print(f"Data deleted for {row_key}")

# 7. Realizar las consultas de selección y filtrado

# Obtener reseñas de 2022
select_filtered_data()

# Obtener reseñas de un usuario específico, por ejemplo 'Kevin'
select_reviews_by_user('Kevin')

# Obtener reseñas de un alojamiento específico, por ejemplo con listing_id=18674
select_reviews_by_listing(18674)

# 8. Realizar operaciones de escritura (inserción, actualización, eliminación)

# Insertar nueva reseña
insert_data(18674, 99999999, "John Doe", "2024-11-18", "Fantastic location and great service!")

# Actualizar comentario existente
update_data(18674, 99999999, "Updated: The location is amazing, and the host was very friendly!")

# Eliminar reseña
delete_data(18674, 99999999)

# Cerrar la conexión
connection.close()
