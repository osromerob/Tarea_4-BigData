import happybase

# Conexión al servidor HBase
connection = happybase.Connection('localhost')  # Cambia 'localhost' por la IP de tu servidor HBase
table = connection.table('reviews')

def write_to_file(filename, data):
    """Función para escribir datos a un archivo .txt."""
    with open(filename, 'w', encoding='utf-8') as file:
        for line in data:
            file.write(line + '\n')

# 1. Obtener todas las reseñas realizadas en el año 2019
data_2019 = []
for key, row in table.scan():
    date = row.get(b'cf:date', b'').decode()
    comments = row.get(b'cf:comments', b'').decode()
    if date.startswith('2019'):
        data_2019.append(f"ID: {key.decode()}, Date: {date}, Comment: {comments}")

write_to_file('reviews_2019.txt', data_2019)

# 2. Contar el número de reseñas por cada listing_id
listing_count = {}
for key, row in table.scan():
    listing_id = row.get(b'cf:listing_id', b'').decode()
    if listing_id in listing_count:
        listing_count[listing_id] += 1
    else:
        listing_count[listing_id] = 1

count_data = [f"Listing ID: {listing_id}, Count: {count}" for listing_id, count in listing_count.items()]
write_to_file('listing_count.txt', count_data)

# 4. Listar los 5 alojamientos (listing_id) con más reseñas
top_listings = sorted(listing_count.items(), key=lambda x: x[1], reverse=True)[:5]
top_listings_data = [f"Listing ID: {listing_id}, Count: {count}" for listing_id, count in top_listings]
write_to_file('top_listings.txt', top_listings_data)

# 5. Realizar un análisis simple de sentimiento (positiva/negativa) en los comentarios usando palabras clave
keywords_positive = ["great", "good", "amazing", "recommend", "nice", "excellent"]
keywords_negative = ["bad", "poor", "disappointing", "dirty", "terrible", "not recommend"]

sentiment_analysis = []
for key, row in table.scan():
    comments = row.get(b'cf:comments', b'').decode().lower()
    if any(word in comments for word in keywords_positive):
        sentiment_analysis.append(f"ID: {key.decode()}, Sentiment: Positive, Comment: {comments}")
    elif any(word in comments for word in keywords_negative):
        sentiment_analysis.append(f"ID: {key.decode()}, Sentiment: Negative, Comment: {comments}")
    else:
        sentiment_analysis.append(f"ID: {key.decode()}, Sentiment: Neutral, Comment: {comments}")

write_to_file('sentiment_analysis.txt', sentiment_analysis)

# Cerrar la conexión
connection.close()

print("Consultas completadas y resultados guardados en archivos .txt.")
