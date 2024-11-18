import happybase
import csv

connection = happybase.Connection('localhost')
table = connection.table('reviews')

with open('reviews.csv', 'r', encoding='utf-8') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        rowkey = f"{row['listing_id']}_{row['id']}"
        table.put(rowkey, {
            b'cf:listing_id': row['listing_id'].encode(),
            b'cf:date': row['date'].encode(),
            b'cf:reviewer_id': row['reviewer_id'].encode(),
            b'cf:reviewer_name': row['reviewer_name'].encode(),
            b'cf:comments': row['comments'].encode(),
        })

connection.close()
