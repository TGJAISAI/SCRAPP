class Query:
    urls = """SELECT id, name , platform_data from stores where deleted_at is null """



    insert_query = """INSERT INTO outlet_ratings (store_id, aggregator, rating, date, created_at, updated_at)
                  VALUES (%s, %s, %s, %s, %s, %s)"""
    



    res = """SELECT name  as name , lat , lng , city from restaurants
      where status = 1 and city != 'Hyderabad' and lat is not null and name not in ('Dil Central Facility','Central Kitchen');"""
    

