import yaml
import psycopg2

# Load database credentials from YAML file
with open('rds_upload_db_creds.yaml', 'r') as file:
    creds = yaml.safe_load(file)

try:
    # Establish a database connection
    connection = psycopg2.connect(
        host=creds['RDS_HOST'],
        user=creds['RDS_USER'],
        password=creds['RDS_PASSWORD'],
        database=creds['RDS_DATABASE']
    )
    # Check if the connection was successful
    if connection:
        print (f"Connected to {creds['RDS_DATABASE']} database")
    
    # Task 1: Modify orders_table
    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT MAX(LENGTH(card_number)) AS max_card_number_length,
                   MAX(LENGTH(store_code)) AS max_store_code_length,
                   MAX(LENGTH(product_code)) AS max_product_code_length
            FROM orders_table
        """)
        max_lengths = cursor.fetchone()
        print(max_lengths)

    with connection.cursor() as cursor:
        # Execute ALTER TABLE commands
        cursor.execute("ALTER TABLE orders_table ALTER COLUMN date_uuid TYPE UUID USING date_uuid::UUID;")
        cursor.execute("ALTER TABLE orders_table ALTER COLUMN user_uuid TYPE UUID USING user_uuid::UUID;")
        cursor.execute("ALTER TABLE orders_table ALTER COLUMN card_number TYPE VARCHAR(%s);", (max_lengths[0],))
        cursor.execute("ALTER TABLE orders_table ALTER COLUMN store_code TYPE VARCHAR(%s);", (max_lengths[1],))
        cursor.execute("ALTER TABLE orders_table ALTER COLUMN product_code TYPE VARCHAR(%s);", (max_lengths[2],))
        cursor.execute("ALTER TABLE orders_table ALTER COLUMN product_quantity TYPE SMALLINT USING product_quantity::SMALLINT;")

        # Commit the transaction
        connection.commit()
        print("Task 1: Data types in orders_table altered successfully.")
    

except Exception as e:
    print(f"Error: {e}")

finally:
    # Close the connection
    if connection:
        connection.close()
        
        print("Connection closed")