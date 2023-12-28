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


    # Task 2: Modify dim_users table

    with connection.cursor() as cursor:
        # Execute ALTER TABLE commands
        alter_commands = [
            "ALTER TABLE dim_users ALTER COLUMN first_name TYPE VARCHAR(255);",
            "ALTER TABLE dim_users ALTER COLUMN last_name TYPE VARCHAR(255);",
            "ALTER TABLE dim_users ALTER COLUMN date_of_birth TYPE DATE USING date_of_birth::DATE;",
            "ALTER TABLE dim_users ALTER COLUMN country_code TYPE VARCHAR(2);",
            "ALTER TABLE dim_users ALTER COLUMN user_uuid TYPE UUID USING user_uuid::UUID;",
            "ALTER TABLE dim_users ALTER COLUMN join_date TYPE DATE USING join_date::DATE;"
        ]

        for command in alter_commands:
            cursor.execute(command)

        # Commit the transaction
        connection.commit()
        print("Task 2: Data types in dim_users altered successfully.")


    # Task 3: Modify dim_store_details table
        
    with connection.cursor() as cursor:
        # Merge latitude columns and drop 'lat' column
        #
        cursor.execute("""
            UPDATE dim_store_details
            SET latitude = COALESCE(latitude, lat);
            ALTER TABLE dim_store_details DROP COLUMN lat;
        """)

        # Alter column data types
        cursor.execute("""
            ALTER TABLE dim_store_details
            ALTER COLUMN longitude TYPE FLOAT USING longitude::FLOAT,
            ALTER COLUMN locality TYPE VARCHAR(255),
            ALTER COLUMN store_code TYPE VARCHAR(255),
            ALTER COLUMN staff_numbers TYPE SMALLINT USING staff_numbers::SMALLINT,
            ALTER COLUMN opening_date TYPE DATE USING opening_date::DATE,
            ALTER COLUMN store_type TYPE VARCHAR(255),
            ALTER COLUMN latitude TYPE FLOAT USING latitude::FLOAT,
            ALTER COLUMN country_code TYPE VARCHAR(255),
            ALTER COLUMN continent TYPE VARCHAR(255);
        """)

    # Handle NULL Values
    with connection.cursor() as cursor:
        # Update string columns
        text_columns_to_update = ['locality', 'store_code', 'store_type', 'country_code', 'continent']
        for column in text_columns_to_update:
            cursor.execute(f"""
                UPDATE dim_store_details
                SET {column} = COALESCE({column}, 'N/A')
                WHERE {column} IS NULL;
            """)
        
        # Update numeric columns with a default value
        cursor.execute("""
            UPDATE dim_store_details
            SET longitude = COALESCE(longitude, 0.0),
                latitude = COALESCE(latitude, 0.0)
            WHERE longitude IS NULL OR latitude IS NULL;
        """)    

        # Commit the transaction
        connection.commit()
        print("Task 3: Operations on dim_store_details completed successfully.")



    # Task 4: Modify dim_products table
    with connection.cursor() as cursor:

        # Step 2: Add the weight_class column
        cursor.execute("""
            ALTER TABLE dim_products
            ADD COLUMN weight_class VARCHAR(255);
        """)

        # Populate the weight_class column
        cursor.execute("""
            UPDATE dim_products
            SET weight_class = CASE
                WHEN weight < 2 THEN 'Light'
                WHEN weight >= 2 AND weight < 40 THEN 'Mid_Sized'
                WHEN weight >= 40 AND weight < 140 THEN 'Heavy'
                WHEN weight >= 140 THEN 'Truck_Required'
                ELSE 'Unknown'
            END;
        """)

        connection.commit()
        print("Task 4: dim_products table updated successfully.")
    

except Exception as e:
    print(f"Error: {e}")

finally:
    # Close the connection
    if connection:
        connection.close()
        
        print("Connection closed")