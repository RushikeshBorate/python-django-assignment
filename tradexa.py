import sqlite3
import threading

# Database setup
def setup_databases():
    users_conn = sqlite3.connect('users.db')
    orders_conn = sqlite3.connect('orders.db')
    products_conn = sqlite3.connect('products.db')

    users_conn.execute('CREATE TABLE IF NOT EXISTS Users (id INTEGER, name TEXT, email TEXT)')
    orders_conn.execute('CREATE TABLE IF NOT EXISTS Orders (id INTEGER, user_id INTEGER, product_id INTEGER, quantity INTEGER)')
    products_conn.execute('CREATE TABLE IF NOT EXISTS Products (id INTEGER, name TEXT, price REAL)')

    users_conn.close()
    orders_conn.close()
    products_conn.close()

# Validation and insertion 
def validate_and_insert(db_name, table, data, validator):
    valid_data = []
    invalid_data = []
    
    for record in data:
        if validator(record):
            valid_data.append(record)
        else:
            invalid_data.append(record)

    try:
        conn = sqlite3.connect(db_name)
        cursor = conn.cursor()
        if valid_data:
            placeholders = ", ".join(["?"] * len(valid_data[0]))
            cursor.executemany(f"INSERT INTO {table} VALUES ({placeholders})", valid_data)
            conn.commit()
        print(f"Inserted into {table}: {len(valid_data)} records.")
        if invalid_data:
            print(f"Error/lack of info in records for {table}: {invalid_data}")
    except Exception as e:
        print(f"Error inserting into {table}: {e}")
    finally:
        conn.close()

# Validators
def validate_users(record):
    return len(record) == 3 and record[0] > 0 and record[1] and "@" in record[2]

def validate_products(record):
    return len(record) == 3 and record[0] > 0 and record[1] and record[2] > 0

def validate_orders(record):
    return len(record) == 4 and record[0] > 0 and record[1] > 0 and record[2] > 0 and record[3] > 0

# M-Threaded 
def threaded_insertion():
    users_data = [
                    (1, 'Alice', 'alice@example.com'),
                    (2, 'Bob', 'bob@example.com'),
                    (3, 'Charlie', 'charlie@example.com'),
                    (4, 'David', 'david@example.com'),
                    (5, 'Eve', 'eve@example.com'),
                    (6, 'Frank', 'frank@example.com'),
                    (7, 'Grace', 'grace@example.com'),
                    (8, 'Alice', 'alice@example.com'),
                    (9, 'Henry', 'henry@example.com'),
                    (10, '', 'jane@example.com')
                 ]
    products_data = [
                        (1, 'Laptop', 1000.00),
                        (2, 'Smartphone', 700.00),
                        (3, 'Headphones', 150.00),
                        (4, 'Monitor', 300.00),
                        (5, 'Keyboard', 50.00),
                        (6, 'Mouse', 30.00),
                        (7, 'Laptop', 1000.00),
                        (8, 'Smartwatch', 250.00),
                        (9, 'Gaming Chair', 500.00),
                        (10, 'Earbuds', -50.00)
                    ]
    orders_data = [
                        (1, 1, 1, 2),
                        (2, 2, 2, 1),
                        (3, 3, 3, 5),
                        (4, 4, 4, 1),
                        (5, 5, 5, 3),
                        (6, 6, 6, 4),
                        (7, 7, 7, 2),
                        (8, 8, 8, 0),
                        (9, 9, 1, -1),
                        (10, 10, 10, 2)
                  ]

    threads = [
        threading.Thread(target=validate_and_insert, args=('users.db', 'Users', users_data, validate_users)),
        threading.Thread(target=validate_and_insert, args=('products.db', 'Products', products_data, validate_products)),
        threading.Thread(target=validate_and_insert, args=('orders.db', 'Orders', orders_data, validate_orders))
    ]

    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

# execution
if __name__ == "__main__":
    setup_databases()
    threaded_insertion()
