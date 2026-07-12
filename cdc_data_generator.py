from faker import Faker
import random
import psycopg2
import time
import json



fake = Faker()  # Initialize Faker for generating realistic data

try:
    # Connect to the PostgreSQL database
    connection = psycopg2.connect(
        host="localhost",
        port=5432,
        database="retail_db",
        user="admin",
        password="admin"
    )
    cursor = connection.cursor()
    connection.autocommit = False
    cursor = connection.cursor()
    print("Successfully connected to Postgres retail_db! Starting generator...")
except Exception as error:
    print(f" Failed to connect to the PostgreSQL database: {error}")
    exit(1)


def run_cdc_generator():
    """ Continuosly generates mock retail transaction data and inserts it into the PostgreSQL database. """
    try:
        while True:
            decipho = random.randint(1,10)

            if decipho <= 7:
                
                name = fake.name()
                email = fake.unique.email()


                #insert data into customers table
                cursor.execute("""
                                        INSERT INTO customers (name, email) 
                                        VALUES (%s, %s) 
                                        ON CONFLICT (email) 
                                        DO UPDATE SET name = EXCLUDED.name 
                                        RETURNING id
                                    """, (name, email))
                
                
                print(f"✅ Inserted new customer: {name} with email: {email}")
                new_customer_id = cursor.fetchone()[0]

                order_amount = round(random.uniform(15.99, 350.00), 2)

                
                cursor.execute("INSERT INTO orders (customer_id, amount, order_status) VALUES (%s, %s, %s) RETURNING id;",
                               (new_customer_id, order_amount,'pending',))

                new_order_id = cursor.fetchone()[0]  
                print(f"✅ Inserted new Order: {new_order_id}")
                
                


                event_payload = {
                    "ordr_id" : new_order_id,
                    "ordr_amt": float(order_amount),
                    "status" : 'pending',
                    "customer": {
                        "cust_id" : new_customer_id,
                        "cust_name" : name,
                        "cust_email" : email }
                }

                json_payload = json.dumps(event_payload)

                cursor.execute("INSERT INTO outbox (aggregate_type, aggregate_id, event_type, payload) VALUES(%s,%s,%s,%s)", 
                               ('RetailOrder',new_order_id,'OrderCreated', json_payload))


                connection.commit()


            else:

                cursor.execute("SELECT id FROM orders WHERE order_status = 'pending' ORDER BY random() LIMIT 1;")  
                pending_order = cursor.fetchone()
                
                cursor.execute("SELECT id, amount, customer_id FROM orders WHERE order_status = 'pending' ORDER BY random() LIMIT 1;")  
                pending_order = cursor.fetchone()
                
                if pending_order:
                    order_id, order_amount, customer_id = pending_order
                    
                    # 1. Update core table
                    cursor.execute("UPDATE orders SET order_status = 'shipped' WHERE id = %s;", (order_id,))
                    print(f"✅ Updated Order: {order_id} to shipped")
                    
                    # 2. Build the Outbox payload for the state change
                    update_payload = {
                        "ordr_id" : order_id,
                        "ordr_amt": float(order_amount),
                        "status" : 'shipped',
                        "customer": {
                            "cust_id" : customer_id
                        }
                    }
                    
                    # 3. Write event to outbox so Debezium can stream it
                    cursor.execute(
                        "INSERT INTO outbox (aggregate_type, aggregate_id, event_type, payload) VALUES(%s,%s,%s,%s)", 
                        ('RetailOrder', order_id, 'OrderShipped', json.dumps(update_payload))
                    )
                    
                    connection.commit()
                
                else:
                    connection.rollback()
                    print("⚠️ No pending orders found to update. Rolling back transaction.")
                
            time.sleep(2)

    
    except KeyboardInterrupt:
        print("\n CDC generation stopped by user")

    except Exception as error:
        print(f"Error during CDC generation:{error}")
    
    finally:
        if connection:
            cursor.close()
            connection.close()
            print("PostgreSQL connection closed.")


if __name__ == "__main__":
    run_cdc_generator()
