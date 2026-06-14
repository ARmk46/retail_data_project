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
    print(f"❌ Failed to connect to the PostgreSQL database: {error}")
    exit(1)


def run_cdc_generator():
    """ Continuosly generates mock retail transaction data and inserts it into the PostgreSQL database. """
    try:
        while True:
            decipho = random.randint(1,10)

            if decipho <= 7:
                
                name = fake.name()
                email = fake.unique().email()


                #insert data into customers table
                cursor.execute("INSERT INTO customers (name, email) VALUES (%s, %s) RETURNING id",
                            (name, email) 
                        )
                
                print(f"✅ Inserted new customer: {name} with email: {email}")
                new_customer_id = cursor.fetchone()[0]

                order_amount = round(random.uniform(15.99, 350.00), 2)

                
                cursor.execute("INSERT INTO transactions (customer_id, amount, order_status) VALUES (%s, %s, %s) RETURNING id;",
                               (new_customer_id, order_amount,'pending',))
                  
                print(f"✅ Inserted new Order: {new_order_id}")
                
                new_order_id = cursor.fetchone()[0]


                event_payload = {
                    "ordr_id" : new_order_id,
                    "ordr_amt": order_amount,
                    "status" : 'Pending',
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
                
                if pending_order:
                    order_id = pending_order[0]
                    cursor.execute("UPDATE orders SET order_status = 'shipped' WHERE id = %s;", (order_id))
                    print(f"✅ Updated Order: {order_id} to completed")
                    connection.commit()
                
                else:
                    connection.rollback()
                    print("⚠️ No pending orders found to update. Rolling back transaction.")
                
            time.sleep(2)

    
    except KeyboardInterrupt:
        print("\n CDC generation stopped by user")

    except Exception as error:
        print(f"Erro during CDC generation:{error}")
    
    finally:
        if connection:
            cursor.close()
            connection.close()
            print("PostgreSQL connection closed.")


if __name__ == "__main__":
    run_cdc_generator()
