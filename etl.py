import json
import psycopg2

def load_json(file_path):
    with open(file_path, 'r') as file:
        return json.load(file)

def normalize_data(order_data):
    orders = []
    bundles = []
    baskets = []
    items = []
    users = []
    payments = []
    
    for order in order_data:
        order_uuid = order.get("uuid", None)
        created_at = order.get("createdAt", None)
        updated_at = order.get("updatedAt", None)
        ext_store_uuid = order.get("extStoreUUID", None)
        ext_tenant_uuid = order.get("extTenantUUID", None)
        channel = order.get("requestedFrom", None)
        status = order.get("status", None)
        takeaway = order.get("takeaway", None)
        timezone = order.get("timezone", None)
        
        if order_uuid:
            orders.append((order_uuid, created_at, updated_at, ext_store_uuid, ext_tenant_uuid, channel, status, takeaway, timezone))
        
        user_info = order.get("user", {})
        user_uuid = user_info.get("extUserUUID", None)
        
        if user_uuid and order_uuid:
            users.append((user_uuid, order_uuid))
        
        payment_info = order.get("payment", {})
        total_amount = payment_info.get("totalAmount", 0)
        vat_amount = payment_info.get("vatAmount", 0)
        delivery_fee = payment_info.get("deliveryFee", 0)
        discount = payment_info.get("discount", 0)
        price = payment_info.get("price", 0)
        service_charge = payment_info.get("serviceCharge", 0)
        subtotal_amount = payment_info.get("subtotalAmount", 0)
        
        if order_uuid:
            payments.append((order_uuid, total_amount, vat_amount, delivery_fee, discount, price, service_charge, subtotal_amount))
        
        for bundle in order.get("bundles", []):
            bundle_uuid = bundle.get("uuid", None)
            category_name = bundle.get("category", {}).get("name", "Unknown")
            basket_id = bundle.get("basketUUID", None)
            name = bundle.get("name", None)
            
            if basket_id and order_uuid:
                baskets.append((bundle_uuid, order_uuid, basket_id, name, category_name))

            if bundle_uuid and order_uuid:
                bundles.append((bundle_uuid, order_uuid, name))
            
            for item in bundle.get("itemTypes", {}).get("items", []):
                item_uuid = item.get("itemUUID", None)
                item_name = item.get("name", "Unknown")
                total_amount = item.get("totalAmount", 0)
                vat_amount = item.get("vatAmount", 0)
                if item_uuid and bundle_uuid:
                    items.append((item_uuid, bundle_uuid, basket_id, item_name, total_amount, vat_amount))
    
    return orders, users, payments, baskets, items, bundles

def create_tables(cursor):
    cursor.execute('''        
        CREATE TABLE IF NOT EXISTS orders (
            order_id UUID PRIMARY KEY,
            created_at BIGINT,
            updated_at BIGINT,
            ext_store_uuid UUID,
            tenant_id UUID,
            channel VARCHAR(100), 
            status VARCHAR(100), 
            takeaway VARCHAR(10), 
            timezone VARCHAR(100)
        );
        
        CREATE TABLE IF NOT EXISTS users (
            user_id UUID,
            order_id UUID REFERENCES orders(order_id),
            PRIMARY KEY (user_id, order_id)
        );
        
        CREATE TABLE IF NOT EXISTS payments (
            order_id UUID REFERENCES orders(order_id),
            total_amount DECIMAL,
            vat_amount DECIMAL,
            delivery_fee DECIMAL, 
            discount DECIMAL,
            price DECIMAL, 
            service_charge DECIMAL, 
            subtotal_amount DECIMAL
        );
        
        CREATE TABLE IF NOT EXISTS bundles (
            bundle_id UUID PRIMARY KEY,
            order_id UUID REFERENCES orders(order_id),
            name VARCHAR(200)
        );
                   
        CREATE TABLE IF NOT EXISTS baskets (
            basket_id VARCHAR(200) PRIMARY KEY,
            order_id UUID REFERENCES orders(order_id),
            bundle_id UUID REFERENCES bundles(bundle_id),
            name VARCHAR(200),
            category_name VARCHAR(100)
        );
        
        CREATE TABLE IF NOT EXISTS items (
            item_id UUID,
            bundle_id UUID REFERENCES bundles(bundle_id),
            basket_id VARCHAR(200) REFERENCES baskets(basket_id),
            name VARCHAR(100),
            total_amount DECIMAL,
            vat_amount DECIMAL,
            PRIMARY KEY (item_id, bundle_id)
        );
    ''')

def insert_data(cursor, orders, users, payments, baskets, items, bundles):
    cursor.executemany('INSERT INTO orders (order_id, created_at, updated_at, ext_store_uuid, tenant_id, channel, status, takeaway, timezone) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING', orders)
    cursor.executemany('INSERT INTO users (user_id, order_id) VALUES (%s, %s) ON CONFLICT DO NOTHING', users)
    cursor.executemany('INSERT INTO payments (order_id, total_amount, vat_amount, delivery_fee, discount, price, service_charge, subtotal_amount) VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING', payments)
    cursor.executemany('INSERT INTO bundles (bundle_id, order_id, name) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING', bundles)
    cursor.executemany('INSERT INTO baskets (bundle_id, order_id, basket_id, name, category_name) VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING', baskets)
    cursor.executemany('INSERT INTO items (item_id, bundle_id, basket_id, name, total_amount, vat_amount) VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING', items)

def connect_db():
    conn = psycopg2.connect(
        host="localhost",
        user="postgres",
        password="1234",
        port=5432
    )
    return conn

if __name__ == "__main__":
    data = load_json('./data/task_data.json')
    
    orders, users, payments, baskets, items, bundles= normalize_data(data)
    
    conn = connect_db()
    cur = conn.cursor()

    create_tables(cur)
    insert_data(cur, orders, users, payments, baskets, items, bundles)
    
    conn.commit()
    cur.close()
    conn.close()
