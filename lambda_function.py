import json
import psycopg2
from decimal import Decimal


db_host = 'serverless-backend.cluster-cdtrga72dt1a.ap-south-1.rds.amazonaws.com'
db_port = '5432'
db_name = 'auroratestdb'
db_user = 'shiva'
db_password = 'who_1281'


table_name = 'product_inventory'
health_path = '/health'
product_path = '/product'
products_path = '/products'

def lambda_handler(event, context):
    print('Request event:', event)
    response = None
    http_method = event['httpMethod']
    path = event['path']

    try:
        if http_method == 'GET' and path == health_path:
            response = build_response(200)
        elif http_method == 'GET' and path == product_path:
            product_id = event['queryStringParameters']['productId']
            response = get_product(product_id)
        elif http_method == 'GET' and path == products_path:
            response = get_products()
        elif http_method == 'POST' and path == product_path:
            product_data = json.loads(event['body'])
            response = save_product(product_data)
        elif http_method == 'PATCH' and path == product_path:
            request_body = json.loads(event['body'])
            response = modify_product(request_body['productId'], request_body['updateKey'], request_body['updateValue'])
        elif http_method == 'DELETE' and path == product_path:
            product_id = json.loads(event['body'])['productId']
            response = delete_product(product_id)
        else:
            response = build_response(404, '404 Not Found')
    except Exception as e:
        print(f"Error processing request: {e}")
        response = build_response(500, 'Internal Server Error')

    return response

def get_product(product_id):
    try:
        conn = psycopg2.connect(
            host=db_host,
            port=db_port,
            database=db_name,
            user=db_user,
            password=db_password
        )
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM product_inventory WHERE productId = %s", (product_id,))
        row = cursor.fetchone()
        if row:
            item_dict = {
                'productId': row[0],
                'name': row[1],
                'price': row[2]
            }
            return build_response(200, item_dict)
        else:
            return build_response(404, 'Product not found')
    except Exception as e:
        print(f"Error retrieving product: {e}")
        return build_response(500, 'Internal Server Error')
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def get_products():
    try:
        conn = psycopg2.connect(
            host=db_host,
            port=db_port,
            database=db_name,
            user=db_user,
            password=db_password
        )
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM product_inventory")
        rows = cursor.fetchall()
        product_list = []
        for row in rows:
            item_dict = {
                'productId': row[0],
                'name': row[1],
                'price': row[2]
            }
            product_list.append(item_dict)
        body = {'products': product_list}
        return build_response(200, body)
    except Exception as e:
        print(f"Error retrieving products: {e}")
        return build_response(500, 'Internal Server Error')
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def save_product(product_data):
    try:
        conn = psycopg2.connect(
            host=db_host,
            port=db_port,
            database=db_name,
            user=db_user,
            password=db_password
        )
        cursor = conn.cursor()
        cursor.execute("INSERT INTO product_inventory (productId, name, price) VALUES (%s, %s, %s)", (product_data['productId'], product_data['name'], product_data['price']))
        conn.commit()
        return build_response(200, {'Operation': 'SAVE', 'Message': 'SUCCESS', 'Item': product_data})
    except Exception as e:
        print(f"Error saving product: {e}")
        return build_response(500, 'Internal Server Error')
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def modify_product(product_id, update_key, update_value):
    try:
        conn = psycopg2.connect(
            host=db_host,
            port=db_port,
            database=db_name,
            user=db_user,
            password=db_password
        )
        cursor = conn.cursor()
        cursor.execute(f"UPDATE product_inventory SET {update_key} = %s WHERE productId = %s", (update_value, product_id))
        conn.commit()
        return build_response(200, {'Operation': 'UPDATE', 'Message': 'SUCCESS', 'UpdatedAttributes': {update_key: update_value}})
    except Exception as e:
        print(f"Error modifying product: {e}")
        return build_response(500, 'Internal Server Error')
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def delete_product(product_id):
    try:
        conn = psycopg2.connect(
            host=db_host,
            port=db_port,
            database=db_name,
            user=db_user,
            password=db_password
        )
        cursor = conn.cursor()
        cursor.execute("DELETE FROM product_inventory WHERE productId = %s", (product_id,))
        conn.commit()
        return build_response(200, {'Operation': 'DELETE', 'Message': 'SUCCESS', 'Item': {'productId': product_id}})
    except Exception as e:
        print(f"Error deleting product: {e}")
        return build_response(500, 'Internal Server Error')
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def build_response(status_code, body=None):
    if body is None:
        body = {}

    def convert_decimal_to_float(obj):
        if isinstance(obj, Decimal):
            return float(obj)
        elif isinstance(obj, list):
            return [convert_decimal_to_float(item) for item in obj]
        elif isinstance(obj, dict):
            return {key: convert_decimal_to_float(value) for key, value in obj.items()}
        else:
            return obj

    body = convert_decimal_to_float(body)

    return {
        'statusCode': status_code,
        'headers': {'Content-Type': 'application/json'},
        'body': json.dumps(body)
    }
