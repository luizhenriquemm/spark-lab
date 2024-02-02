from flask import Flask, redirect, url_for, request, jsonify
import psycopg, os, json, datetime
from confluent_kafka import Producer
from psycopg.rows import dict_row

producer = Producer({'bootstrap.servers':'kafka:29092'})
def kafka_producer(table: str, event: str="create", data_source: dict = None):
    data = {
        **data_source,
        "event": event,
        "timestamp": datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
    }
    data_json = json.dumps(data)
    producer.poll(1)
    producer.produce(
        table, 
        data_json.encode('utf-8'),
        callback=kafka_producer_callback
    )
    producer.flush()

def kafka_producer_callback(err,msg):
    if err is not None:
        print('Error: {}'.format(err))
    else:
        message = 'Produced message on topic {} with value of {}\n'.format(msg.topic(), msg.value().decode('utf-8'))
        print(message)

def pgsql_get_conn(host="postgres-debezium", port="5432", dbname="app", user="postgres", password="password"):
    conn = psycopg.connect(
        f"host={host} port={port} dbname={dbname} user={user} password={password}",
        row_factory=dict_row)
    return conn

def pgsql_close_conn(conn):
    conn.commit()
    conn.close()

def pgsql_insert(table: str, values: list):
    conn = pgsql_get_conn()
    cur = conn.cursor()
    table_info = get_table_info(table)
    cols_str = ",".join(table_info["cols"])
    types_str = ",".join(["%"+v for v in table_info["cols"].values()])
    cur.execute(
        f"INSERT INTO public.{table} ({cols_str}) VALUES ({types_str}) RETURNING *",
        tuple(values)
    )
    r = cur.fetchone()
    pgsql_close_conn(conn)

    kafka_producer(table, "create", r)

    return r

def pgsql_select(table: str, cols: list=None, where: str=None, where_values: list=None):
    conn = pgsql_get_conn()
    cur = conn.cursor()
    if cols is None:
        cols_str = "*"
    else:
        cols_str = ",".join(cols)
    if where is not None:
        cur.execute(f"SELECT {cols_str} from public.{table} WHERE {where}", tuple(where_values))
    else:
        cur.execute(f"SELECT {cols_str} from public.{table}")
    r = cur.fetchall()
    pgsql_close_conn(conn)
    return r

def pgsql_update(table: str, values: dict=None, where: str=None, where_values: list=None):
    conn = pgsql_get_conn()
    cur = conn.cursor()
    update_str = ",".join([f"{k}=%s" for k in values.keys()])
    cur.execute(
        f"UPDATE public.{table} SET {update_str} WHERE {where} RETURNING *",
        tuple(values.values() + where_values)
    )
    updated_data = cur.fetchone()
    pgsql_close_conn(conn)

    kafka_producer(table, "update", updated_data)

    return True

def pgsql_delete(table: str, where: str=None, where_values: list=None):
    conn = pgsql_get_conn()
    cur = conn.cursor()
    if where is not None:
        cur.execute(f"DELETE FROM public.{table} WHERE {where}", tuple(where_values))
    else:
        return False
    pgsql_close_conn(conn)
    return True

def get_table_info(table: str):
    if table == "clients":
        return {
            "pk": "client_id",
            "cols": {
                "name": "s", 
                "gender": "s", 
                "birthdate": "s", 
                "address": "s", 
                "city": "s", 
                "state": "s"
            }
        }
    elif table == "products":
        return {
            "pk": "product_id",
            "cols": {
                "name": "s", 
                "value": "s", 
                "category_id": "s"
            }
        }
    elif table == "sales":
        return {
            "pk": "sale_id",
            "cols": {
                "client_id": "s", 
                "total_value": "s"
            }
        }
    elif table == "sale_items":
        return {
            "pk": "item_id",
            "cols": {
                "product_id": "s", 
                "sale_id": "s",
                "quantity": "s",
                "unitary_value": "s"
            }
        }
    return {}

def get_data_for_table(table: str, ddd: dict):
    table_info = get_table_info(table)["cols"]
    r = {c: None for c in table_info}
    for c in table_info.keys():
        r[c] = ddd.get(c, None)
    return r

app = Flask(__name__)

@app.route('/api/cadastro/<cliente_id>', methods=['GET', 'POST', 'PATCH', 'DELETE'])
def cadastro_cliente(cliente_id):
    if request.method == 'GET':
        data = pgsql_select(
            table = "clients",
            cols = "*",
            where = "client_id = %s",
            where_values = [cliente_id])
        
        return jsonify(data), 200
    
    elif request.method == 'POST' and cliente_id == "novo":
        data = request.json
        needed_params = get_table_info("clients")["cols"]

        for param in needed_params:
            if param not in data:
                return jsonify({
                    "status_code": 400,
                    "error": "HTTP_400_BAD_REQUEST / MISSING_PARAMETER",
                    "detail": f"Missing parameter: {param}" 
                }), 400
        
        q = pgsql_insert("clients", get_data_for_table("clients", data).values())

        return jsonify({
            "status_code": 200,
            "r": q
        }), 200
    
    elif request.method == 'PATCH':
        data = request.json
        table_info = get_table_info("clients")
        to_update = {}
        for col in data:
            if col in table_info.keys():
                to_update[col] = data[col]
        q = pgsql_update(
            table="clients", 
            values=to_update, 
            where = "client_id = %s",
            where_values = [cliente_id]
        )
        return jsonify({
            "status_code": 200,
            "r": q
        }), 200

    elif request.method == 'DELETE':
        q = pgsql_delete(
            table="clients",
            where = "client_id = %s",
            where_values = [cliente_id]
        )
        return jsonify({
            "status_code": 200,
            "r": q
        }), 200
    
    else:
        return jsonify({
            "status_code": 405,
            "error": "HTTP_405_METHOD_NOT_ALLOWED",
            "detail": f"Method not allowed: {request.method}" 
        }), 405
    
@app.route('/api/produto/<item_id>', methods=['GET', 'POST', 'PATCH', 'DELETE'])
def produto_item(item_id):
    if request.method == 'GET':
        data = pgsql_select(
            table = "products",
            cols = "*",
            where = "product_id = %s",
            where_values = [item_id])
        
        return jsonify(data), 200
    
    elif request.method == 'POST' and item_id == "novo":
        data = request.json
        needed_params = get_table_info("products")["cols"]

        for param in needed_params:
            if param not in data:
                return jsonify({
                    "status_code": 400,
                    "error": "HTTP_400_BAD_REQUEST / MISSING_PARAMETER",
                    "detail": f"Missing parameter: {param}" 
                }), 400
        
        q = pgsql_insert("products", get_data_for_table("products", data).values())

        return jsonify({
            "status_code": 200,
            "r": q
        }), 200
    
    elif request.method == 'PATCH':
        data = request.json
        table_info = get_table_info("products")
        to_update = {}
        for col in data:
            if col in table_info.keys():
                to_update[col] = data[col]
        q = pgsql_update(
            table="products", 
            values=to_update, 
            where = "product_id = %s",
            where_values = [item_id]
        )
        return jsonify({
            "status_code": 200,
            "r": q
        }), 200

    elif request.method == 'DELETE':
        q = pgsql_delete(
            table="products",
            where = "product_id = %s",
            where_values = [item_id]
        )
        return jsonify({
            "status_code": 200,
            "r": q
        }), 200
    
    else:
        return jsonify({
            "status_code": 405,
            "error": "HTTP_405_METHOD_NOT_ALLOWED",
            "detail": f"Method not allowed: {request.method}" 
        }), 405
  
@app.route('/api/venda/<venda_id>', methods=['GET', 'POST', 'PATCH', 'DELETE'])
def venda_item(venda_id):
    if request.method == 'GET':
        data = pgsql_select(
            table = "sales",
            cols = "*",
            where = "sale_id = %s",
            where_values = [venda_id]
        )
        
        items = pgsql_select(
            table = "sale_items",
            cols = "*",
            where = "sale_id = %s",
            where_values = [venda_id]
        )
        
        return jsonify({
            **data, 
            "items": items
        }), 200
    
    elif request.method == 'POST' and venda_id == "novo":
        data = request.json
        needed_params = [
            *get_table_info("sales")["cols"].keys(),
            "items"
        ]

        for param in needed_params:
            if param not in data:
                return jsonify({
                    "status_code": 400,
                    "error": "HTTP_400_BAD_REQUEST / MISSING_PARAMETER",
                    "detail": f"Missing parameter: {param}" 
                }), 400
        
        q = pgsql_insert("sales", get_data_for_table("sales", data).values())
        sale_id = q["sale_id"]

        for item in data["items"]:
            q = pgsql_insert("sale_items", [
                item["product_id"],
                sale_id,
                item["quantity"],
                item["unitary_value"]
            ])

        return jsonify({
            "status_code": 200,
            "r": q
        }), 200
    
    elif request.method == 'PATCH':
        data = request.json
        table_info = get_table_info("sales")
        to_update = {}
        for col in data:
            if col in table_info.keys():
                to_update[col] = data[col]
        q = pgsql_update(
            table="sales", 
            values=to_update, 
            where = "sale_id = %s",
            where_values = [venda_id]
        )
        return jsonify({
            "status_code": 200,
            "r": q
        }), 200

    elif request.method == 'DELETE':
        q = pgsql_delete(
            table="sales",
            where = "sale_id = %s",
            where_values = [venda_id]
        )
        return jsonify({
            "status_code": 200,
            "r": q
        }), 200
    
    else:
        return jsonify({
            "status_code": 405,
            "error": "HTTP_405_METHOD_NOT_ALLOWED",
            "detail": f"Method not allowed: {request.method}" 
        }), 405

if __name__ == '__main__':
    host = os.environ.get("SERVER_HOST", "localhost")
    port = os.environ.get("SERVER_PORT", 5000)

    app.run(
        host=host, 
        port=port,
        debug=True
    )