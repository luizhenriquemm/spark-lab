from flask import Flask, redirect, url_for, request, jsonify
import psycopg, os
from psycopg.rows import dict_row

def pgsql_query(*args):
    result = None
    with psycopg.connect(
        "host=postgres-app-server port=5432 dbname=app user=postgres password=password",
        row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(*args)
            result = cur.fetchall()
            conn.commit()
    return result

app = Flask(__name__)

@app.route('/api/cadastro/novo', methods=['POST'])
def cadastro_novo():
    if request.method == 'POST':
        data = request.json
        needed_params = [
            "name",
            "gender",
            "birthdate",
            "address",
            "city",
            "state"
        ]

        for param in needed_params:
            if param not in data:
                return {
                    "status_code": 400,
                    "error": "HTTP_400_BAD_REQUEST / MISSING_PARAMETER",
                    "detail": f"Missing parameter: {param}" 
                }, 400
        
        try:
            q = pgsql_query(
                "INSERT INTO public.clients (name, gender, birthdate, address, city, state) VALUES (%s, %s, %s, %s, %s, %s)",
                (
                    data["name"],
                    data["gender"],
                    data["birthdate"],
                    data["address"],
                    data["city"],
                    data["state"]
                )
            )
        except psycopg.errors.InvalidDatetimeFormat as e:
            return {
                "status_code": 400,
                "error": "HTTP_400_BAD_REQUEST / INVALID_PARAMETER",
                "detail": e
            }, 400

        return {
            "status_code": 200,
            "r": q
        }, 200
            

    else:
        return {
            "status_code": 405,
            "error": "HTTP_405_METHOD_NOT_ALLOWED",
            "detail": f"Method not allowed: {request.method}" 
        }, 405

@app.route('/api/cadastro/cliente/<cliente_id>', methods=['GET', 'PATCH', 'DELETE'])
def cadastro_cliente(cliente_id):
    if request.method == 'GET':
        client = pgsql_query(f"SELECT * FROM public.clients WHERE client_id = %i", (cliente_id))
        return jsonify(client), 200
    
    elif request.method == 'PATCH':
        data = request.json
        needed_params = []
        for param in needed_params:
            if param not in data:
                return {
                    "status_code": 400,
                    "error": "HTTP_400_BAD_REQUEST / MISSING_PARAMETER",
                    "detail": f"Missing parameter: {param}" 
                }, 400
            
    elif request.method == 'DELETE':
        pass
    
    else:
        return {
            "status_code": 405,
            "error": "HTTP_405_METHOD_NOT_ALLOWED",
            "detail": f"Method not allowed: {request.method}" 
        }, 405
    
@app.route('/api/cadastro/novo', methods=['POST'])
@app.route('/api/cadastro/novo', methods=['POST'])

def hello():
    if request.method == 'POST':
        user = request.form['nm']
        return redirect(url_for('success', name=user))
    else:
        user = request.args.get('nm')
        return redirect(url_for('success', name=user))
    
    return 'Hello, World'

if __name__ == '__main__':
    host = os.environ.get("SERVER_HOST", "localhost")
    port = os.environ.get("SERVER_PORT", 5000)

    app.run(
        host=host, 
        port=port
    )