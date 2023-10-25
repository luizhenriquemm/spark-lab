from flask import Flask, redirect, url_for, request, jsonify
import psycopg

def pgsql_query(**args):
    result = None
    with psycopg.connect("host=postgres-app-server port=5432 dbname=app user=postgres password=password") as conn:
        with conn.cursor() as cur:
            cur.execute(**args)
            result = cur.fetchall()
            conn.commit()
    return result

app = Flask(__name__)

@app.route('/')
def index():
    return 'Index Page'

@app.route('/api/cadastro/novo', methods=['POST'])

@app.route('/api/cadastro/cliente/<cliente_id>', methods=['GET', 'PATCH', 'DELETE'])
def cadastro_cliente(cliente_id):
    if request.method == 'GET':
        pass

    elif request.method == 'PATCH':
        data = request.form
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
    app.run(
        host="0.0.0.0", 
        port=5000
    )