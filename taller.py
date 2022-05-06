#!/usr/bin/env python
import os
import logging
from json import dumps
from this import d

from flask import Flask, g, Response, request
from neo4j import GraphDatabase, basic_auth

app = Flask(__name__, static_url_path = "/static/")

# Try to load database connection info from environment
url = os.getenv("NEO4J_URI", "bolt://localhost:7687")
username = os.getenv("NEO4J_USER", "neo4j")
password = os.getenv("NEO4J_PASSWORD", "1234")
neo4jVersion = os.getenv("NEO4J_VERSION", "4")
database = os.getenv("NEO4J_DATABASE", "taller")
port = os.getenv("PORT", 8080)

# Create a database driver instance
driver = GraphDatabase.driver(url, auth = basic_auth(username, password))


# Connect to database only once and store session in current context
def get_db():
    if not hasattr(g, "taller"):
        if neo4jVersion.startswith("4"):
            g.neo4j_db = driver.session(database = database)
        else:
            g.neo4j_db = driver.session()
    return g.neo4j_db




#Post Comprador
#atributo nombre
@app.route("/comprador", methods=['POST'])
def post_comprador():
    """
    Crea un nuevo comprador
    """
    db = get_db()
    nombre = request.json['nombre']
    db.run("CREATE (a:Comprador {nombre: $nombre})", nombre=nombre)
    return Response(status=201)

#Get Compradores
@app.route("/compradores", methods=['GET'])
def get_comprador():
    """
    Obtiene todos los compradores
    """
    db = get_db()
    result = db.run("MATCH (a:Comprador) RETURN a.nombre AS nombre")
    return Response(dumps(result.data()),  mimetype='application/json')

#Post Vendedor
#atributo nombre
@app.route("/vendedor", methods=['POST'])
def post_vendedor():
    """
    Crea un nuevo vendedor"""
    db = get_db()
    nombre = request.json['nombre']
    db.run("CREATE (a:Vendedor {nombre: $nombre})", nombre=nombre)
    return Response(status=201)

#get Vendedores
@app.route("/vendedores", methods=['GET'])
def get_vendedores():
    """
    Retorna todos los vendedores
    """
    db = get_db()
    result = db.run("MATCH (a:Vendedor) RETURN a.nombre AS nombre")
    print(result)
    return Response(dumps(result.data()),  mimetype='application/json')


#Post producto
#atributo nombre categoria 
@app.route("/producto", methods=['POST'])
def post_producto():
    """
    Crea un producto
    """
    db = get_db()
    nombre = request.json['nombre']
    categoria = request.json['categoria']
    db.run("CREATE (a:Producto {nombre: $nombre, categoria: $categoria})", nombre=nombre, categoria=categoria)
    return Response(status=201)

#get productos
@app.route("/productos", methods=['GET'])
def get_productos():
    """
    Retorna todos los productos
    """
    db = get_db()
    result = db.run("MATCH (a:Producto) RETURN a.nombre AS nombre, a.categoria AS categoria")
    return Response(dumps(result.data()),  mimetype='application/json')


#producto asociado a un vendedor.
@app.route("/producto_with_vendedor", methods=['POST'])
def post_producto_with_vendedor():
    """
    Relacion producto con vendedor
    """
    print("entro")
    print("Producto con vendedor")
    db = get_db()
    nombre_producto = request.json['producto']
    nombre_vendedor = request.json['vendedor']
    db.run("MATCH (a:Producto {nombre: $nombre_producto}), (b:Vendedor {nombre: $nombre_vendedor}) CREATE (a)-[:VENDIDO_POR]->(b)", nombre_producto=nombre_producto, nombre_vendedor=nombre_vendedor)
    return Response(status=201)

#Get productos asociados a un vendedor
@app.route("/productos_with_vendedor", methods=['GET'])
def get_productos_with_vendedor():
    """
    Retorna todos los productos asociados a un vendedor
    """
    db = get_db()
    nombre_vendedor = request.args.get('nombre_vendedor')
    result = db.run("MATCH (a:Producto)-[:VENDIDO_POR]->(b:Vendedor {nombre: $nombre_vendedor}) RETURN a.nombre AS nombre, a.categoria AS categoria", nombre_vendedor=nombre_vendedor)
    return Response(dumps(result.data()),  mimetype='application/json')

#Realizar una compra de un producto por parte de un comprador.
@app.route("/comprar", methods=['POST'])
def post_comprar():
    db = get_db()
    nombre_producto = request.json['producto']
    nombre_comprador = request.json['comprador']
    db.run("MATCH (a:Producto {nombre: $nombre_producto}), (b:Comprador {nombre: $nombre_comprador}) CREATE (a)-[:COMPRADO_POR]->(b)", nombre_producto=nombre_producto, nombre_comprador=nombre_comprador)
    return Response(status=201)

#Recomendar un producto.
#Un comprador relaciona la RECOMIENDA un producto con atributo calificación siendo este un entero entre 1 y 5.
#Metodo documentado
@app.route("/recomendar", methods=['POST'])
def post_recomendar():
    """
    Recomendar un producto
    """
    db = get_db()
    nombre_comprador = request.json['comprador']
    nombre_producto = request.json['producto']
    calificacion = request.json['calificacion']
    db.run("MATCH (a:Producto {nombre: $nombre_producto}), (b:Comprador {nombre: $nombre_comprador}) CREATE (b)-[:RECOMIENDA {calificacion: $calificacion}]->(a)", nombre_producto=nombre_producto, nombre_comprador=nombre_comprador, calificacion=calificacion)
    return Response(status=201)

#Retornar el TOP 5 de los productos más vendidos
#Obtener promedio de la relacion con atributo calificacion para cada producto
@app.route("/top_5_productos", methods=['GET'])
def get_top_5_productos():
    db = get_db()
    result = db.run("MATCH (a:Comprador)-[r:RECOMIENDA]->(b:Producto) RETURN b.nombre AS nombre, AVG(r.calificacion) AS promedio ORDER BY promedio DESC LIMIT 5")
    return Response(dumps(result.data()),  mimetype='application/json')
    

# Encontrar otros compradores que han comprado el mismo producto,
# retornar los productos que han comprado
@app.route("/otros_compradores", methods=['POST'])


#metodo get 
@app.route("/sugerencia", methods=['GET'])
def get_sugerencia():
    db = get_db()
    nombre_producto = request.json['producto']
    nombre_comprador = request.json['comprador']
    result = db.run("MATCH (a:Producto {nombre: $nombre_producto})-[:COMPRADO_POR]->(b:Comprador) WHERE b.nombre <> $nombre_comprador RETURN b.nombre AS nombre", nombre_producto=nombre_producto, nombre_comprador=nombre_comprador)
    print(result.data())    
    for r in result:
        print(r)
    return Response(dumps(result.data()),  mimetype='application/json')

@app.route("/sugerencia_calificacion", methods=['GET'])
def get_sugerencia_calificacion():
    db = get_db()
    nombre_producto = request.json['producto']
    result =  db.run("MATCH (b:Comprador)<-[c:COMPRADO_POR]-(a:Producto {nombre: 'Jean'})<-[r:RECOMIENDA]-(b)  RETURN 0.4*COUNT(c) + AVG(r.calificacion) as ranking_sugerencia", nombre_producto=nombre_producto)
    return Response(dumps(result.data()),  mimetype='application/json')
    
    



if __name__ == '__main__':
    logging.info('Running on port %d, database is at %s', port, url)
    app.run(port=port)