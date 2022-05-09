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




#1Post Comprador
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

#2Post Vendedor
#atributo nombre
@app.route("/vendedor", methods=['POST'])
def post_vendedor():
    """
    Crea un nuevo vendedor"""
    db = get_db()
    nombre = request.json['nombre']
    db.run("CREATE (a:Vendedor {nombre: $nombre})", nombre=nombre)
    return Response(status=201)
#postman vendedor
#{"nombre":"vendedor1"}

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


#3Post producto asociado a un vendedor
#atributo nombre categoria 
@app.route("/producto", methods=['POST'])
def post_producto():
    """
    Crea un producto
    """
    db = get_db()
    nombre = request.json['nombre']
    categoria = request.json['categoria']
    ned = request.json['vendedor']
    db.run("MATCH (a:Vendedor {nombre: $ned}) CREATE (a)<-[:VENDIDO_POR]-(b:Producto {categoria: $categoria, nombre: $nombre})", nombre=nombre, categoria=categoria, ned=ned)
    return Response(status=201)
#postman producto
#{"nombre":"producto1","categoria":"categoria1","vendedor":"vendedor1"}
#get productos
@app.route("/productos", methods=['GET'])
def get_productos():
    """
    Retorna todos los productos
    """
    db = get_db()
    result = db.run("MATCH (a:Producto) RETURN a.nombre AS nombre, a.categoria AS categoria")
    return Response(dumps(result.data()),  mimetype='application/json')

#4Realizar una compra de un producto por parte de un comprador.
@app.route("/comprar", methods=['POST'])
def post_comprar():
    db = get_db()
    nombre_producto = request.json['producto']
    nombre_comprador = request.json['comprador']
    db.run("MATCH (a:Producto {nombre: $nombre_producto}), (b:Comprador {nombre: $nombre_comprador}) CREATE (a)-[:COMPRADO_POR]->(b)", nombre_producto=nombre_producto, nombre_comprador=nombre_comprador)
    return Response(status=201)
#postman compra
#{"producto":"producto1","comprador":"comprador1"}
#5Recomendar un producto.
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
#postman recomendar
#{"comprador":"comprador1","producto":"producto1","calificacion":5}

#6Retornar el TOP 5 de los productos más vendidos
#Obtener promedio de la relacion con atributo calificacion para cada producto
@app.route("/top_5_productos", methods=['GET'])
def get_top_5_productos():
    db = get_db()
    result = db.run("MATCH (a:Comprador)<-[c:COMPRADO_POR]-(b:Producto)<-[r:RECOMIENDA]-(a) RETURN b.nombre AS nombre, AVG(r.calificacion) AS promedio, count(c) as compras ORDER BY compras DESC, promedio DESC LIMIT 5")
    return Response(dumps(result.data()),  mimetype='application/json')
    

#7 Sugerencia TOP 3
#ranking_sugerencia = 0.4*cantidad_compras + 0.6*calificacion_promedio 

@app.route("/sugerencia", methods=['GET'])
def get_sugerencia_calificacion():
    """
    Retorna las sugerencias de productos para un comprador dado 
    """
    db = get_db()
    nombre_producto = request.json['producto']
    nombre_comprador = request.json['comprador']
    producto = db.run("MATCH (p1:Producto {nombre: $nombre_producto})-[:COMPRADO_POR]->(b:Comprador WHERE b.nombre <>  $nombre_comprador)<-[c:COMPRADO_POR]-(p2:Producto WHERE p2.nombre <> $nombre_producto)<-[r:RECOMIENDA]-() RETURN  p2.nombre as name_product, AVG(r.calificacion) as calificacion", nombre_producto=nombre_producto, nombre_comprador=nombre_comprador)
    dict ={}
    for i in producto.data():
        dict[i['name_product']] = {'calificacion': i['calificacion'],'compras':n_compras(i['name_product']),'ranking':0.4*n_compras(i['name_product'])+0.6*i['calificacion']}

    #sort by ranking 
    sorted_dict = sorted(dict.items(), key=lambda kv: kv[1]['ranking'], reverse=True)
    #retornar los 3 primeros
    return Response(dumps(sorted_dict[:3]),  mimetype='application/json')
    
    

def n_compras(nombre_producto):
    """Retorna la cantidad de compras de un producto"""
    db = get_db()
    result = db.run("MATCH (a:Producto {nombre: $nombre_producto})-[c:COMPRADO_POR]->(b:Comprador) RETURN count(c) as compras", nombre_producto=nombre_producto)
    return result.data()[0]['compras']


if __name__ == '__main__':
    logging.info('Running on port %d, database is at %s', port, url)
    app.run(port=port)