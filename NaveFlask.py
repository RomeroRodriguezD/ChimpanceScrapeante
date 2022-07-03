from flask import Flask, render_template, redirect, url_for, session, send_file, request, Response, make_response
import requests
import json
import pandas as pd
import os
import io
import time
import random
from bs4 import BeautifulSoup as bs
from io import BytesIO
import os
import psycopg2 as sql
import csv
import re
import unicodedata

app = Flask(__name__)
app.secret_key = '8b98c57127466fe0e293ca56f47cfaf81c96c2d098680dfd62f81133236dcbdb'


#---------- Índice --------- #

@app.route("/")

def index():
    return render_template("index.html")


@app.route("/busqueda_viviendas")

def viviendas():
    return render_template("viviendas.html")

@app.route("/busqueda_habitaciones")

def habitaciones():
    return render_template("habitaciones.html")

@app.route("/resultados", methods=['GET', 'POST'])

def scrapear_naves():

    try:

        if request.method == 'POST':
            # Podría ir en una función esta parte, elimina acentos:
            ciudad = str(request.form.get('ciudad'))
            ciudad = unicodedata.normalize('NFD', ciudad)
            ciudad = ciudad.encode('ascii', 'ignore')
            ciudad = ciudad.decode('utf-8')
            ciudad = str(ciudad)
            ciudad_label= str(request.form.get('ciudad')).title()
            ciudad = ciudad.replace(" ", "-")
            provincia = str(request.form.get('provincia'))
            provincia = unicodedata.normalize('NFD', provincia)
            provincia = provincia.encode('ascii', 'ignore')
            provincia = provincia.decode('utf-8')
            provincia = str(provincia)
            regimen = str(request.form.get('regimen'))
            print(ciudad, regimen, provincia)
            headers = {
                'Host':'www.idealista.com',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
                'Accept-Encoding': 'gzip, deflate, br',
                'Accept-Language': 'es-ES,es;q=0.8,en-US;q=0.5,en;q=0.3',
                'Connection': 'keep-alive',
                'DNT': '1',
                'Host': 'www.idealista.com',
                'Sec-Fetch-Dest': 'document',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:100.0) Gecko/20100101 Firefox/101.0'}


            headers = {k: str(v).encode("utf-8") for k, v in headers.items()}  # Encode en UTF-8

            # Data that will be on the output dataframe

            precios = []
            calles = []
            enlaces = []
            descripciones = []
            media = []
            moda = []
            metros_finales = []
            # media_m2 = [] De momento descartada
            siguiente = True
            pagina = 1

            while siguiente:
                time.sleep(random.randint(3,7))
                base_url = f"https://www.idealista.com/{regimen.lower()}-locales/{ciudad.lower()}-{provincia.lower()}/con-naves/pagina-{pagina}.htm?ordenado-por=precios-asc"
                print(base_url)
                web = requests.get(base_url, headers=headers)
                soup = bs(web.content, 'html.parser')

                precio = soup.findAll("span", attrs={"class", "item-price h2-simulated"})
                calle = soup.findAll("a", attrs={"class", "item-link"})
                enlace = soup.findAll("a", attrs={"class", "item-link", "href", "/inmueble*"})  # Enlace del inmueble
                descrip = soup.findAll("div", attrs={"class", "item-description description"})
                metros = soup.findAll("span", attrs={"class", "item-detail"})
                seguir = soup.findAll("li", attrs={"class", "next"}) #a, icon-arrow-right-after


                if len(seguir) == 0:  # If there's no next page, it stops the loop.

                    siguiente=False

                #print(seguir)
                print(f"Vuelta {pagina}") # Page check

                for i in precio:  # First converts the price into string, removes dots, then it converts it into a float.
                    new_precio = str(i.contents[0])
                    new_precio2 = new_precio.replace(".", "")
                    new_precio2 = float(new_precio2)
                    precios.append(new_precio2)

                for j in calle:
                    new_calle = str(j.contents)
                    calles.append(new_calle)

                for p in enlace:
                    new_enlace = "https://www.idealista.com"
                    new_enlace += str(p['href'])
                    enlaces.append(new_enlace)

                for result in descrip:  # If it does not find any description, adds a default row to make the data rows match.

                    try:
                        meaning = result.find(class_="ellipsis")
                        descripciones.append(meaning.text)

                    except:
                        descripciones.append("Sin descripción")

                # Cleans useless symbols from descriptions.

                signo1 = "["
                signo2 = "]"
                calles = [s.replace(signo1, "") for s in calles]
                calles = [s.replace(signo2, "") for s in calles]

                try:   #Makes corrections on description content, if there's no description, it just pass.

                    for i in descripciones:
                        descripciones = [s.replace(signo1, "") for s in descripciones]
                        descripciones = [s.replace(signo2, "") for s in descripciones]
                        descripciones = [s.replace('\\n', "") for s in descripciones]

                except:
                    pass

                # Loop to get m2 tag. Double loop since it needs to be separated from useless data inside "metros".


                meses = ['ene', 'feb', 'mar', 'abr', 'may', 'jun', 'jul', 'ago', 'sep', 'oct', 'nov', 'dic']

                '''for item in metros:
                    for i in item:
                        if "," in i:
                            metros.remove(item)'''

                for item in metros:
                    print(item)
                    new_item = str(item)
                    if '€' in new_item:
                        metros.remove(item)




                for item in metros:  # Eliminar items con fechas que tienen también 6 carácteres:
                    for i in item:
                        if "jun" in i:
                            metros.remove(item)
                        elif "may" in i:
                            metros.remove(item)
                        elif "jul" in i:
                            metros.remove(item)
                        elif "ago" in i:
                            metros.remove(item)
                        elif "sep" in i:
                            metros.remove(item)
                        elif "abr" in i:
                            metros.remove(item)


                for item in metros:
                    cuenta=1
                    for i in item:

                        item2 = str(i).strip(' ') #Strip spaces from each "item-detail"
                        item2 = str(item2.strip('.'))
                        item2 = str(item2.strip(','))
                        if len(item2) <=6: # If it has 3 numbers its a valid m2 value.
                            metros_finales.append((item2))
                        elif len(item2)==2:
                           # if int(item2)>25:  # Filter needed to avoid getting 2 rows from very big houses because they have 10 or more rooms.
                            metros_finales.append((item2))
                        break
                pagina += 1

            #Mean and mode for prices. Then, converted to dataframe.

           # mediahecha = round(statistics.mean(precios), 2)
            #modahecha = statistics.mode(precios)
            # Append to lists
            #media.append(mediahecha)
            #moda.append(modahecha)

            #metros_media = round(statistics.mean(metros_finales))
            #media_m2.append(metros_media)
            #media_m2frame = pd.DataFrame({'Media m2':media_2})
            # Makes them a DataFrame
            modaframe = pd.DataFrame({'Moda': moda})
            mediaframe = pd.DataFrame({'Media': media})
            # Resets index.
            modaframe = modaframe.reset_index()
            mediaframe = mediaframe.reset_index()
            # Turns m2 into a DataFrame
            metros_frame = pd.DataFrame({'m2':metros_finales})
            metros_frame.reset_index()
            #Crafts the main dataframe
            viviendas = pd.DataFrame({'Precio': precios, 'Calle': calles, 'Enlace': enlaces, 'Descripciones':descripciones})
            viviendas.reset_index()
            # Crafts the final DataFrame with all the previous ones merged, turns it into a .xls
            viviendas_finales = [viviendas, metros_frame]
            archivo_viviendas = pd.concat(viviendas_finales, axis=1)
            #del archivo_viviendas['index'] #Deletes index column
            password = os.environ['password']
            host = os.environ['host']
            database = os.environ['database']
            user = os.environ['user']
            port = os.environ['port']

            new_connection = sql.connect(host=host, user=user, password=password, database=database, port=port)
            cursor = new_connection.cursor()
            query4 = 'DROP TABLE IF EXISTS webdatabase.temporal;'
            cursor.execute(query4)
            new_connection.commit()
            query1 = 'CREATE TABLE webdatabase.temporal( id serial NOT NULL, precio numeric, calle text, enlace text,descripciones text,m2 numeric,PRIMARY KEY (id));'
            cursor.execute(query1)
            new_connection.commit()

            for row in archivo_viviendas.itertuples():
                precio = row.Precio
                calle = row.Calle
                enlace = row.Enlace
                descripciones = row.Descripciones
                m2 = row.m2
                lista = (precio, calle, enlace, descripciones, m2)
                query2 = 'INSERT into webdatabase.temporal (precio, calle, enlace, descripciones, m2) values ( %s, %s, %s, %s, %s);'
                cursor.execute(query2, lista)
                new_connection.commit()

            # Devolver la tabla:

            query3 = 'SELECT * FROM webdatabase.temporal;'
            cursor.execute(query3)
            tabla = cursor.fetchall()
            query_table = pd.DataFrame(tabla)
            query_table.columns = [desc[0] for desc in cursor.description]
            global exportar
            exportar = json.loads(query_table.to_json(orient='records'))

            return render_template('blog.html', posts=exportar, nombre=ciudad_label)
    except:
        return render_template('criterios.html')

@app.route("/resultados_viviendas", methods=['GET', 'POST'])

def scrapear_viviendas():

    try:

        if request.method == 'POST':
            # Podría ir en una función esta parte, elimina acentos:
            ciudad = str(request.form.get('ciudad'))
            ciudad = unicodedata.normalize('NFD', ciudad)
            ciudad = ciudad.encode('ascii', 'ignore')
            ciudad = ciudad.decode('utf-8')
            ciudad = str(ciudad)
            ciudad_label = str(request.form.get('ciudad')).title()
            ciudad = ciudad.replace(" ", "-")
            provincia = str(request.form.get('provincia'))
            provincia = unicodedata.normalize('NFD', provincia)
            provincia = provincia.encode('ascii', 'ignore')
            provincia = provincia.decode('utf-8')
            provincia = str(provincia)
            regimen = str(request.form.get('regimen'))
            print(ciudad, regimen, provincia)
            headers = {
                'Host':'www.idealista.com',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
                'Accept-Encoding': 'gzip, deflate, br',
                'Accept-Language': 'es-ES,es;q=0.8,en-US;q=0.5,en;q=0.3',
                'Connection': 'keep-alive',
                'DNT': '1',
                'Host': 'www.idealista.com',
                'Sec-Fetch-Dest': 'document',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:100.0) Gecko/20100101 Firefox/101.0'}


            headers = {k: str(v).encode("utf-8") for k, v in headers.items()}  # Encode en UTF-8

            # Data that will be on the output dataframe

            precios = []
            calles = []
            enlaces = []
            descripciones = []
            media = []
            moda = []
            metros_finales = []
            # media_m2 = [] De momento descartada
            siguiente = True
            pagina = 1

            while siguiente:
                time.sleep(random.randint(3,7))
                base_url = f"https://www.idealista.com/{regimen.lower()}-viviendas/{ciudad.lower()}-{provincia.lower()}/pagina-{pagina}.htm?ordenado-por=precios-asc"
                web = requests.get(base_url, headers=headers)
                soup = bs(web.content, 'html.parser')

                precio = soup.findAll("span", attrs={"class", "item-price h2-simulated"})
                calle = soup.findAll("a", attrs={"class", "item-link"})
                enlace = soup.findAll("a", attrs={"class", "item-link", "href", "/inmueble*"})  # Enlace del inmueble
                descrip = soup.findAll("div", attrs={"class", "item-description description"})
                #metros = soup.findAll("span", attrs={"class", "item-detail"})
                seguir = soup.findAll("li", attrs={"class", "next"}) #a, icon-arrow-right-after


                if len(seguir) == 0:  # If there's no next page, it stops the loop.

                    siguiente=False

                #print(seguir)
                print(f"Vuelta {pagina}") # Page check

                for i in precio:  # First converts the price into string, removes dots, then it converts it into a float.
                    new_precio = str(i.contents[0])
                    new_precio2 = new_precio.replace(".", "")
                    new_precio2 = float(new_precio2)
                    precios.append(new_precio2)

                for j in calle:
                    new_calle = str(j.contents)
                    calles.append(new_calle)

                for p in enlace:
                    new_enlace = "https://www.idealista.com"
                    new_enlace += str(p['href'])
                    enlaces.append(new_enlace)

                for result in descrip:  # If it does not find any description, adds a default row to make the data rows match.

                    try:
                        meaning = result.find(class_="ellipsis")
                        descripciones.append(meaning.text)

                    except:
                        descripciones.append("Sin descripción")

                # Cleans useless symbols from descriptions.

                signo1 = "["
                signo2 = "]"
                calles = [s.replace(signo1, "") for s in calles]
                calles = [s.replace(signo2, "") for s in calles]

                try:   #Makes corrections on description content, if there's no description, it just pass.

                    for i in descripciones:
                        descripciones = [s.replace(signo1, "") for s in descripciones]
                        descripciones = [s.replace(signo2, "") for s in descripciones]
                        descripciones = [s.replace('\\n', "") for s in descripciones]

                except:
                    pass


                pagina += 1


            viviendas = pd.DataFrame({'Precio': precios, 'Calle': calles, 'Enlace': enlaces, 'Descripciones':descripciones})
            viviendas.reset_index()
            # Crafts the final DataFrame with all the previous ones merged, turns it into a .xls
            viviendas_finales = [viviendas]
            archivo_viviendas = pd.concat(viviendas_finales, axis=1)
            password = os.environ['password']
            host = os.environ['host']
            database = os.environ['database']
            user = os.environ['user']
            port = os.environ['port']

            new_connection = sql.connect(host=host, user=user, password=password, database=database, port=port)
            cursor = new_connection.cursor()
            query4 = 'DROP TABLE IF EXISTS webdatabase.temporal;'
            cursor.execute(query4)
            new_connection.commit()
            query1 = 'CREATE TABLE webdatabase.temporal( id serial NOT NULL, precio numeric, calle text, enlace text,descripciones text,m2 numeric,PRIMARY KEY (id));'
            cursor.execute(query1)
            new_connection.commit()
            print(base_url)
            for row in archivo_viviendas.itertuples():
                precio = row.Precio
                calle = row.Calle
                enlace = row.Enlace
                descripciones = row.Descripciones
                lista = (precio, calle, enlace, descripciones)
                query2 = 'INSERT into webdatabase.temporal (precio, calle, enlace, descripciones) values ( %s, %s, %s, %s);'
                print(precio)
                print(calle)
                print(enlace)
                print(descripciones)
                cursor.execute(query2, lista)
                new_connection.commit()

            # Devolver la tabla:

            query3 = 'SELECT * FROM webdatabase.temporal;'
            cursor.execute(query3)
            tabla = cursor.fetchall()
            query_table = pd.DataFrame(tabla)
            query_table.columns = [desc[0] for desc in cursor.description]
            global exportar
            exportar = json.loads(query_table.to_json(orient='records'))
            return render_template('blog_viviendas.html', posts=exportar, nombre=ciudad_label)

    except:

        return render_template('criterios.html')

@app.route("/resultados_habitaciones", methods=['GET', 'POST'])

def scrapear_habitaciones():

    try:

        if request.method == 'POST':
            # Podría ir en una función esta parte, elimina acentos:
            ciudad = str(request.form.get('ciudad'))
            ciudad = unicodedata.normalize('NFD', ciudad)
            ciudad = ciudad.encode('ascii', 'ignore')
            ciudad = ciudad.decode('utf-8')
            ciudad = str(ciudad)
            ciudad_label = str(request.form.get('ciudad')).title()
            ciudad = ciudad.replace(" ", "-")
            provincia = str(request.form.get('provincia'))
            provincia = unicodedata.normalize('NFD', provincia)
            provincia = provincia.encode('ascii', 'ignore')
            provincia = provincia.decode('utf-8')
            provincia = str(provincia)
            headers = {
                'Host':'www.idealista.com',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
                'Accept-Encoding': 'gzip, deflate, br',
                'Accept-Language': 'es-ES,es;q=0.8,en-US;q=0.5,en;q=0.3',
                'Connection': 'keep-alive',
                'DNT': '1',
                'Host': 'www.idealista.com',
                'Sec-Fetch-Dest': 'document',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:100.0) Gecko/20100101 Firefox/101.0'}


            headers = {k: str(v).encode("utf-8") for k, v in headers.items()}  # Encode en UTF-8

            # Data that will be on the output dataframe

            precios = []
            calles = []
            enlaces = []
            descripciones = []
            media = []
            moda = []
            metros_finales = []
            # media_m2 = [] De momento descartada
            siguiente = True
            pagina = 1

            while siguiente:
                time.sleep(random.randint(3,7))
                base_url = f"https://www.idealista.com/alquiler-habitacion/{ciudad.lower()}-{provincia.lower()}/pagina-{pagina}.htm?ordenado-por=precios-asc"
                web = requests.get(base_url, headers=headers)
                soup = bs(web.content, 'html.parser')

                precio = soup.findAll("span", attrs={"class", "item-price h2-simulated"})
                calle = soup.findAll("a", attrs={"class", "item-link"})
                enlace = soup.findAll("a", attrs={"class", "item-link", "href", "/inmueble*"})  # Enlace del inmueble
                descrip = soup.findAll("div", attrs={"class", "item-description description"})
                #metros = soup.findAll("span", attrs={"class", "item-detail"})
                seguir = soup.findAll("li", attrs={"class", "next"}) #a, icon-arrow-right-after


                if len(seguir) == 0:  # If there's no next page, it stops the loop.

                    siguiente=False

                #print(seguir)
                print(f"Vuelta {pagina}") # Page check

                for i in precio:  # First converts the price into string, removes dots, then it converts it into a float.
                    new_precio = str(i.contents[0])
                    new_precio2 = new_precio.replace(".", "")
                    new_precio2 = float(new_precio2)
                    precios.append(new_precio2)

                for j in calle:
                    new_calle = str(j.contents)
                    calles.append(new_calle)

                for p in enlace:
                    new_enlace = "https://www.idealista.com"
                    new_enlace += str(p['href'])
                    enlaces.append(new_enlace)

                for result in descrip:  # If it does not find any description, adds a default row to make the data rows match.

                    try:
                        meaning = result.find(class_="ellipsis")
                        descripciones.append(meaning.text)

                    except:
                        descripciones.append("Sin descripción")

                # Cleans useless symbols from descriptions.

                signo1 = "["
                signo2 = "]"
                calles = [s.replace(signo1, "") for s in calles]
                calles = [s.replace(signo2, "") for s in calles]

                try:   #Makes corrections on description content, if there's no description, it just pass.

                    for i in descripciones:
                        descripciones = [s.replace(signo1, "") for s in descripciones]
                        descripciones = [s.replace(signo2, "") for s in descripciones]
                        descripciones = [s.replace('\\n', "") for s in descripciones]

                except:
                    pass



                meses = ['ene', 'feb', 'mar', 'abr', 'may', 'jun', 'jul', 'ago', 'sep', 'oct', 'nov', 'dic']

                pagina += 1

            viviendas = pd.DataFrame({'Precio': precios, 'Calle': calles, 'Enlace': enlaces, 'Descripciones':descripciones})
            viviendas.reset_index()
            # Crafts the final DataFrame with all the previous ones merged, turns it into a .xls
            viviendas_finales = [viviendas]
            archivo_viviendas = pd.concat(viviendas_finales, axis=1)
            #del archivo_viviendas['index'] #Deletes index column
            password = os.environ['password']
            host = os.environ['host']
            database = os.environ['database']
            user = os.environ['user']
            port = os.environ['port']

            new_connection = sql.connect(host=host, user=user, password=password, database=database, port=port)
            cursor = new_connection.cursor()
            query4 = 'DROP TABLE IF EXISTS webdatabase.temporal;'
            cursor.execute(query4)
            new_connection.commit()
            query1 = 'CREATE TABLE webdatabase.temporal( id serial NOT NULL, precio numeric, calle text, enlace text,descripciones text,m2 numeric,PRIMARY KEY (id));'
            cursor.execute(query1)
            new_connection.commit()


            for row in archivo_viviendas.itertuples():
                precio = row.Precio
                calle = row.Calle
                enlace = row.Enlace
                descripciones = row.Descripciones
                lista = (precio, calle, enlace, descripciones)
                query2 = 'INSERT into webdatabase.temporal (precio, calle, enlace, descripciones) values ( %s, %s, %s, %s);'
                print(precio)
                print(calle)
                print(enlace)
                print(descripciones)
                cursor.execute(query2, lista)
                new_connection.commit()

            # Devolver la tabla:

            query3 = 'SELECT * FROM webdatabase.temporal;'
            cursor.execute(query3)
            tabla = cursor.fetchall()
            query_table = pd.DataFrame(tabla)
            query_table.columns = [desc[0] for desc in cursor.description]
            global exportar
            exportar = json.loads(query_table.to_json(orient='records'))

            return render_template('blog_habitaciones.html', posts=exportar, nombre=ciudad_label)
    except:
        return render_template('criterios.html')


@app.route("/download", methods=['GET','POST'])

def get_table(): # ESTA ES LA BUENA

    tabla = exportar

    si = io.StringIO()
    cw = csv.writer(si)
    line = ['Precio, Calle, Enlace, Descripciones, m2']
    cw.writerow(line)

    for row in tabla:
        line = [str(row['precio']) + ',' + str(row['calle']) + ',' + str(row['enlace']) + ',' + str(row['descripciones']) + ',' + str(row['m2'])]
        cw.writerow(line)

    output = make_response(si.getvalue())
    output.headers["Content-Disposition"] = "attachment; filename=export.csv"
    output.headers["Content-type"] = "text/csv"
    return output

@app.route("/download_viviendas", methods=['GET','POST'])

def get_table_viviendas(): # ESTA ES LA BUENA

    tabla = exportar

    si = io.StringIO()
    cw = csv.writer(si)
    line = ['Precio, Calle, Enlace, Descripciones']
    cw.writerow(line)

    for row in tabla:
        line = [str(row['precio']) + ',' + str(row['calle']) + ',' + str(row['enlace']) + ',' + str(row['descripciones'])]
        cw.writerow(line)

    output = make_response(si.getvalue())
    output.headers["Content-Disposition"] = "attachment; filename=export.csv"
    output.headers["Content-type"] = "text/csv"
    return output

@app.route("/download_habitaciones", methods=['GET','POST'])

def get_table_habitaciones(): # ESTA ES LA BUENA

    tabla = exportar

    si = io.StringIO()
    cw = csv.writer(si)
    line = ['Precio, Calle, Enlace, Descripciones']
    cw.writerow(line)

    for row in tabla:
        line = [str(row['precio']) + ',' + str(row['calle']) + ',' + str(row['enlace']) + ',' + str(row['descripciones'])]
        cw.writerow(line)

    output = make_response(si.getvalue())
    output.headers["Content-Disposition"] = "attachment; filename=export.csv"
    output.headers["Content-type"] = "text/csv"
    return output

if __name__ == "__main__": #activa el servidor. Hace referencia al archivo siendo utilizado en ese momento, los módulos importados no devuelven "__main__"



    app.run(debug=True)