import os
from flask import Flask,render_template, request, redirect, jsonify
from flask_sqlalchemy import SQLAlchemy
import psycopg2
from dotenv import load_dotenv
import pandas as pd
#from app import db

load_dotenv()


app = Flask(__name__)
URL = os.getenv("DATABASE_URL")

#connection = psycopg2.connect(URL)

#app.config['SQLALCHEMY_DATABASE_URI'] = url
#app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
#db = SQLAlchemy(app)


CREATE_DEPARTMENTS_TABLE = """
            CREATE TABLE IF NOT EXISTS empleados.departments
            (
                id integer NOT NULL,
                department text COLLATE pg_catalog."default",
                CONSTRAINT departments_pkey PRIMARY KEY (id)
            );
"""
LLENADO_DEPARTMENTS ="""
            INSERT INTO empleados.departments (id,department) VALUES (%s,%s) ON CONFLICT (id) DO UPDATE SET department = EXCLUDED.department;
"""

CREATE_HIRED_TABLE = """
            CREATE TABLE IF NOT EXISTS empleados.hired
            (
                id integer NOT NULL,
                name text COLLATE pg_catalog."default",
                datetime timestamp with time zone,
                department_id integer,
                job_id integer,
                CONSTRAINT hired_pkey PRIMARY KEY (id)
            );
"""

LLENADO_HIRED ="""
            INSERT INTO empleados.hired (id,name,datetime,department_id,job_id) VALUES (%s,%s,%s,%s,%s) ON CONFLICT (id) DO UPDATE SET (name,datetime,department_id,job_id) = (EXCLUDED.name,EXCLUDED.datetime,EXCLUDED.department_id,EXCLUDED.job_id);
"""

CREATE_JOBS_TABLE = """
            CREATE TABLE IF NOT EXISTS empleados.jobs
            (
                id integer NOT NULL,
                jobs text COLLATE pg_catalog."default",
                CONSTRAINT jobs_pkey PRIMARY KEY (id)
            )
"""

LLENADO_JOBS ="""
            INSERT INTO empleados.jobs (id,jobs) VALUES (%s,%s) ON CONFLICT (id) DO UPDATE SET jobs = EXCLUDED.jobs
"""

GET_METRICA1 = """
    SELECT
        departments.department AS dep,
        jobs.jobs AS job,
        SUM(CASE WHEN DATE_PART('quarter', hired.datetime) = 1 THEN 1 ELSE 0 END) AS Q1,
        SUM(CASE WHEN DATE_PART('quarter', hired.datetime) = 2 THEN 1 ELSE 0 END) AS Q2,
        SUM(CASE WHEN DATE_PART('quarter', hired.datetime) = 3 THEN 1 ELSE 0 END) AS Q3,
        SUM(CASE WHEN DATE_PART('quarter', hired.datetime) = 4 THEN 1 ELSE 0 END) AS Q4
    FROM
        empleados.hired
    JOIN empleados.departments ON hired.department_id = departments.id
    JOIN empleados.jobs ON hired.job_id = jobs.id
    WHERE
        DATE_PART('year', hired.datetime) = 2021
    GROUP BY
        departments.department, jobs.jobs
    ORDER BY
        departments.department, jobs.jobs;
"""

GET_METRICA2 = """
     WITH Prom AS (
            SELECT
                AVG(cantidad)
            FROM(SELECT
                    hired.department_id,
                    COUNT(*) as cantidad
                    FROM empleados.hired
                WHERE DATE_PART('year', hired.datetime) = 2021 AND hired.department_id IS NOT NULL
                GROUP BY
                hired.department_id) Prom_SQ
        ),
        Metrica AS(
            SELECT
                departments.id AS IDEN,
                departments.department AS DEP,
                COUNT(*) AS cantidad
            FROM empleados.departments
            JOIN empleados.hired ON departments.id = hired.department_id
            WHERE
                DATE_PART('year', hired.datetime) = 2021
            GROUP BY
                departments.id, departments.department
            ORDER BY
                Cantidad DESC
        )
        SELECT
            Metrica.IDEN as id,
            Metrica.DEP as department,
            Metrica.cantidad as hired
        FROM Metrica
        WHERE Metrica.cantidad>(SELECT * FROM Prom)
"""

# Configuración para la carga de archivos
UPLOAD_FOLDER = 'uploads'
ALLOWED_EXTENSIONS = {'csv'}
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/upload', methods=['GET','POST'])
def upload_csv():
    connection = psycopg2.connect(URL)
    if request.method == 'POST':
        selected_database = request.form['database']
        if 'file' not in request.files:
            return redirect(request.url)
        
        file = request.files['file']
        
        if file and allowed_file(file.filename):
            filename = os.path.join(app.config['UPLOAD_FOLDER'],file.filename)
            file.save(filename)

        csv_data = pd.read_csv(filename,header=None,delimiter=',')

        #with open(filename, 'r') as csv_file:
        #    lines = csv_file.readlines()
            #batch_size = 1000
            #batches = [lines[i:i + batch_size] for i in range(0, len(lines), batch_size)]

        cursor = connection.cursor()
        
        if selected_database == 'database2':
            creacion = CREATE_DEPARTMENTS_TABLE
            cursor.execute(creacion)
            llenado = LLENADO_DEPARTMENTS
            for i in range(len(csv_data)):
                cursor.execute(llenado,(int(csv_data.iloc[i][0]),str(csv_data.iloc[i][1])))
            connection.commit() 
            cursor.execute("SELECT * FROM empleados.departments")
            print(cursor.fetchall())

        elif selected_database == 'database3':
            creacion = CREATE_JOBS_TABLE
            cursor.execute(creacion)
            llenado = LLENADO_JOBS
            for i in range(len(csv_data)):
                cursor.execute(llenado,(int(csv_data.iloc[i][0]),str(csv_data.iloc[i][1])))
            connection.commit() 
            cursor.execute("SELECT * FROM empleados.jobs")
            print(cursor.fetchall())
        else:
            creacion = CREATE_HIRED_TABLE
            cursor.execute(creacion)
            llenado = LLENADO_HIRED
            for i in range(len(csv_data)):
                data1 = int(csv_data.iloc[i][0])
                data2 = str(csv_data.iloc[i][1])
                data3 = csv_data.iloc[i][2]
                if pd.isna(data3): 
                    data3 = None 
                else: 
                    data3=str(data3)
                data4 = csv_data.iloc[i][3]
                if pd.isna(data4): 
                    data4 = None 
                else: 
                    data4=int(data4)
                data5 = csv_data.iloc[i][4]
                if pd.isna(data5): 
                    data5 = None
                else: 
                    data5=int(data5)

                #print(data1,data2,data3,data4,data5)

                cursor.execute(llenado,(data1,data2,data3,data4,data5))
            connection.commit() 
            cursor.execute("SELECT * FROM empleados.hired")
            print(cursor.fetchall())
    
        cursor.close()
        connection.close()

        return 'Archivo subido y procesado exitosamente'

    return render_template('upload.html')

@app.get('/consulta1/json')
def metrica1_json():
    metrica_json = []
    connection = psycopg2.connect(URL)
    cursor = connection.cursor()
    cursor.execute(GET_METRICA1)
    metricas = cursor.fetchall()

    for row in metricas:
        metrica_json.append({
            'dep': row[0],
            'job': row[1],
            'q1': row[2],
            'q2': row[3],
            'q3': row[4],
            'q4': row[5]
        })
    cursor.close()
    connection.close()
    
    return jsonify(metrica_json)

@app.route('/consulta1')
def consulta1():
    connection = psycopg2.connect(URL)
    cursor = connection.cursor()
    cursor.execute(GET_METRICA1)
    data = cursor.fetchall()

    formatted_data = [(dep, job, str(q1),str(q2),str(q3),str(q4)) for dep, job, q1, q2, q3, q4 in data]

    cursor.close()
    connection.close()

    return render_template('consulta1.html', data=formatted_data)

@app.get('/consulta2/json')
def metrica2_json():
    metrica_json = []
    connection = psycopg2.connect(URL)
    cursor = connection.cursor()
    cursor.execute(GET_METRICA2)
    metricas = cursor.fetchall()

    for row in metricas:
        metrica_json.append({
            'dep': row[0],
            'job': row[1],
            'q1': row[2]
        })
    cursor.close()
    connection.close()
    
    return jsonify(metrica_json)

@app.route('/consulta2')
def consulta2():
    connection = psycopg2.connect(URL)
    cursor = connection.cursor()

    cursor.execute(GET_METRICA2)
    data = cursor.fetchall()

    formatted_data = [(str(id), department, str(hired)) for id, department, hired in data]

    cursor.close()
    connection.close()

    return render_template('consulta2.html', data=formatted_data)

if __name__ == '__main__':
    app.run(debug=True)
