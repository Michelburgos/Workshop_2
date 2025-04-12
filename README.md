# Workshop_2


Este proyecto ETL recopila, transforma y almacena datos de artistas musicales desde tres fuentes principales: Spotify (CSV), los Premios Grammy (base de datos PostgreSQL) y Wikidata (API SPARQL). El objetivo es generar un dataset unificado y limpio para análisis posteriores mediante herramientas como Power BI o Python.

## 🧰 Estructura del Proyecto

```
ETL-Musical/
├── data/
│   ├── artists.csv
│   ├── spotify_dataset.csv
│   └── outputs/ (archivos transformados y merged.csv)
├── source/
│   ├── extract/
│   │   ├── extract_spotify.py
│   │   ├── extract_grammy.py
│   │   └── extract_wikidata.py
│   ├── transform/
│   │   ├── transform_spotify.py
│   │   ├── transform_grammy.py
│   │   └── transform_api.py
│   ├── load/
│   │   ├── load.py
│   │   └── store.py
│   └── BD_connection
│       
├── dag/
│   └── etl_dag.py
├── notebooks/
│   └── (EDA y análisis exploratorio)
├── .env
├── requirements.txt
└── README.md
```

## 📦 Requisitos

- Python 3.8+
- PostgreSQL
- Apache Airflow
- Acceso a Google Drive (credenciales OAuth 2.0)
- Archivo `.env` con las credenciales

## ⚙️ Instalación

1. **Clona este repositorio**
   ```bash
   git clone https://github.com/tu_usuario/ETL-Musical.git](https://github.com/Michelburgos/Workshop_2.git
   cd Workshop_2
   ```

2. **Crea y activa un entorno virtual**
   ```bash
   python3 -m venv venv
   source venv/bin/activate  # En Windows: venv\Scripts\activate
   ```

3. **Instala las dependencias**
   ```bash
   pip install -r requirements.txt
   ```

4. **Instala y configura PostgreSQL**
   - Instala:  
     ```bash
     sudo apt update
     sudo apt install postgresql -y
     ```
   - Crea usuario y base de datos:
     ```sql
     CREATE USER postgres WITH PASSWORD 'pg';
     ALTER USER postgres CREATEDB;
     createdb -U postgres gra
     createdb -U postgres gra_dimensional
     ```

5. **Configura el archivo `.env`**
   ```
   PG_USER=<tu_usuario>
   PG_PASSWORD=<tu_contraseña>
   PG_HOST=localhost
   PG_PORT=5432
   PG_DATABASE=<la_db_de_grammys>
   PG_DATABASE_DIMENSIONAL=<la_db_de_merge>
   GOOGLE_CREDENTIALS_PATH=credenciales.json
   GOOGLE_DRIVE_FOLDER_ID=tu_folder_id
   ```

## 🚀 Cómo ejecutar el ETL

1. **Inicializa Airflow**
   ```bash
   export AIRFLOW_HOME=~/airflow
   airflow db init
   ```

2. **Crea el DAG**
   Copia `etl_dag.py` a `~/airflow/dags/`.

3. **Ejecuta el scheduler y la web UI**
   ```bash
   airflow scheduler
   airflow webserver --port 8080
   ```

4. Accede a: `http://localhost:8080`  
   Activa y ejecuta el DAG `etl_musical_dag`.

---

## 📊 Salida del Proyecto

- Archivo final: `merged.csv`
- Base de datos `merged_db` con los datos cargados.
- Archivos exportados a Google Drive automáticamente.
- Análisis exploratorio disponible en `notebooks/`.

---

## 🧑‍💻 Autor

**Michel Dahiana Burgos Santos**  
Proyecto académico de Ingeniería de Datos e Inteligencia Artificial

