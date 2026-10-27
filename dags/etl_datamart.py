from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values
from datetime import datetime
import pandas as pd
import numpy as np

@dag(
    schedule_interval='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['datamart']
)
def datamart_etl_pipeline():

    @task
    def extract_and_transform():
        # Rutas de los volúmenes mapeados en Docker
        daily_path = '/opt/airflow/data/data.csv'
        historical_path = '/opt/airflow/data/online_retail.csv'
        
        try:
            df_daily = pd.read_csv(daily_path, encoding='unicode_escape')
            df_hist = pd.read_csv(historical_path, encoding='unicode_escape')
        except FileNotFoundError:
            print("Faltan archivos CSV en la carpeta 'data/'. Asegúrate de tener data.csv y online_retail.csv.")
            return

        # 0. Unificación de fuentes (Histórico + Diario)
        # Renombramos las columnas del histórico para que coincidan con el diario
        df_hist = df_hist.rename(columns={
            'Invoice': 'InvoiceNo',
            'Customer ID': 'CustomerID',
            'Price': 'UnitPrice'
        })
        
        # Concatenamos ambos DataFrames
        df = pd.concat([df_hist, df_daily], ignore_index=True)
        
        # Eliminamos duplicados exactos producto del solapamiento de fechas entre ambas fuentes
        df = df.drop_duplicates(subset=['InvoiceNo', 'StockCode', 'InvoiceDate', 'Quantity'])

        # 1. Limpieza y estandarización
        df['CustomerID'] = df['CustomerID'].fillna('UNKNOWN').astype(str)
        df['StockCode'] = df['StockCode'].astype(str).str.upper().str.replace(' ', '')
        df['Description'] = df['Description'].astype(str).str.upper()
        
        # Estandarización de fecha a UTC
        df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate']).dt.tz_localize('UTC', ambiguous='NaT', nonexistent='NaT')
        
        # 2. Reglas de negocio
        # Rechazos: Precio <= 0
        rechazos = df[df['UnitPrice'] <= 0].copy()
        validos = df[df['UnitPrice'] > 0].copy()
        
        # Ventas: Cantidad > 0
        ventas = validos[validos['Quantity'] > 0].copy()
        ventas['GrossRevenue'] = ventas['Quantity'] * ventas['UnitPrice']
        
        # Devoluciones: Cantidad <= 0
        devoluciones = validos[validos['Quantity'] <= 0].copy()
        devoluciones['Quantity'] = devoluciones['Quantity'].abs()
        devoluciones['ReturnAmount'] = devoluciones['Quantity'] * devoluciones['UnitPrice']
        
        # 3. Dimensiones
        clientes = df[['CustomerID']].drop_duplicates().copy()
        clientes['CustomerType'] = np.where(clientes['CustomerID'] == 'UNKNOWN', 'Unknown', 'Registered')
        
        # Descripción canónica (primera aparición)
        productos = df[['StockCode', 'Description']].drop_duplicates(subset=['StockCode'], keep='first').copy()
        productos['Category'] = 'General'
        
        # 4. Guardar temporalmente para la carga (Staging)
        clientes.to_csv('/opt/airflow/data/stg_customers.csv', index=False)
        productos.to_csv('/opt/airflow/data/stg_products.csv', index=False)
        ventas.to_csv('/opt/airflow/data/stg_sales.csv', index=False)
        devoluciones.to_csv('/opt/airflow/data/stg_returns.csv', index=False)
        rechazos.to_csv('/opt/airflow/data/stg_rejections.csv', index=False)

    @task
    def load_dimensions():
        hook = PostgresHook(postgres_conn_id='POSTGRES_DW')
        
        # Cargar Clientes (Idempotente)
        clientes = pd.read_csv('/opt/airflow/data/stg_customers.csv')
        for _, row in clientes.iterrows():
            hook.run("""
                INSERT INTO dim_customers (customer_id, customer_type) 
                VALUES (%s, %s) ON CONFLICT (customer_id) DO NOTHING;
            """, parameters=(row['CustomerID'], row['CustomerType']))

        # Cargar Productos (Idempotente)
        productos = pd.read_csv('/opt/airflow/data/stg_products.csv')
        for _, row in productos.iterrows():
            hook.run("""
                INSERT INTO dim_products (product_code, canonical_description, category) 
                VALUES (%s, %s, %s) ON CONFLICT (product_code) DO NOTHING;
            """, parameters=(row['StockCode'], row['Description'], row['Category']))

    @task
    def load_facts():
        from psycopg2.extras import execute_values
        hook = PostgresHook(postgres_conn_id='POSTGRES_DW')
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        # Cargar Ventas en Bloque (Bulk Insert)
        ventas = pd.read_csv('/opt/airflow/data/stg_sales.csv')
        if not ventas.empty:
            # Seleccionar y ordenar las 8 columnas exactas que espera la base de datos
            ventas = ventas[['InvoiceNo', 'StockCode', 'CustomerID', 'InvoiceDate', 'Country', 'Quantity', 'UnitPrice', 'GrossRevenue']]
            ventas_tuples = [tuple(x) for x in ventas.to_numpy()]
            execute_values(cursor, """
                INSERT INTO fact_sales (invoice_no, product_code, customer_id, invoice_date, country, quantity, unit_price, gross_revenue) 
                VALUES %s 
                ON CONFLICT (invoice_no, product_code, invoice_date) DO NOTHING;
            """, ventas_tuples)

        # Cargar Devoluciones en Bloque (Bulk Insert)
        devoluciones = pd.read_csv('/opt/airflow/data/stg_returns.csv')
        if not devoluciones.empty:
            # Seleccionar y ordenar las 8 columnas exactas
            devoluciones = devoluciones[['InvoiceNo', 'StockCode', 'CustomerID', 'InvoiceDate', 'Country', 'Quantity', 'UnitPrice', 'ReturnAmount']]
            dev_tuples = [tuple(x) for x in devoluciones.to_numpy()]
            execute_values(cursor, """
                INSERT INTO fact_returns (invoice_no, product_code, customer_id, return_date, country, quantity, unit_price, return_amount) 
                VALUES %s 
                ON CONFLICT (invoice_no, product_code, return_date) DO NOTHING;
            """, dev_tuples)
            
        # Cargar Rechazos
        rechazos = pd.read_csv('/opt/airflow/data/stg_rejections.csv')
        for _, row in rechazos.iterrows():
            cursor.execute("""
                INSERT INTO log_rejections (source_file, record_data, rejection_reason) 
                VALUES (%s, %s, %s);
            """, ('data.csv', str(row.to_dict()), 'Precio unitario inválido (<= 0)'))

        conn.commit()
        cursor.close()

    # Definir orden de ejecución
    extract = extract_and_transform()
    dim_load = load_dimensions()
    fact_load = load_facts()

    extract >> dim_load >> fact_load

etl = datamart_etl_pipeline()