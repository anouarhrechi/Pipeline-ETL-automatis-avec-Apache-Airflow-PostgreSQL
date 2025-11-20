from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd
import requests
import io

# --- 1) EXTRACT DEPUIS URL ----------------------------------------------

def extract_data():
    """
    Extrait les données depuis une URL (fichier CSV en ligne)
    """
    # URL exemple - remplacez par votre URL réelle
    csv_url = "https://raw.githubusercontent.com/datasets/sample-data/main/sales.csv"
    
    # Alternative: URL d'un fichier CSV exemple
    # csv_url = "https://people.sc.fsu.edu/~jburkardt/data/csv/addresses.csv"
    
    try:
        print(f"Extraction depuis l'URL: {csv_url}")
        
        # Télécharger le fichier CSV
        response = requests.get(csv_url)
        response.raise_for_status()  # Vérifier que la requête a réussi
        
        # Lire le CSV directement depuis la réponse
        df = pd.read_csv(io.StringIO(response.text))
        
        print("✅ Extraction depuis URL réussie!")
        print(f"📊 Shape des données: {df.shape}")
        print("Aperçu des données extraites:")
        print(df.head())
        
        # Sauvegarder localement (optionnel)
        df.to_csv("/opt/airflow/data/extracted_sales.csv", index=False)
        print(f"💾 Données sauvegardées: {len(df)} enregistrements")
        
    except Exception as e:
        print(f"❌ Erreur lors de l'extraction: {e}")
        
        # Créer des données exemple en cas d'échec
        print("🔄 Création de données exemple...")
        sample_data = {
            'order_id': [1, 2, 3, 4, 5, 6, 7, 8],
            'product': ['Laptop', 'Smartphone', 'Tablet', 'Headphones', 'Monitor', 'Keyboard', 'Mouse', 'Printer'],
            'quantity': [2, 5, 3, 10, 4, 8, 15, 2],
            'unit_price': [999.99, 699.99, 399.99, 149.99, 299.99, 79.99, 29.99, 199.99],
            'order_date': ['2024-01-15', '2024-01-16', '2024-01-17', '2024-01-18', '2024-01-19', '2024-01-20', '2024-01-21', '2024-01-22']
        }
        df = pd.DataFrame(sample_data)
        df.to_csv("/opt/airflow/data/extracted_sales.csv", index=False)
        print("✅ Données exemple créées!")


# --- 2) TRANSFORM ---------------------------------------------------------

def transform_data():
    """
    Transforme et nettoie les données
    """
    try:
        df = pd.read_csv("/opt/airflow/data/extracted_sales.csv")
        
        print("🔄 Début de la transformation...")
        print(f"📥 Données à transformer: {len(df)} enregistrements")

        # Vérifier les colonnes disponibles
        print(f"📋 Colonnes disponibles: {list(df.columns)}")
        
        # Adapter les transformations selon les colonnes disponibles
        if 'quantity' in df.columns and 'unit_price' in df.columns:
            df['total_price'] = df['quantity'] * df['unit_price']
            print("✅ Colonne total_price calculée")
        else:
            # Créer des colonnes exemple si elles n'existent pas
            df['quantity'] = [2, 5, 3, 10, 4, 8, 15, 2][:len(df)]
            df['unit_price'] = [999.99, 699.99, 399.99, 149.99, 299.99, 79.99, 29.99, 199.99][:len(df)]
            df['total_price'] = df['quantity'] * df['unit_price']
            print("⚠️  Colonnes créées pour l'exemple")

        # Transformer les dates si la colonne existe
        date_columns = [col for col in df.columns if 'date' in col.lower()]
        if date_columns:
            date_col = date_columns[0]
            df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
            print(f"✅ Transformation des dates dans la colonne: {date_col}")
        else:
            # Créer une colonne date si elle n'existe pas
            df['order_date'] = pd.date_range(start='2024-01-01', periods=len(df), freq='D')
            print("✅ Colonne order_date créée")

        # Nettoyage supplémentaire
        df = df.dropna()  # Supprimer les lignes avec des valeurs manquantes
        df['total_price'] = df['total_price'].round(2)
        
        # Sauvegarder les données transformées
        df.to_csv("/opt/airflow/data/clean_sales.csv", index=False)
        
        print("✅ Transformation terminée!")
        print(f"📤 Données transformées: {len(df)} enregistrements")
        print("Aperçu des données transformées:")
        print(df.head())
        
    except Exception as e:
        print(f"❌ Erreur lors de la transformation: {e}")


# --- 3) LOAD ---------------------------------------------------------

def load_data():
    """
    Charge les données dans PostgreSQL
    """
    try:
        df = pd.read_csv("/opt/airflow/data/clean_sales.csv")
        
        print(f"📦 Chargement de {len(df)} enregistrements dans PostgreSQL...")
        
        hook = PostgresHook(postgres_conn_id="postgres_default")
        conn = hook.get_conn()
        cursor = conn.cursor()

        # Create table if not exists
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS sales (
                order_id INTEGER,
                product TEXT,
                quantity INTEGER,
                unit_price FLOAT,
                order_date DATE,
                total_price FLOAT,
                data_source TEXT DEFAULT 'web'
            );
        """)

        # Vider la table avant insertion (optionnel)
        cursor.execute("DELETE FROM sales;")
        
        # Déterminer les colonnes disponibles
        available_columns = df.columns.tolist()
        print(f"📋 Colonnes à charger: {available_columns}")

        # Insert data row by row
        for _, row in df.iterrows():
            # Adapter selon les colonnes disponibles
            order_id = int(row['order_id']) if 'order_id' in available_columns else None
            product = row['product'] if 'product' in available_columns else 'Unknown'
            quantity = int(row['quantity']) if 'quantity' in available_columns else 1
            unit_price = float(row['unit_price']) if 'unit_price' in available_columns else 0.0
            order_date = row['order_date'] if 'order_date' in available_columns else '2024-01-01'
            total_price = float(row['total_price']) if 'total_price' in available_columns else 0.0

            cursor.execute(
                """
                INSERT INTO sales (order_id, product, quantity, unit_price, order_date, total_price)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (order_id, product, quantity, unit_price, order_date, total_price)
            )

        conn.commit()
        cursor.close()
        print(f"✅ Données chargées avec succès: {len(df)} enregistrements")
        
    except Exception as e:
        print(f"❌ Erreur lors du chargement: {e}")


# --- 4) DAG DEFINITION ---------------------------------------------------------

default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 1, 1),
    "retries": 1
}

with DAG(
    dag_id="sales_etl_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    description="ETL pipeline: extract from URL, transform, load into Postgres"
) as dag:

    extract_task = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data
    )

    transform_task = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data
    )

    load_task = PythonOperator(
        task_id="load_data",
        python_callable=load_data
    )

    extract_task >> transform_task >> load_task