# Pipeline-ETL-automatis-avec-Apache-Airflow-PostgreSQL
Ce projet consiste à construire un pipeline ETL automatisé en utilisant Apache Airflow, afin d’extraire, transformer et charger des données dans une base PostgreSQL.


🚀 Objectif du projet

Le but de ce pipeline est de :

Télécharger automatiquement un dataset depuis une source publique

Nettoyer et transformer les données avec Python (Pandas)

Charger les données nettoyées dans une base PostgreSQL

Planifier et orchestrer l’ensemble grâce à Apache Airflow

Vérifier la qualité du chargement via une étape de validation

Ce pipeline tourne tous les jours automatiquement grâce au scheduler d’Airflow.


🏗️ Architecture du pipeline

Voici un aperçu simple du workflow :
      Source CSV (GitHub)
               ↓
         Airflow DAG
     1. extract_data            
     2. transform_data          
     3. load_data               
     4. validate_load  
               ↓
         Base PostgreSQL

         
🧰 Technologies utilisées

Apache Airflow : orchestration du pipeline

Python : extraction, nettoyage et transformation

Pandas : manipulation du dataset

PostgreSQL : stockage des données transformées

Docker & Docker Compose : environnement reproductible

Airflow Web UI : monitoring et gestion du DAG


