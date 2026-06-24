# Bienvenue sur Make Open Data

### Des données publiques exploitables déployées sur une BDD Postgres/PostGIS accessibles depuis l'outil de votre choix.

- Présentation du projet ou contactez-nous pour une démo : https://make-open-data.fr/
- Catalogue des données : https://data.make-open-data.fr/

## Qu'est-ce que Make Open Data ?

Make Open Data est un ELT Open Source pour les données publiques :

- **Extrait** les fichiers sources (data.gouv, INSEE, Etalab, etc.) les plus adaptés et récents.
- **Transforme** ces données selon des règles transparentes et le moins irréversibles possible.
- **Stocke** ces données dans une base de données PostgreSQL (avec PostGIS).
- **Teste** des hypothèses sur ces données (ex. : prix par transaction immobilière sur DVF).

<img src="assets/make-open-data-flow.png" width="600">

Les données spatiales sont intégrables dans QGIS et autres SIG.

<img src="assets/demo-qgis.png" width="600">

---

## Déploiement managé par Make Open Data

Nous fournissons les accès à une base PostgreSQL dans le cloud avec des données à jour.

Contactez-nous : https://make-open-data.fr/

---

## Déploiement manuel

### 1. Installation des outils nécessaires

Mettre à jour les paquets et installer les dépendances :

```sh
sudo apt update
sudo apt install git python3-venv postgresql postgis
```

### 2. Cloner le projet et configurer l'environnement virtuel

```sh
git clone git@github.com:<utilisateur_orga_destination>/<nom_repo_destination>.git
cd make-open-data-EPF
python3 -m venv dbt-env
source dbt-env/bin/activate
```

### 3. Configuration de PostgreSQL

Définir les variables d'environnement :

```sh
nano dbt-env/env.sh
```

Ajouter :

```sh
export POSTGRES_USER=<YOUR_POSTGRES_USER> # ex: postgres
export POSTGRES_PASSWORD=<YOUR_POSTGRES_PASSWORD>
export POSTGRES_HOST=<YOUR_POSTGRES_HOST> # ex: localhost
export POSTGRES_PORT=<YOUR_POSTGRES_PORT> # ex: 5432
export POSTGRES_DB=<YOUR_POSTGRES_DB> # ex: postgres
```

Appliquer les changements :

```sh
source dbt-env/env.sh
```

Modifier la configuration PostgreSQL :

```sh
sudo nano /etc/postgresql/XX/main/postgresql.conf
```

Changer :

```sh
#listen_addresses = 'localhost'
```

Par :

```sh
listen_addresses = '*'
```

Redémarrer PostgreSQL :

```sh
sudo systemctl restart postgresql.service
```

### 4. Activer PostGIS

Se connecter à PostgreSQL et activer l'extension :

```sh
psql postgresql://$POSTGRES_USER:$POSTGRES_PASSWORD@$POSTGRES_HOST:$POSTGRES_PORT/$POSTGRES_DB
CREATE EXTENSION postgis;
\q
```

### 5. Chargement des données

```sh
python3 -m load  # Chargement des données d'exemple
python3 -m load --production  # Chargement complet des données
```

### 6. Configuration et exécution de DBT

```sh
export DBT_PROFILES_DIR=.
dbt debug
dbt deps
dbt seed
dbt run --target dev  # Tables logement sur Occitanie et DVF Hérault
```

Pour une exécution complète en production :

```sh
dbt run --target production  # Environ 1 heure
```

Tester les transformations :

```sh
dbt test
```

### 7. Installation de pgAdmin4 (facultatif)

```sh
sudo apt install pgadmin4
```