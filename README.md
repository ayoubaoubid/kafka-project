# 🚀 Kafka Fraud Detection System

# Kafka Fraud Detection System 🚀

Projet de détection de fraude en temps réel avec **Apache Kafka**, **Machine Learning**, **Docker**, et **HDFS**.

---

# 📌 Description

Ce projet simule des transactions e-commerce en streaming avec Kafka, puis utilise un modèle de Machine Learning pour détecter les transactions frauduleuses en temps réel.

Les données sont :
- Produites par un producer Kafka
- Consommées par plusieurs consumers
- Analysées par un modèle IA
- Stockées dans HDFS
- Visualisées via des rapports générés automatiquement

---

# 🏗️ Architecture du Projet

```bash
KAFKA_PROJECT/
│
├── config/
│   └── hadoop.env
│
├── consumers/
│   ├── consumer_hdfs.py
│   ├── consumer_stats.py
│   ├── fraud_detector.py
│   ├── train_fraud_model.py
│   ├── visualize_fraud_results.py
│   ├── requirements.txt
│   └── Dockerfile
│
├── producer/
│   ├── producer.py
│   ├── requirements.txt
│   └── Dockerfile
│
├── data/
│   └── ecommerce.csv
│
├── models/
│   └── fraud_detection_model.pkl
│
├── reports/
│   ├── dataset_overview.json
│   ├── fraud_detection_report.png
│   ├── model_summary.json
│   ├── preprocessing_report.json
│   ├── top_suspicious_transactions.csv
│   └── training_results.csv
│
├── docker-compose.yml
├── .gitignore
└── README.md
```

---

# ⚙️ Technologies Utilisées

- Python 3.11
- Apache Kafka
- Apache Spark / PySpark
- Zookeeper
- Docker & Docker Compose
- Pandas
- Scikit-learn
- Matplotlib
- HDFS
- Joblib

---
# 🚀 Kafka Fraud Detection Project (Spark + ML)

Ce projet détecte des fraudes à partir de données e-commerce en utilisant :
- Apache Spark (data processing)
- Scikit-learn (modèle Isolation Forest)
- Kafka (streaming pipeline)
- Pandas (analyse & reporting)

---

# 🧠 IMPORTANT

Deux façons d’exécuter le projet :

1. 🐳 MODE DOCKER (RECOMMANDÉ)
2. 🖥️ MODE LOCAL (installation Python + Java)



---

# 🐳 1. MODE DOCKER (RECOMMANDÉ)

## 📦 Avantages
- Aucun Python à installer
- Aucun Java à installer
- Environnement identique pour tous
- Zéro problème de version

---
## Cloner le projet

### Linux / Mac

```bash
git clone https://github.com/ayoubaoubid/kafka-project.git
cd kafka-project
```

### Windows (PowerShell)

```powershell
git clone https://github.com/ayoubaoubid/kafka-project.git
cd kafka-project
```

---

# 🐳 Vérifier Docker

### Linux / Mac

```bash
docker --version
docker compose version
```

### Windows

```powershell
docker --version
docker compose version
```

---

## 🚀 Lancer le projet

```bash
docker compose up -d --build
```

### Windows

```powershell
docker compose up -d --build
```

Le modèle sera sauvegardé dans :

```
models/fraud_detection_pipeline.pkl
```
Les résultats seront générés dans le dossier :

```
reports/
```

---
## 📊 Vérifier les containers

```bash
docker ps
```

### Windows

```powershell
docker ps
```

---
## 🛑 Stopper le projet

```bash
docker compose down
```
### Windows

```powershell
docker compose down
```

---

## Voir les logs

### Linux / Mac

```bash
docker compose logs -f
```

### Windows

```powershell
docker compose logs -f
```

---

# 🚀 Streaming Kafka - Test Manuel

## Ouvrir un producer Kafka

### Linux / Mac

```bash
docker compose exec kafka kafka-console-producer \
--broker-list kafka:29092 \
--topic ecommerce-orders
```

### Windows PowerShell

```powershell
docker compose exec kafka kafka-console-producer --broker-list kafka:29092 --topic ecommerce-orders
```

---

## Envoyer une transaction normale

```json
{"InvoiceNo":"TESTNORMAL001","StockCode":"ITEM01","Description":"NORMAL ORDER","Quantity":2,"InvoiceDate":"12/1/2010 10:30","UnitPrice":5.99,"CustomerID":"20001","Country":"France"}
```

---

## Envoyer une transaction frauduleuse

```json
{"InvoiceNo":"TESTFRAUD001","StockCode":"LUXE01","Description":"VERY HIGH VALUE ORDER","Quantity":500,"InvoiceDate":"12/1/2010 23:59","UnitPrice":999.99,"CustomerID":"99999","Country":"Brazil"}
```

---

## Quitter le producer

### Linux / Mac

```bash
CTRL + C
```

### Windows

```powershell
CTRL + C
```

---

# 🔍 Vérifier les Logs du Détecteur

## Vérifier la transaction normale

### Linux / Mac

```bash
docker compose logs fraud-detector | grep TESTNORMAL001
```

### Windows PowerShell

```powershell
docker compose logs fraud-detector | Select-String "TESTNORMAL001"
```

---

## Vérifier la transaction frauduleuse

### Linux / Mac

```bash
docker compose logs fraud-detector | grep TESTFRAUD001
```

### Windows PowerShell

```powershell
docker compose logs fraud-detector | Select-String "TESTFRAUD001"
```

---


# 📈 Résultats

Le projet génère :
- Détection des fraudes
- Statistiques temps réel
- Graphiques
- Transactions suspectes
- Rapports JSON/CSV

---

# 👨‍💻 Auteur

Projet réalisé par : Groupe 3

# git

### Linux / Mac

```bash
git checkout -b feature/nouvelle-feature
git commit -m "Ajout d'une nouvelle feature"
git push origin feature/nouvelle-feature
```

### Windows

```powershell
git checkout -b feature/nouvelle-feature
git commit -m "Ajout d'une nouvelle feature"
git push origin feature/nouvelle-feature
```

---

# 📜 Licence

Ce projet est sous licence MIT.
