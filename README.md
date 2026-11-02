# TP05: Real-Time Data Streaming and Processing Using Apache Kafka

**Étudiante :** Houda Touahria  
**Niveau :** Master 2 - Intelligence Artificielle & Data Science  
**Module :** Big Data Analytics[cite: 1]  
**Université :** Université Hamma Lakhdar - El Oued[cite: 1]  
**Année Universitaire :** 2026/2027[cite: 1]  

---

## 1. Description du TP
Ce travail pratique consiste à concevoir et déployer un pipeline de streaming de données en temps réel pour la surveillance d'un campus intelligent[cite: 1]. Le système s'appuie sur **Apache Kafka** configuré en mode **KRaft** (sans ZooKeeper) et déployé via **Docker Compose**[cite: 1].

Le pipeline est constitué de :
- **Un Producer (Python) :** simule et transmet des mesures environnementales (température et humidité) sous format JSON vers le topic `campus-sensors`[cite: 1, 2].
- **Un Consumer (Python) :** consomme le flux en continu, extrait les métriques et déclenche instantanément une alerte lorsque la température atteint ou dépasse 35°C[cite: 1, 3].

---

## 2. Structure du Dépôt
```text
tp05-kafka/
├── compose.yaml
├── requirements.txt
├── producer.py
├── consumer.py
├── .gitignore
├── README.md
└── screenshots/
    ├── docker_compose_ps.png
    ├── topic_describe.png
    └── producer_consumer_flow.png
```[cite: 1]

---

## 3. Guide de Déploiement et d'Exécution

### Prérequis
- Docker Engine et Docker Compose[cite: 1].
- Python 3.10+ et l'environnement virtuel `.venv`[cite: 1, 2].

### Instructions pas-à-pas
1. **Démarrer le conteneur Kafka :**
   ```powershell
   docker compose up -d
   docker compose ps
   ```[cite: 2]

2. **Configurer l'environnement Python :**
   ```powershell
   python -m venv .venv
   Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
   .\.venv\Scripts\Activate.ps1
   pip install -r requirements.txt
   ```[cite: 2]

3. **Lancer le Consumer (dans un premier terminal) :**
   ```powershell
   python consumer.py
   ```[cite: 3]

4. **Lancer le Producer (dans un deuxième terminal) :**
   ```powershell
   python producer.py
   ```[cite: 3]

5. **Vérifier le Topic et les Partitions via la CLI Kafka :**
   ```powershell
   docker exec -it tp05-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic campus-sensors
   ```[cite: 3]

---

## 4. Réponses aux Questions du TP

### Q1. Quel est le problème résolu par Apache Kafka dans une architecture de streaming ?
Kafka résout les problèmes de dépendance directe (couplage fort) et de congestion inhérents aux architectures point-à-point classiques[cite: 3]. En tant que tampon distribué, persistant et tolérant aux pannes, il découple les producteurs des consommateurs, permettant l'ingestion de flux de données massifs à grande vitesse tout en offrant à chaque service la possibilité de lire les données à son propre rythme[cite: 3, 4].

### Q2. Définitions des concepts clés :
- **Producer :** Composant applicatif responsable de la publication et de l'envoi des messages vers un topic Kafka[cite: 5].
- **Consumer :** Application cliente qui souscrit à un topic pour lire et traiter les flux d'événements[cite: 5].
- **Broker :** Nœud serveur Kafka assurant le stockage, l'écriture sur disque et la diffusion des messages[cite: 5].
- **Topic :** Catégorie logique ou canal de destination auquel les messages sont adressés[cite: 5].
- **Partition :** Division physique ordonnée et immuable d'un topic permettant le parallélisme et la distribution[cite: 5].
- **Offset :** Indice numérique séquentiel marquant la position exacte de chaque message à l'intérieur d'une partition[cite: 5].
- **Consumer Group :** Groupe de consommateurs se partageant l'écoute des partitions d'un topic pour assurer une répartition de charge sans duplication[cite: 5].

### Q3. Quelle est la différence entre un Topic et une Partition ?
Le **Topic** représente une abstraction logique globale pour classer les flux de messages[cite: 5]. La **Partition** est l'unité physique de stockage sur disque[cite: 5]. L'ordre des messages n'est garanti de manière stricte qu'à l'échelle d'une partition, et non pas globalement sur l'ensemble du topic[cite: 5].

### Q4. Pourquoi utilise-t-on `auto_offset_reset='earliest'` et quand s'applique-t-il ?
Cette configuration garantit que le consommateur lit l'ensemble des événements archivés depuis le début du journal (l'offset le plus ancien)[cite: 3]. Ce mécanisme entre en action uniquement lorsque le groupe de consommateurs ne dispose d'aucun offset validé (première initialisation du groupe ou suppression des index d'offset)[cite: 5].

### Q5. Quel est le rôle du Consumer Group ID et que se passe-t-il si un nouvel identifiant est utilisé ?
Le `group_id` identifie le groupe auprès du broker afin de sauvegarder la progression de la lecture[cite: 3, 5]. L'utilisation d'un nouvel identifiant crée une nouvelle session indépendante : Kafka applique la règle `auto_offset_reset`, permettant au nouveau groupe de relire l'intégralité du flux sans impacter l'état des autres consommateurs[cite: 5].

### Q6. Pourquoi le format JSON est-il adapté à ce TP ? Citez une limitation.
- **Avantage :** Standard universel, lisible en clair et s'intégrant naturellement avec les dictionnaires Python[cite: 1].
- **Limitation :** Surcharge de bande passante due à la répétition textuelle des clés à chaque événement, ainsi que l'absence de vérification binaire du schéma (contrairement à Avro ou Protobuf).

### Q7. Comment Docker Compose simplifie-t-il le déploiement par rapport à une installation manuelle ?
Docker Compose centralise l'ensemble de l'infrastructure (images, mappage des ports, variables KRaft, volumes persistants) au sein d'un seul fichier `compose.yaml`[cite: 1, 2]. Il permet une mise en service reproductible, propre et immédiate en une seule commande (`docker compose up -d`), éliminant la complexité de l'installation manuelle de Java et Kafka sur le système hôte[cite: 2, 4].

### Q8. Quelles sont les limites d'un cluster à broker unique par rapport à un cluster de production ?
- **Point unique de défaillance (SPOF) :** En cas d'arrêt ou de plantage du conteneur, l'ensemble du service s'interrompt.
- **Absence de réplication des données (`replication-factor: 1`) :** Les partitions ne sont pas dupliquées sur des nœuds secondaires, ce qui présente un risque de perte de données en cas de panne matérielle[cite: 2, 5].
- **Scalabilité restreinte :** Les performances d'écriture et de lecture sont bornées aux ressources de la machine locale.

---

## 5. Problèmes Rencontrés et Solutions

1. **Erreur de validation YAML lors du lancement du conteneur :**
   - *Problème :* Docker Compose a rejeté la directive de volume avec l'erreur `services.kafka.volumes must be a array`.
   - *Solution :* Correction de la syntaxe YAML en ajoutant le tiret d'énumération `-` sous la clé `volumes` pour monter le volume persistant `kafka_data`[cite: 2].

2. **Politique d'exécution restreinte sur PowerShell :**
   - *Problème :* Blocage de l'activation de l'environnement virtuel `.venv` par la sécurité Windows.
   - *Solution :* Exécution de la commande `Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass` pour débloquer le script de session.

3. **Incompatibilité de `kafka-python` avec Python moderne :**
   - *Problème :* Déclenchement de l'exception `ModuleNotFoundError: No module named 'kafka.vendor.six.moves'`.
   - *Solution :* Remplacement de la dépendance obsolète par la version active et maintenue `kafka-python-ng==2.2.3` dans `requirements.txt`.
