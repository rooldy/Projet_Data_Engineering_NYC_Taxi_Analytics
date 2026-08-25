# Fiches de modélisation — NYC Taxi Analytics
## Grain, clés, mesures — à valider avant toute implémentation

---

## 📌 Principe directeur

Chaque table est définie **avant** d'être codée : son grain (ce que représente une ligne), sa clé primaire, ses clés étrangères, ses mesures, et ses exclusions volontaires. Une fois validées, ces fiches ne changent plus pendant l'implémentation — si un besoin nouveau apparaît, on modifie la fiche d'abord, puis le code.

**Décision de fond adoptée** : les référentiels de codes statiques et immuables (types de paiement, fournisseurs, types de tarif) ne sont **pas** ingérés depuis une source externe — ils sont connus, stables, et publiés une fois pour toutes par la documentation NYC TLC. Ils seront donc gérés comme des **dbt seeds** (fichiers CSV versionnés dans le projet dbt), pas comme des tables RAW chargées par script Python. C'est la pratique standard pour ce type de référentiel : pas de pipeline d'ingestion pour une donnée qui ne bouge jamais.

---

## 🗂️ Vue d'ensemble du schéma cible

```
                    dim_date
                       |
dim_zones (pickup) ----+---- dim_zones (dropoff)
                       |
dim_vendor ------------+------- fct_trips ------- dim_payment_type
                       |
                  dim_rate_code
```

Modèle en étoile classique : `fct_trips` au centre, connectée à 6 axes d'analyse (dates, zones de départ, zones d'arrivée, vendeur, type de tarif, type de paiement).

---

## 1️⃣ Table de faits : `fct_trips`

**Grain** : une ligne = une course de taxi individuelle.

**Clé primaire** : `trip_id` — identifiant technique **séquentiel**, généré à l'ingestion (pas de surrogate key basé sur des colonnes métier, pour garantir l'unicité à 100 % même à grande échelle).

> Justification : le dataset NYC TLC ne fournit aucun identifiant natif de course. Une clé composite basée sur `pickup_datetime + do_location_id + fare_amount` présente un risque de collision non négligeable sur un volume de plusieurs millions de lignes par mois. Un identifiant technique séquentiel, généré au chargement RAW, élimine ce risque. Ce choix est documenté ici pour pouvoir être justifié tel quel en entretien.

**Clés étrangères** :

| Colonne | Référence | Type |
|---|---|---|
| `pickup_location_id` | `dim_zones.location_id` | Zone de départ |
| `dropoff_location_id` | `dim_zones.location_id` | Zone d'arrivée |
| `vendor_id` | `dim_vendor.vendor_id` | Fournisseur du système taxi |
| `rate_code_id` | `dim_rate_code.rate_code_id` | Type de tarif appliqué |
| `payment_type_id` | `dim_payment_type.payment_type_id` | Moyen de paiement |
| `pickup_date` | `dim_date.date_key` | Date de la course |

**Mesures** (colonnes numériques agrégeables) :

| Colonne | Description |
|---|---|
| `passenger_count` | Nombre de passagers déclaré |
| `trip_distance` | Distance parcourue (miles) |
| `trip_duration_minutes` | Durée calculée (dropoff - pickup) |
| `fare_amount` | Tarif de base |
| `extra` | Suppléments (heures de pointe, nuit) |
| `mta_tax` | Taxe MTA |
| `tip_amount` | Pourboire |
| `tolls_amount` | Péages |
| `improvement_surcharge` | Surcharge d'amélioration |
| `congestion_surcharge` | Surcharge de congestion |
| `airport_fee` | Frais aéroport |
| `total_amount` | Montant total (somme des composantes ci-dessus) |

**Attributs techniques** :

| Colonne | Description |
|---|---|
| `pickup_datetime` | Horodatage précis de départ (conservé en plus de `pickup_date` pour l'analyse infra-journalière) |
| `dropoff_datetime` | Horodatage précis d'arrivée |
| `store_and_fwd_flag` | Indicateur technique de transmission différée |

**Exclusions volontaires** :
- Aucune colonne descriptive de zone (`borough`, `zone` en clair) — ces informations restent exclusivement dans `dim_zones`, à joindre au moment de la restitution. `fct_trips` ne contient que des clés étrangères et des mesures, conformément au principe de modélisation en étoile.

**Matérialisation** : `incremental`, clé unique `trip_id`, filtrage sur `pickup_date` pour les runs incrémentaux.

---

## 2️⃣ Dimension : `dim_zones`

**Grain** : une ligne = une zone de taxi new-yorkaise.

**Clé primaire** : `location_id` (identifiant natif du référentiel NYC TLC, stable).

**Colonnes** :

| Colonne | Description |
|---|---|
| `location_id` | Identifiant unique de la zone |
| `borough` | Arrondissement (Manhattan, Brooklyn, Queens, Bronx, Staten Island, EWR, Unknown) |
| `zone` | Nom de la zone |
| `service_zone` | Type de zone de service (Boro Zone, Yellow Zone, Airports, etc.) |

**Source** : table RAW `RAW_TAXI_ZONES`, alimentée par ingestion Python (référentiel qui évolue occasionnellement — nouvelles zones, reclassifications — donc traité en pipeline, contrairement aux codes ci-dessous qui sont figés).

---

## 3️⃣ Dimension : `dim_date`

**Grain** : une ligne = un jour calendaire.

**Clé primaire** : `date_key`.

**Colonnes** :

| Colonne | Description |
|---|---|
| `date_key` | Date (clé) |
| `year`, `month`, `day` | Composantes calendaires |
| `day_of_week` | Jour de la semaine |
| `is_weekend` | Booléen |

**Source** : générée via `dbt_utils.date_spine`, aucune dépendance externe.

---

## 4️⃣ Dimension : `dim_payment_type` (seed)

**Grain** : une ligne = un type de paiement possible.

**Clé primaire** : `payment_type_id`.

**Contenu** (valeurs officielles NYC TLC, figées) :

| payment_type_id | payment_label |
|---|---|
| 0 | Flex Fare trip |
| 1 | Credit card |
| 2 | Cash |
| 3 | No charge |
| 4 | Dispute |
| 5 | Unknown |
| 6 | Voided trip |

**Source** : dbt seed (`seeds/dim_payment_type.csv`), pas de pipeline d'ingestion.

---

## 5️⃣ Dimension : `dim_vendor` (seed)

**Grain** : une ligne = un fournisseur de système de taxi.

**Clé primaire** : `vendor_id`.

**Contenu** (valeurs officielles NYC TLC) :

| vendor_id | vendor_name |
|---|---|
| 1 | Creative Mobile Technologies, LLC |
| 2 | Curb Mobility, LLC |
| 6 | Myle Technologies Inc |
| 7 | Helix |

**Source** : dbt seed (`seeds/dim_vendor.csv`).

---

## 6️⃣ Dimension : `dim_rate_code` (seed)

**Grain** : une ligne = un type de tarif.

**Clé primaire** : `rate_code_id`.

**Contenu** (valeurs officielles NYC TLC) :

| rate_code_id | rate_code_label |
|---|---|
| 1 | Standard rate |
| 2 | JFK |
| 3 | Newark |
| 4 | Nassau or Westchester |
| 5 | Negotiated fare |
| 6 | Group ride |
| 99 | Unknown / Null |

**Source** : dbt seed (`seeds/dim_rate_code.csv`).

---

## 🔄 Impact sur l'ingestion RAW (Phase A à refaire)

La table `RAW_TAXI_TRIPS` doit être recréée pour inclure toutes les colonnes sources, y compris celles actuellement ignorées :

```sql
CREATE OR REPLACE TABLE NYC_TAXI_ANALYTICS_DEV.RAW.RAW_TAXI_TRIPS (
    trip_id NUMBER AUTOINCREMENT START 1 INCREMENT 1,  -- identifiant technique, garantit l'unicite
    vendor_id INT,
    tpep_pickup_datetime TIMESTAMP,
    tpep_dropoff_datetime TIMESTAMP,
    passenger_count INT,
    trip_distance FLOAT,
    rate_code_id INT,
    store_and_fwd_flag STRING,
    pu_location_id INT,
    do_location_id INT,
    payment_type INT,
    fare_amount FLOAT,
    extra FLOAT,
    mta_tax FLOAT,
    tip_amount FLOAT,
    tolls_amount FLOAT,
    improvement_surcharge FLOAT,
    congestion_surcharge FLOAT,
    airport_fee FLOAT,
    total_amount FLOAT,
    _loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    _source_file STRING
);
```

`trip_id` en `AUTOINCREMENT` répond directement à la problématique de clé de grain identifiée plus tôt — l'unicité est garantie nativement par Snowflake, dès la couche RAW.

---

## 🧪 Couverture de tests cible (rappel du cadrage précédent)

| Modèle | Tests minimum prévus |
|---|---|
| `dim_zones` | `unique` + `not_null` sur `location_id` |
| `dim_date` | `unique` + `not_null` sur `date_key` |
| `dim_payment_type` | `unique` + `not_null` sur `payment_type_id` |
| `dim_vendor` | `unique` + `not_null` sur `vendor_id` |
| `dim_rate_code` | `unique` + `not_null` sur `rate_code_id` |
| `fct_trips` | `unique` + `not_null` sur `trip_id` ; `relationships` vers chacune des 6 dimensions ; `not_null` sur `total_amount`, `fare_amount` |
| `stg_taxi_trips` | `not_null` sur les colonnes déjà couvertes, + `not_null` sur `vendor_id`, `rate_code_id` |
| `mart_daily_revenue` | `unique` sur `pickup_date` |

**Total estimé une fois complété : ~18-20 tests.**

---

## ✅ Validation requise avant implémentation

- [ ] Grain de `fct_trips` validé (identifiant technique séquentiel)
- [ ] Les 3 nouvelles dimensions seed (`dim_payment_type`, `dim_vendor`, `dim_rate_code`) validées
- [ ] Le fait que `fct_trips` ne contienne aucun attribut descriptif de zone est validé
- [ ] La liste des colonnes RAW complètes est validée

Une fois ces points cochés, on passe à l'implémentation (Phase A : ingestion, Phase B : modèles dbt), sans revenir sur ces décisions en cours de route.
