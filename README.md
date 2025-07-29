# MS Fabric stockanaliser project
### General descritpion
Built natively on MS Fabric, the project concentrates on data-engineering aspects. Deliverables include:

  1. Real-time analytical report assessing the volumes of listed shares.
  
  2. Two Reports designed to provide information of the stock environment (data are ingested hourly).

Synthetic (randomized) market-quote events are created by local Python scrypt, ingested via Eventstream into Eventhouse. Eventstream also load data to Lakehouse bronze tier. Orchestration is handled by Data Pipelines that execute PySpark notebooks which load silver and gold tier. Final outputs are exposed in a Reports (Lakehouse source) and an Real-Time Dashboard (Eventhouse source).


### Project Architecture

<img width="1919" height="862" alt="image" src="https://github.com/user-attachments/assets/45699364-238a-4db9-8d67-ac64892b105a" />
<img width="1829" height="671" alt="image" src="https://github.com/user-attachments/assets/f863a076-3603-46dd-8ea6-6dca5363126f" />


  # 1. Python script - data generation
Script gen_stockanaliser.py generates date JSON every 5 second. Using azure.eventhub script uplod this data to Eventstream in MS Fabric.
  <img width="500" height="370" alt="py_script" src="https://github.com/user-attachments/assets/58521ca9-773b-4b4d-a8e2-70882c63c2dc" />

It is important to provide correct EVENTHUB_NAME and CONNECTION_STR to script. Theses can be found in Eventstream Live mode in Source details:

<img width="925" height="343" alt="image" src="https://github.com/user-attachments/assets/fec4dbb7-325b-4d45-9316-a78415edb0fc" />
<img width="925" height="343" alt="eventstream_conf" src="https://github.com/user-attachments/assets/2a2b8e44-dcc5-47d9-b26e-34626c1f2734" />

  # 2. Eventstream – ingest and data distribution

  Eventstream load data to Eventhouse and bronze Lakehouse table tbl_bronze.
<img width="1108" height="432" alt="eventstrean_graph" src="https://github.com/user-attachments/assets/46a9b005-87cb-464c-8227-69b8055fd4f8" />

  
  # 3. Eventhouse and Real-Time Report rt_ds_volumen_of_last_5minutes
Data lands in Eventhouse (KQL Database) eh_stockanaliser table tbl_eventtable:
<img width="1488" height="568" alt="image" src="https://github.com/user-attachments/assets/9f50f05e-0429-45fd-9448-9b9ae043d07d" />

Real-Time Report base on KQL table using following query which shows agregated data from 5 last minutes:
<img width="678" height="484" alt="image" src="https://github.com/user-attachments/assets/68a6c62e-f9e7-4ff7-92ad-5b9a7c47a405" />

The reports are a simple tabular visualization, but the information they contain allows to get an idea of the situation on the stock exchange.
<img width="730" height="369" alt="image" src="https://github.com/user-attachments/assets/132e93d1-f819-4b97-9ca6-dc0d99d69c92" />

  # 4. Data orchestration – Data pipelines step 1
Data pipeline dpl_load_medalion includes 3 Notebooks. Every notebook are loading and transforming data to different stages.
  <img width="838" height="319" alt="image" src="https://github.com/user-attachments/assets/c28b2f8d-024a-4801-b28e-1c09ca5c9c31" />

Step 1 described in this section is responsible for loading data from tbl_bronze into tbl_silver_part and tbl_silver_wrong_currency. Step 1 runs Notebook **notebooks/nb_load_silver_part.ipynb**.
The CDF feature is set on the tbl_bronze table. Script uses CDF and helping table cdf_control_silver_part to insert only new rows into tbl_silver_part and tbl_silver_wrong_currency table.

```
ctrl = spark.table("cdf_control_silver_part").filter("table_name = 'tbl_bronze'").first()
last_version = ctrl["last_version"]     #946

# find current table version
current_version = spark.sql("DESCRIBE HISTORY tbl_bronze")\
                       .selectExpr("max(version) as v")\
                       .first()["v"]

if current_version > last_version:

    # get changes and add partition_date column
    changes_df = spark.sql(f"""
    SELECT 
            timestamp,            symbol,            exchange,            event_type,            latency_ms,            order_id,            transaction_type,            price,            volume,            bid_price,            ask_price,            bid_size,            ask_size,            canceled_order_id,            currency,            trade_id,            event_id,            EventProcessedUtcTime,            PartitionId,            EventEnqueuedUtcTime   
    FROM table_changes('tbl_bronze', {last_version})
    WHERE _change_type = 'insert'
    """)
```

Script adds partition_date column which is DATA format, this column is used as partition key in tbl_silver_part and tbl_silver_wrong_currency.

```
    with_date = changes_df \
    .withColumn("partition_date", to_date(col("timestamp"))) \
    .cache()                   # <<< cacheujemy wynik
```
<img width="871" height="378" alt="image" src="https://github.com/user-attachments/assets/39403592-4051-4619-b40b-c9d3631b1e63" />

  # 5. Data orchestration – Data pipelines step 2
<img width="820" height="346" alt="image" src="https://github.com/user-attachments/assets/3dfc40ce-a25e-466e-a70d-c0b8aa48060f" />

Notebook step 2 **notebooks/nb_load_gold_tables_wrong_volume_per_day.ipynb** zasila danymi tabelę tbl_gold_wrong_volume_per_day. Dane w tej tabeli przedstawiają zgrupowany dla symbolu, typu transakcji oraz dnia zsumowany wolumen w milionach EUR. Ze względu na wydajność przetwarzane z danymi dzisiejszymi:
```
         .filter((col("event_type") == "TRADE") & (col("partition_date") == current_date()))
```
a dane które są ładowane do tabeli docelowej są nadpisywane tylko jeśli wartość w partition_date to data dzisiejsza:
```
    .option("replaceWhere", "partition_date = current_date()")
```
<img width="547" height="468" alt="image" src="https://github.com/user-attachments/assets/548b260d-43b5-4432-9903-5f48e7e5687b" />
 
  # 6. Data orchestration – Data pipelines step 3
<img width="559" height="638" alt="image" src="https://github.com/user-attachments/assets/762a162a-fd0e-4365-88d5-9ab2e3e833e8" />

Step 3 **notebooks/nb_load_gold_tables_amount_per_event_type.ipynb** ładuje dane do tabeli tbl_gold_amount_per_event_type, która jest partycjonowana. Dane pobierane są z tabeli tbl_silver_part za pomocą powyższego zapytania. Dane wyjściowe mają określać ilość zdarzeń w notowaniach danej spółki danego dnia.

<img width="592" height="447" alt="image" src="https://github.com/user-attachments/assets/04eb49e7-517f-4c12-8fc6-1984ea150013" />

  # 7. Report rp_gold_amount_per_event_type
<img width="596" height="516" alt="image" src="https://github.com/user-attachments/assets/76700dc3-ff2c-4351-889b-535c4b4747f6" />

Raport w prosty sposób przedstawia tabelę tbl_gold_amount_per_event_type.
  
  ### Scheduling

Data pipeline dpl_load_medalion jest uruchamiany w harmonogramie co godzinę. Dane do Eventstream spłwają i są zapisywane w tabeli tbl_bronze w sposób ciągły.

  ### Summary
Podsumowując w projekcie zostały użyte items MS Fabric do zbudowania przepływu danych od przyjęcia ich w trybie Stream przez Eventstream, przetrzymywanie ich w tier bronze. Następnie poprzez manipulację danych w obrębie Lakehouse dane zostały przetworzone w PySpark. Dziek harmonogramom przetwarzanie danych uruchamiane jest co określony okres czasu, a prezentowane dane są aktualne. Wyniki końcowe zaprezentowane są jako raporty, choć proste to niosące wiele informacji o obecnym rynku notowań giełdowych. Użycie mechanizmu Change Data Feed umożliwia ładowanie danych, które jeszcze nie zostały załadowane z tier bronze do tier silver.
