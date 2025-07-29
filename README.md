# MS Fabric stockanaliser project
### General description
The solution is built natively on Microsoft Fabric and focuses on robust data-engineering pipelines that simulate, ingest, transform, and visualise stock-exchange data.

  1. Real-Time Volume Dashboard – live analysis of traded share volumes.
  
  2. Two Hourly Snapshot Reports – contextual metrics on the trading environment, refreshed every hour.

Synthetic (randomized) market-quote events are created by local Python scrypt, ingested via Eventstream into Eventhouse. Eventstream also load data to Lakehouse bronze tier. Orchestration is handled by Data Pipelines that execute PySpark notebooks which load silver and gold tier. Final outputs are exposed in a Reports (Lakehouse source) and an Real-Time Dashboard (Eventhouse source).


### Project Architecture

<img width="1919" height="862" alt="image" src="https://github.com/user-attachments/assets/45699364-238a-4db9-8d67-ac64892b105a" />
<img width="1829" height="671" alt="image" src="https://github.com/user-attachments/assets/f863a076-3603-46dd-8ea6-6dca5363126f" />


  # 1. Python script - data generation
Script gen_stockanaliser.py generates JSON data every 5 seconds. Uses the azure-eventhub Python SDK to upload the data to Eventstream in MS Fabric.
  <img width="500" height="370" alt="py_script" src="https://github.com/user-attachments/assets/58521ca9-773b-4b4d-a8e2-70882c63c2dc" />

It is important to provide correct EVENTHUB_NAME and CONNECTION_STR to script. Theses can be found in Eventstream Live mode in Source details:

<img width="925" height="343" alt="image" src="https://github.com/user-attachments/assets/fec4dbb7-325b-4d45-9316-a78415edb0fc" />
<img width="925" height="343" alt="eventstream_conf" src="https://github.com/user-attachments/assets/2a2b8e44-dcc5-47d9-b26e-34626c1f2734" />

  # 2. Eventstream – ingest and data distribution

  Eventstream load data to Eventhouse and bronze Lakehouse table **tbl_bronze**.
<img width="1108" height="432" alt="eventstrean_graph" src="https://github.com/user-attachments/assets/46a9b005-87cb-464c-8227-69b8055fd4f8" />

  
  # 3. Eventhouse and Real-Time Report rt_ds_volumen_of_last_5minutes
Data lands in Eventhouse (KQL Database (Real-Time Analytics)) **eh_stockanaliser** table **tbl_eventtable**:
<img width="1488" height="568" alt="image" src="https://github.com/user-attachments/assets/9f50f05e-0429-45fd-9448-9b9ae043d07d" />

Real-Time Report base on KQL table using following query which shows agregated data from 5 last minutes:
<img width="678" height="484" alt="image" src="https://github.com/user-attachments/assets/68a6c62e-f9e7-4ff7-92ad-5b9a7c47a405" />

The reports are a simple tabular visualization, but the information they contain allows to get an idea of the situation on the stock exchange.
<img width="730" height="369" alt="image" src="https://github.com/user-attachments/assets/132e93d1-f819-4b97-9ca6-dc0d99d69c92" />

  # 4. Data orchestration – Data pipelines step 1
Data pipeline **dpl_load_medalion** includes 3 Notebooks. Every notebook load and transform data to different stages.
<img width="838" height="319" alt="image" src="https://github.com/user-attachments/assets/c28b2f8d-024a-4801-b28e-1c09ca5c9c31" />

Step 1 loads data from **tbl_bronze** into the **tbl_silver_par**t and **tbl_silver_wrong_currency** tables by executing the notebook **notebooks/nb_load_silver_part.ipynb**. Change Data Feed (CDF) is enabled on **tbl_bronze**; the notebook leverages CDF together with the helper table **cdf_control_silver_part** to append only the new rows to the target silver tables.

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

The script adds a **partition_date** column of DATE type, which serves as the partition key for both **tbl_silver_part** and **tbl_silver_wrong_currency**.

```
    with_date = changes_df \
    .withColumn("partition_date", to_date(col("timestamp"))) \
    .cache()                   # <<< cacheujemy wynik
```
<img width="871" height="378" alt="image" src="https://github.com/user-attachments/assets/39403592-4051-4619-b40b-c9d3631b1e63" />

  # 5. Data orchestration – Data pipelines step 2
<img width="820" height="346" alt="image" src="https://github.com/user-attachments/assets/3dfc40ce-a25e-466e-a70d-c0b8aa48060f" />

The Step 2 notebook **notebooks/nb_load_gold_tables_wrong_volume_per_day.ipynb** populates the table **tbl_gold_wrong_volume_per_day**. This table stores the aggregated trade volume (in millions of EUR) grouped by symbol, transaction type, and day. For performance reasons, the notebook processes only the data for the current day.
```
    .filter((col("event_type") == "TRADE") & (col("partition_date") == current_date()))
```
Rows in the target table are overwritten only when **partition_date** equals today’s date.
```
    .option("replaceWhere", "partition_date = current_date()")
```
<img width="547" height="468" alt="image" src="https://github.com/user-attachments/assets/548b260d-43b5-4432-9903-5f48e7e5687b" />
 
  # 6. Data orchestration – Data pipelines step 3
<img width="559" height="638" alt="image" src="https://github.com/user-attachments/assets/762a162a-fd0e-4365-88d5-9ab2e3e833e8" />

Step 3 runs the notebook **notebooks/nb_load_gold_tables_amount_per_event_type.ipynb**, which writes data to the partitioned table **tbl_gold_amount_per_event_type**. The notebook reads from **tbl_silver_part** using the query shown above. The resulting table records, for each traded symbol and trading day, the total count of market-quote events.

<img width="592" height="447" alt="image" src="https://github.com/user-attachments/assets/04eb49e7-517f-4c12-8fc6-1984ea150013" />

  # 7. Report rp_gold_amount_per_event_type
<img width="596" height="516" alt="image" src="https://github.com/user-attachments/assets/76700dc3-ff2c-4351-889b-535c4b4747f6" />

The report provides a straightforward view of the **tbl_gold_amount_per_event_type** table.

  ### Scheduling

The **dpl_load_medallion** data pipeline is scheduled to run every hour, while data continues to stream into Eventstream and is written to the **tbl_bronze** table in real time.

  ### Summary
In summary, the solution uses Microsoft Fabric components to build a complete data pipeline: streaming events are ingested through Eventstream and stored in the bronze layer, then refined within the Lakehouse via PySpark transformations. A scheduled pipeline runs these transformations at fixed intervals, ensuring that the data presented is always current. The resulting reports, though intentionally simple, deliver meaningful insight into the live equities market. The use of the Change Data Feed mechanism guarantees that only new records are promoted from the bronze layer to the silver layer, eliminating redundant loads.
