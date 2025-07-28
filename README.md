# MS Fabric stockanaliser project
### General descritpion
Built natively on MS Fabric, the project concentrates on data-engineering aspects. Deliverables include:

  1. Real-time analytical report assessing the volumes of listed shares.
  
  2. Two Reports designed to provide information of the stock environment (data are ingested hourly).

Synthetic (randomized) market-quote events are created by local Python scrypt, ingested via Eventstream into Eventhouse. Eventstream also load data to Lakehouse bronze tier. Orchestration is handled by Data Pipelines that execute PySpark notebooks which load silver and gold tier. Final outputs are exposed in a Reports (Lakehouse source) and an Real-Time Dashboard (Eventhouse source).


### Project Architecture

<img width="1919" height="905" alt="image" src="https://github.com/user-attachments/assets/e3391c5c-ba5c-4261-96a9-0d341c914185" />
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
  # 4. Data orchestration – Data pipelines step 1
  # 5. Data orchestration – Data pipelines step 2
  # 6. Report rp_gold_wrong_volume_per_day
  # 7. Data orchestration – Data pipelines step 3
  # 8. Report rp_gold_amount_per_event_type
  
  
  ### Scheduling
  ### Summary
