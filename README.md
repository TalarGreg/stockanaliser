# MS Fabric stockanaliser project
### General descritpion
Built natively on MS Fabric, the project concentrates on data-engineering aspects. Deliverables include:

  1. Real-time analytical report assessing the volumes of listed shares.
  
  2. Two Reports designed to provide information of the stock environment (data are ingested hourly).

Synthetic (randomized) market-quote events are created by local Python scrypt, ingested via Eventstream into Eventhouse. Eventstream also load data to Lakehouse bronze tier. Orchestration is handled by Data Pipelines that execute PySpark notebooks which load silver and gold tier. Final outputs are exposed in a Reports (Lakehouse source) and an Real-Time Dashboard (Eventhouse source).


### 1. Project Architecture

<img width="1884" height="853" alt="general" src="https://github.com/user-attachments/assets/8a21a47d-9d8b-4259-9a72-9823816309a4" />
