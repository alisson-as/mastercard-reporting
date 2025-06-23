# Mastercard Reporting
Mastercard Reporting (QMR) - Active Card Follow-Up Quarterly

1. Context: Flash, a Mastercard Master Licensee, and are responsible for building and reporting operational statistics to Mastercard on a quarterly basis, in accordance with the rules and definitions provided in the “Issuance Definitions” document ([IssuingDefinitions](https://github.com/alisson-as/mastercard-reporting/blob/3269eea938c3d5b2db47946054894d1b23f03d4a/docs/Issuing%20Definitions.pdf)).
2. Problem:
Creating an active card submission process that allows the company to accurately meet Mastercard’s reporting requirements, as well as analyze this data to propose and lead process changes that aim to leverage Flash’s results.

---

## Data Pipeline:
![data_pipeline](https://github.com/user-attachments/assets/7f27b186-5c3d-455c-bfef-fe6636c5c6c5)

## 📂 Repository Structure
```
├── analysis
|   ├── analysis_card_validity.sql             
|   ├── analysis_model.sql
|   ├── average_ticket_card.sql
|   ├── cards_per_valid_thru.sql              
|   ├── quarter_results.sql
|   ├── temporarily_blocked.sql
|   └── data_analysis.ipynb                     # Script to perform detailed data analysis and answer about case
|
├── docs
|   ├── Case Tecnico_Analista de Dados.pdf      # File with case details
|   └── Issuing Definitions.pdf                 # File with rules and definitions provided
|                                        
├── src                                         # Contains notebooks and sql script used for data intake
│   ├── bronze                                  # Files used to intake data from raw to bronze
│   │   └── intake.ipynb                        # File that getting csv files from google cloud storage (GCP) and creating bronze delta tables
│   ├── silver
│   │   ├── cards.sql
│   │   ├── cards_status.sql
│   │   ├── cards_transactions.sql
│   │   └── intake_silver.ipynb                 # Script to get bronze delta tables and creating silver delta tables
|
├── LICENSE
└── README.md                                   # Project principal documentation
```
---

## 💻 Technologies Used

- **Databricks**: Used to create data intake and analysis
- **Google Cloud**: Used like data storage
- **SQL Language**: Program Language used for create queries
- **Python with Apache Spark**: Program language used to create data intake and analysis
  
---

## 📜 Final Products

- [Mastercard Reporting - Presentation](https://docs.google.com/presentation/d/1wpbgeWIegw_WWHALlYMmZRo2W3hgboIm/edit?usp=sharing&ouid=108268082734461286547&rtpof=true&sd=true)
- [Script Data Analysis](https://storage.googleapis.com/site-htm/data_analysis.html) 

---

## 📞 Contato

Alisson Aragão dos Santos | [E-mail](alissonaragao1@gmail.com) | [LinkedIn](https://www.linkedin.com/in/alisson-arag%C3%A3o-dos-santos-459297120/)</a>
