# GL_Recon_Proj
Project Name: Stock Cash Reconciliation Platform

Business Problem

An organisation receives:

1. Stock Transactions

Stock purchases
Stock sales
Inventory movements

2. Cash Transactions

Payments to suppliers
Customer receipts
Bank transactions

3. Overhead Costs

Salaries
Rent
Utilities
Transportation

The objective is to:

Ingest data from multiple sources
Build a Medallion Architecture
Reconcile stock values against cash movements
Identify mismatches
Visualise KPIs on a dashboard


Architecture


            Source Systems
    ┌──────────┬───────────┬───────────┐
    │ Stock    │ Cash      │ Overheads │
    │ CSV/API  │ Bank File │ ERP File  │
    └────┬─────┴─────┬─────┴─────┬─────┘
         │           │           │
         ▼           ▼           ▼

              BRONZE LAYER
      Raw ingestion using Auto Loader

                     ▼

              SILVER LAYER
     Cleansing + Standardisation
     Data Quality Validation

                     ▼

                GOLD LAYER
     Reconciliation Data Mart
     Financial KPIs

                     ▼

         Databricks Dashboard
      Power BI / Tableau Optional
