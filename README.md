Python Cloud Data Pipeline — Financial Data Simulation
Project Overview
This project implements a Python-based cloud data pipeline designed to simulate how financial systems capture, validate, and manage operational data. The entire application was developed and executed using Python and Azure Functions within VS Code, making it a structured example of modern data handling and core programming logic.

Project Features
This simulated environment supports essential data operations to ensure accurate financial reporting:

Automated Data Capture: Automatically fetches market data from external API sources.

Structured Data Layers: Implements a "Medallion Architecture" to organize data into Raw (Bronze), Cleaned (Silver), and Analytical (Gold) stages.

Account & Transaction Management: Logic for processing stock records and financial metrics into structured formats.

Cloud Database Integration: Automatically validates and loads final data into a centralized SQL database.

Tech Stack & Environment
Language: Python 3.x

Orchestration: Azure Functions (Serverless Automation)

Cloud Platform: Microsoft Azure (Blob Storage & SQL Database)

Development Tool: Visual Studio Code (VS Code)

Data Format: Parquet & JSON

Key Results
Full Automation: The pipeline is triggered by a timer, removing the need for manual data entry.

Data Validation: Built-in checks ensure that only clean, formatted data reaches the final database.

High Efficiency: Processes multiple financial datasets and updates the system in under two minutes.

Centralized Logging: Every transaction and data movement is logged for easy monitoring and history tracking.
