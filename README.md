# LOTUS-PRISM
## Price & Retail Intelligence Streaming Monitor

## Topic
LOTUS-PRISM (Price & Retail Intelligence Streaming Monitor) - A system for competitive price analysis and optimization of pricing & promotions for Lotus's.

## Description
Development of a competitive price analysis and promotion optimization system for Lotus's, addressing the challenge of competition with retail industry rivals such as AEON, Lotte Mart, and Winmart. The system utilizes data from multiple sources, performs real-time analysis, and provides detailed reports to help make effective business decisions.

## Detail

### Extract: Collect data from multiple sources
- Web scraping product prices from competitor websites (AEON, Lotte Mart, MM Mega, Winmart)
- API connection to internal sales data
- Collection of market price trend data

### Transform: Process and transform data
- Data cleaning (null handling, duplicate removal)
- Price normalization across retailers
- Product categorization for comparison
- RFM (Recency, Frequency, Monetary) analysis for products

### Load: Import data into Data Lakehouse
- Store raw data in Bronze layer (Azure Data Lake Storage Gen2 + Delta Lake)
- Process and standardize data in Silver layer
- Create analytical data models in Gold layer
- Schema management with Delta Lake metadata layer
- Support for ACID transactions and time travel for historical pricing

### Real-time Data Simulation with Kafka
- Create producers to simulate real-time price change data
- Build Kafka topics or Azure Event Hubs containing price change events
- Use Azure Databricks Streaming/Azure Stream Analytics to process streaming data
- Update dashboards in real time

### Business Data Analysis
#### Price Analytics:
- Compare Lotus's product prices with competitors
- Analyze price sensitivity by product category

#### Promotion Analytics:
- Evaluate effectiveness of promotional campaigns
- Forecast impact of price adjustments

#### Product Analytics:
- Analyze the proportion of best-selling products at different price points
- Forecast product demand based on price fluctuations

### Building Infrastructure with Terraform
- Deploy Azure Data Lake Storage Gen2 for data lake
- Configure Azure Databricks for Spark processing
- Set up Azure Synapse Analytics for data warehouse and unified analytics
- Automate infrastructure deployment with Azure Resource Manager templates

### Developing REST API for Business Intelligence
- API endpoints (with Azure API Management) to query key KPIs
- APIs for retrieving analytical data by:
  + Price difference compared to competitors
  + Promotion performance
  + Price analysis by product category

### Dashboard & Reporting
- Build intuitive dashboards using Power BI
- Integrate Power BI directly with Azure Synapse Analytics
- Create automated reports for leadership

## Expected Output (Business)
- System capable of updating competitor prices in real time
- Dashboard comparing prices with competitors
- Model for automatic price adjustment recommendations
- API for accessing analytics and recommendations
- Reports evaluating the effectiveness of pricing and promotion strategies

## Expected Output (Technical)
### Data Lakehouse System on Azure
- Complete 3-tier data architecture (Bronze-Silver-Gold)
- ACID transaction support ensuring data consistency
- Time travel for price history retrieval
- Schema management with metadata layer

### Batch ETL Pipeline
- Automated scheduled data processing
- Data validation and quality checks
- Audit logs for all ETL activities

### Real-time Streaming Pipeline
- Kafka/Event Hubs topics for price change data
- Low-latency stream processing (<5 minutes)
- Real-time aggregations and alerting

### Competitive Price Analysis Dashboards
- Real-time price comparison with competitors
- Price trend reports over time
- Alerts when significant price changes are detected

### Price Optimization Model
- Forecast optimal price levels
- Price sensitivity analysis by category
- Price adjustment recommendations based on market data

### Promotion Effectiveness Analysis
- ROI assessment of promotional campaigns
- Reports on promotion impact on sales
- Forecast effectiveness of new promotion strategies

### Product Demand Forecasting
- Analysis of best-selling product rates by price
- Demand forecasting based on price fluctuations
- Analysis of correlation between price and demand

### REST API for Business Intelligence
- API endpoints with authentication and rate limiting
- API documentation (Swagger/OpenAPI)
- Endpoints for KPIs and in-depth analysis

### Power BI Dashboards
- Visual dashboards
- Direct integration with Azure Synapse Analytics
- Scheduled automatic reports

### Terraform IaC
- Scripts for deploying the entire infrastructure
- CI/CD pipeline for automatic updates
- Deployment and operation documentation

## Requirement
### Technical
- Knowledge of Azure Cloud (Data Lake Storage, Databricks, Synapse Analytics)
- Experience with Apache Kafka for streaming data processing
- Skills in Apache Spark and PySpark
- Understanding of Terraform and Infrastructure as Code
- Experience in REST API development
- Skills in working with Power BI

### Tools & Frameworks
- Azure Cloud Services (ADLS Gen2, Databricks, Synapse)
- Apache Kafka / Azure Event Hubs
- Apache Spark / PySpark
- Delta Lake
- Terraform
- Python (pandas, BeautifulSoup, Flask/FastAPI)
- Power BI

## Architecture
```
+------------------------+    +----------------------+    +--------------------+
|                        |    |                      |    |                    |
|  DATA SOURCES          +--->+  DATA PROCESSING     +--->+  STORAGE & ANALYSIS|
|                        |    |                      |    |                    |
+------------------------+    +----------------------+    +--------------------+
       |                              |                           |
       v                              v                           v
+------------------------+    +----------------------+    +--------------------+
|                        |    |                      |    |                    |
|  - Web Scraping        |    |  - Batch Processing  |    |  - Data Lakehouse |
|  - Internal API        |    |    (Apache Spark)    |    |    (Azure ADLS)   |
|  - Market data         |    |  - Stream Processing |    |  - Power BI       |
|  - Kafka Producers     |    |    (Kafka + Spark)   |    |  - REST API       |
|                        |    |                      |    |                    |
+------------------------+    +----------------------+    +--------------------+
                                                                   |
                                                                   v
                                                          +--------------------+
                                                          |                    |
                                                          |  USERS             |
                                                          |  - Dashboards      |
                                                          |  - API Clients     |
                                                          |  - Reports         |
                                                          |                    |
                                                          +--------------------+
```

### Data Flow Details:
1. **Data Collection** from sources (web scraping, internal API, market data)
2. **Data Processing** through batch and stream processing
3. **Data Storage** in Azure Data Lake Storage with Bronze-Silver-Gold architecture
4. **Data Analysis** with Azure Synapse Analytics and Databricks
5. **Data Visualization** through Power BI dashboards and API endpoints
