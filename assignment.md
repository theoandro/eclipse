# Take-Home Assignment: Real-Time Energy Data Processing and Analysis

## Objective
To evaluate the candidate's ability to work with real-time data streaming, data processing frameworks, cloud infrastructure, and containerization.

## Description
You are provided with simulation data for energy trades via WebSockets. Your task is to set up a data pipeline that ingests this streaming data, processes it, and performs some analysis. The WebSocket server will first send all historical data up to the current time and then continue sending live data starting from 6am CET. The processed data should then be stored in a format that can be queried for insights.

## Suggested Tasks

1. **Data Ingestion:**
   - Develop a batch ingestion pipeline to read the CSV file containing historical energy trades data.

2. **Data Processing:**
   - Use Apache Spark (preferably on Databricks) to process the ingested data. Perform the following transformations:
     - Calculate the average trade price and volume for each financial product. A financial product being defined as a tuple (Product x DeliveryStart x DeliveryEnd x DeliveryArea)
     - Identify the top 5 financial products traded in terms of volume.
     - Detect any anomalies in trade prices (e.g., prices that are significantly higher or lower than the average).

3. **Data Storage:**
   - Store the processed data in an AWS S3 bucket in Parquet format for efficient querying.

4. **Infrastructure as Code:**
   - Use Terraform to provision the necessary AWS resources (S3 bucket, IAM roles, etc.).
   - Use Docker to containerize the batch ingestion and Spark job.

5. **Optional (Bonus):**
   - Set up a Kafka topic using Confluent for intermediate data streaming before processing it with Spark.
   - Implement Flink for anomaly detection to showcase versatility with different streaming frameworks.

## Deliverables

1. **Code Repository (mandatory):**
   - A GitHub repository containing the code for the batch ingestion pipeline, Spark job, Terraform scripts, and Dockerfiles.
   - Provide a README file with clear instructions on how to set up and run the project.

2. **Data Processing Pipeline (mandatory):**
   - A working pipeline that ingests, processes, and stores the data as specified.

3. **Analysis Report:**
   - A Jupyter Notebook or a Markdown file containing the analysis of the processed data. Include visualizations and insights derived from the data.

4. **Presentation:**
   - Be prepared to present this in a 1-hour session. 20min demo + 40min questions/discussion

## Simulation Data
You will be provided with a script to simulate real-time energy trade data. The script will send data from a CSV file through WebSockets, starting from a specified `ExecutionTime`.

### Sample WebSocket Data Format
```json
{
  "TradeId": "1363439822",
  "RemoteTradeId": "221153888",
  "Side": "SELL",
  "Product": "XBID_Hour_Power",
  "DeliveryStart": "2023-01-01T10:00:00Z",
  "DeliveryEnd": "2023-01-01T11:00:00Z",
  "ExecutionTime": "2022-12-31T16:05:06.392Z",
  "DeliveryArea": "FR",
  "TradePhase": "CONT",
  "UserDefinedBlock": "N",
  "SelfTrade": "U",
  "Price": "-5.03",
  "Currency": "EUR",
  "Volume": "5",
  "VolumeUnit": "MWH",
  "OrderID": "13550086602"
}
```

### Simulation Script
You will be provided with a script `data_generator.py` to run the WebSocket server that simulates real-time data. The script sends all historical data up to the current time first and then continues sending live data starting from 6am CET. Ensure you have Python and the necessary libraries installed.


#### Instructions to Run the WebSocket Server
1. **Install the Required Library:**
   ```sh
   pip install websockets
   ```

2. **Run the Script:**
   ```sh
   python data_generator.py
   ```

### Sample WebSocket Client Script for Testing

```python
import asyncio
import websockets

async def listen():
    uri = "ws://localhost:8765"
    async with websockets.connect(uri) as websocket:
        while True:
            data = await websocket.recv()
            print(f"Received: {data}")

if __name__ == "__main__":
    asyncio.run(listen())
```

## Evaluation Criteria

1. **Technical Skills:**
   - Proficiency in using Spark, Databricks, and AWS.
   - Ability to write clean, efficient, and maintainable code.
   - Understanding of data processing and analysis techniques.

2. **Infrastructure and Deployment:**
   - Competence in using Terraform and Docker for infrastructure provisioning and containerization.

3. **Problem-Solving:**
   - Ability to handle real-time data and implement the required transformations and analyses.

4. **Communication:**
   - Clarity in the presentation and documentation.
   - Ability to explain technical details and decisions effectively.

## Submission
Please submit your GitHub repository link and the analysis report by Thursday 30 May 10am CET. Be prepared to present your work in a 1-hour session.

