import asyncio
import websockets
import csv
import json
from datetime import datetime, timedelta
import os

CSV_FILE = os.path.join(os.path.dirname(
    __file__), "../data/Continuous_Trades-FR-20230101-20230102T001700000Z.csv")
START_TIME = datetime.strptime("2023-01-01T06:00:00Z", "%Y-%m-%dT%H:%M:%SZ")
TIME_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"  # Format of ExecutionTime in CSV


async def send_data(websocket, path):
    print("Client connected")
    try:
        with open(CSV_FILE, mode='r') as file:
            reader = csv.reader(file)
            next(reader)  # Skip the comment line
            headers = next(reader)  # Read the actual headers
            # Debug: Print the headers to check column names
            print(f"CSV Headers: {headers}")

            start_time = None
            # Send all historical data
            for row in reader:
                row_data = dict(zip(headers, row))
                try:
                    execution_time = datetime.strptime(
                        row_data['ExecutionTime'], TIME_FORMAT)
                    await websocket.send(json.dumps(row_data))
                    print(f"Sent historical data: {row_data}")
                except KeyError as e:
                    print(f"KeyError: {e} in row {row_data}")
                except Exception as e:
                    print(f"Error processing row {row_data}: {e}")

            # Wait until 6am to start sending live data
            now = datetime.utcnow()
            wait_time = (START_TIME - now).total_seconds()
            if wait_time > 0:
                print(
                    f"Waiting {wait_time} seconds to start sending live data...")
                await asyncio.sleep(wait_time)

            # Continue sending data from 6am onwards
            file.seek(0)
            next(reader)  # Skip the comment line again
            next(reader)  # Skip the headers again
            for row in reader:
                row_data = dict(zip(headers, row))
                try:
                    execution_time = datetime.strptime(
                        row_data['ExecutionTime'], TIME_FORMAT)
                    if execution_time >= START_TIME:
                        if start_time is None:
                            start_time = execution_time

                        # Calculate the time difference to simulate real-time sending
                        time_diff = (execution_time -
                                     start_time).total_seconds()
                        await asyncio.sleep(time_diff)

                        # Send the row as JSON
                        await websocket.send(json.dumps(row_data) + "\n")
                        print(f"Sent live data: {row_data}")

                        # Update start_time to current row's execution time
                        start_time = execution_time
                except KeyError as e:
                    print(f"KeyError: {e} in row {row_data}")
                except Exception as e:
                    print(f"Error processing row {row_data}: {e}")
    except Exception as e:
        print(f"Error: {e}")


async def main():
    async with websockets.serve(send_data, "localhost", 8765):
        await asyncio.Future()  # Run forever

if __name__ == "__main__":
    asyncio.run(main())
