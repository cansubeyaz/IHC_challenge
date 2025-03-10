# Attribution Pipeline Orchestration

This project implements a data pipeline for attribution modeling using the IHC Attribution API. The pipeline consists of multiple steps, including querying data from a database, processing customer journeys, sending data to an API, saving attribution results, and generating reports.

## Project Structure

- `database_operations.py` - Handles database interactions, including querying session and conversion data, storing attribution results, and generating reports.
- `attribution_processing.py` - Processes customer journey data and interacts with the IHC API.
- `pipeline_runner.py` - Main pipeline script orchestrating all steps.

## Setup and Installation

1. Install dependencies:
   ```sh
   pip install -r requirements.txt
   ```

2. Ensure you have a SQLite database file (`challenge.db`) and schema file (`challenge_db_create.sql`).

## Running the Pipeline

The pipeline can be run using the `pipeline_runner.py` script. You can specify an optional date range for processing data.

### Basic Execution
```sh
python pipeline_runner.py --api_token <your_api_token>
```

### Execution with Date Range
To filter sessions and conversions within a specific date range:
```sh
python pipeline_runner.py --api_token <your_api_token> --start_date YYYY-MM-DD --end_date YYYY-MM-DD
```
Example:
```sh
python pipeline_runner.py --api_token my_secret_token --start_date 2023-08-01 --end_date 2023-08-31
```

## Pipeline Steps

1. **Database Setup:**
   - Creates necessary tables if not already present.
   - Loads session and conversion data from `challenge.db`.

2. **Customer Journey Processing:**
   - Matches sessions to conversions.
   - Structures data for the IHC API.

3. **IHC API Call:**
   - Sends customer journey data in batches to comply with API limits.
   - Retrieves attribution values for each session.

4. **Store Attribution Results:**
   - Saves received attribution data in the `attribution_customer_journey` table.

5. **Generate Channel Reporting:**
   - Aggregates marketing spend and revenue attribution per channel and date.

6. **Export Report to CSV:**
   - Includes cost-per-order (CPO) and return on ad spend (ROAS) calculations.

## Output Files

The pipeline generates a CSV report containing:
- `channel_name` - The marketing channel.
- `date` - The event date.
- `cost` - Total marketing costs.
- `ihc` - Sum of attribution values.
- `ihc_revenue` - Revenue attributed to the channel.
- `CPO` (Cost Per Order) - `cost / ihc`
- `ROAS` (Return on Ad Spend) - `ihc_revenue / cost`

Example CSV output:
```
channel_name,date,cost,ihc,ihc_revenue,CPO,ROAS
TikTok Ads,2023-09-06,1062.4659999999978,2.0,71.9,531.2329999999989,0.0676727537634147
Microsoft Ads,2023-09-07,141.924999999999,3.0,130.47,47.3083333333333,0.91928835652635
```

## Assumptions and Improvements

- **Assumptions:**
  - The IHC API requires sessions before conversion.
  - Some channels may have more weight in attribution calculations.
  - The schema file contains the necessary table definitions.

- **Possible Improvements:**
  - Optimize API batching for large datasets.
  - Improve error handling for API failures.
  - Extend support for additional attribution models.

## Author
Cansu Beyaz
