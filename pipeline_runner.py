import sys
import logging
import argparse
import os
from datetime import datetime
import database_operations as db_ops
import attribution_processing as attribution

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('ihc_pipeline')
DEFAULT_SCHEMA_FILE = "challenge_db_create.sql"
DEFAULT_API_URL = "https://api.ihc-attribution.com/v1/compute_ihc"

def run_pipeline(db_path, schema_file, output_dir, api_token, start_date=None, end_date=None,test_mode=True, api_url=DEFAULT_API_URL):
    """
    Args:
        db_path (str): Path to SQLite database
        schema_file (str): Path to schema SQL file
        output_dir (str): Directory to save output files
        api_token (str): API token for IHC API
        start_date (str, optional): Start date in format 'YYYY-MM-DD'
        end_date (str, optional): End date in format 'YYYY-MM-DD'
        test_mode (bool): Whether to use test mode for API calls
        api_url (str): URL for IHC API

    Returns:
        bool: True if successful
    """
    try:
        start_time = datetime.now()
        date_range_info = ""
        if start_date and end_date:
            date_range_info = f" (date range: {start_date} to {end_date})"
        elif start_date:
            date_range_info = f" (from {start_date})"
        elif end_date:
            date_range_info = f" (to {end_date})"

        logger.info(f"Starting attribution pipeline{date_range_info} at {start_time}")
        logger.info("Step 1: Creating database tables")
        db_ops.create_database_tables(db_path, schema_file)
        logger.info(f"Step 2: Getting data from database{date_range_info}")
        sessions_df = db_ops.get_sessions_data(db_path, start_date, end_date)
        conversions_df = db_ops.get_conversions_data(db_path, start_date, end_date)
        logger.info("Step 3: Building customer journeys")
        journey_payload = attribution.build_customer_journeys(sessions_df, conversions_df)
        logger.info("Step 4: Calling IHC API")
        if test_mode:
            attribution_results = attribution.call_ihc_api_test(journey_payload, api_token=api_token)
        else:
            attribution_results = attribution.call_ihc_api_batched(journey_payload, api_token=api_token)
        logger.info("Step 5: Saving attribution results")
        db_ops.save_attribution_results(attribution_results, db_path)
        logger.info("Step 6: Generating channel reporting")
        db_ops.generate_channel_reporting(db_path, start_date, end_date)
        logger.info("Step 7: Exporting channel reporting")
        report_path = db_ops.export_channel_reporting(db_path, output_dir, start_date, end_date)

        end_time = datetime.now()
        duration = end_time - start_time

        logger.info(f"Attribution pipeline completed in {duration}")
        logger.info(f"Channel report saved to: {report_path}")

        print("\nAttribution Pipeline Summary:")
        print(f"Started: {start_time}")
        print(f"Completed: {end_time}")
        print(f"Duration: {duration}")
        if date_range_info:
            print(f"Date Range: {date_range_info.strip()}")
        print(f"Channel report: {report_path}")
        print("Pipeline completed successfully!")

        return True

    except Exception as e:
        logger.error(f"Error in attribution pipeline: {e}")
        return False

def validate_date_format(date_str):
    """Validate date format YYYY-MM-DD
    Args:
        date_str (str): Date string to validate

    Returns:
        bool: True if valid
    """
    try:
        if date_str:
            datetime.strptime(date_str, '%Y-%m-%d')
        return True
    except ValueError:
        return False

def main():
    parser = argparse.ArgumentParser(description='Run IHC attribution pipeline')
    parser.add_argument('--db', default='challenge.db', help='Path to SQLite database')
    parser.add_argument('--schema', default=DEFAULT_SCHEMA_FILE, help='Path to schema SQL file')
    parser.add_argument('--output', default='output', help='Directory to save output files')
    parser.add_argument('--start_date', help='Start date in format YYYY-MM-DD')
    parser.add_argument('--end_date', help='End date in format YYYY-MM-DD')
    parser.add_argument('--test', action='store_true', default=True, help='Use test mode for API calls')
    parser.add_argument('--prod', action='store_true', help='Use production API endpoints')
    parser.add_argument('--api_token', required=True, help='API token for IHC API (required)')
    parser.add_argument('--api_url', default=DEFAULT_API_URL, help='URL for IHC API')
    args = parser.parse_args()

    api_token = args.api_token
    api_url = os.environ.get('IHC_API_URL', args.api_url)

    if args.start_date and not validate_date_format(args.start_date):
        logger.error(f"Invalid start date format. Please use YYYY-MM-DD. Got: {args.start_date}")
        return 1

    if args.end_date and not validate_date_format(args.end_date):
        logger.error(f"Invalid end date format. Please use YYYY-MM-DD. Got: {args.end_date}")
        return 1

    test_mode = not args.prod  # If prod flag is set, disable test mode

    success = run_pipeline(
        args.db,
        args.schema,
        args.output,
        api_token,
        args.start_date,
        args.end_date,
        test_mode,
        api_url)

    return 0 if success else 1

if __name__ == "__main__":
    sys.exit(main())