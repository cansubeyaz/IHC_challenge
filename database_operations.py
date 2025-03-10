import os
import sqlite3
import logging
import pandas as pd
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('ihc_pipeline.database')

def create_database_tables(db_path, schema_file):
    """Create database tables from schema file
    Args:
        db_path (str): Path to SQLite database
        schema_file (str): Path to schema SQL file

    Returns:
        bool: True if successful
    """
    logger.info(f"Creating database tables using file: {schema_file}")

    try:
        if not os.path.exists(schema_file):
            logger.error(f"Schema file not found: {schema_file}")
            return False

        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        with open(schema_file, 'r') as f:
            schema_sql = f.read()
            cursor.executescript(schema_sql)

        conn.commit()
        conn.close()

        logger.info("Database tables created successfully.")
        return True

    except Exception as e:
        logger.error(f"Error creating tables: {e}")
        if 'conn' in locals():
            conn.close()
        return False

def get_sessions_data(db_path, start_date=None, end_date=None):
    """Get session data from database with optional date filtering

    Args:
        db_path (str): Path to SQLite database
        start_date (str, optional): Start date in format 'YYYY-MM-DD'
        end_date (str, optional): End date in format 'YYYY-MM-DD'

    Returns:
        pandas.DataFrame: Session data
    """
    logger.info(f"Getting session data from database (date range: {start_date} to {end_date})")

    try:
        conn = sqlite3.connect(db_path)

        ## Query to get session data with costs
        query = """
        SELECT 
            s.session_id, 
            s.user_id, 
            s.event_date, 
            s.event_time, 
            s.channel_name, 
            s.holder_engagement, 
            s.closer_engagement, 
            s.impression_interaction,
            COALESCE(c.cost, 0) AS cost
        FROM 
            session_sources s
        LEFT JOIN 
            session_costs c ON s.session_id = c.session_id
        """

        params = []
        if start_date:
            query += " WHERE s.event_date >= ?"
            params.append(start_date)

            if end_date:
                query += " AND s.event_date <= ?"
                params.append(end_date)
        elif end_date:
            query += " WHERE s.event_date <= ?"
            params.append(end_date)

        if params:
            sessions_df = pd.read_sql_query(query, conn, params=params)
        else:
            sessions_df = pd.read_sql_query(query, conn)

        sessions_df['timestamp'] = pd.to_datetime(sessions_df['event_date'] + ' ' + sessions_df['event_time']) ## Create timestamp column
        conn.close()
        logger.info(f"Retrieved {len(sessions_df)} sessions")
        return sessions_df

    except Exception as e:
        logger.error(f"Error getting sessions data: {e}")
        if 'conn' in locals():
            conn.close()
        raise

def get_conversions_data(db_path, start_date=None, end_date=None):
    """Get conversion data from database with optional date filtering

    Args:
        db_path (str): Path to SQLite database
        start_date (str, optional): Start date in format 'YYYY-MM-DD'
        end_date (str, optional): End date in format 'YYYY-MM-DD'

    Returns:
        pandas.DataFrame: Conversion data
    """
    logger.info(f"Getting conversion data from database (date range: {start_date} to {end_date})")

    try:
        conn = sqlite3.connect(db_path)

        ## Query to get conversion data
        query = """
        SELECT 
            conv_id, 
            user_id, 
            conv_date, 
            conv_time, 
            revenue
        FROM 
            conversions
        """

        params = []
        if start_date:
            query += " WHERE conv_date >= ?"
            params.append(start_date)

            if end_date:
                query += " AND conv_date <= ?"
                params.append(end_date)
        elif end_date:
            query += " WHERE conv_date <= ?"
            params.append(end_date)

        if params:
            conversions_df = pd.read_sql_query(query, conn, params=params)
        else:
            conversions_df = pd.read_sql_query(query, conn)

        conversions_df['timestamp'] = pd.to_datetime(conversions_df['conv_date'] + ' ' + conversions_df['conv_time'])
        conn.close()
        logger.info(f"Retrieved {len(conversions_df)} conversions")
        return conversions_df

    except Exception as e:
        logger.error(f"Error getting conversions data: {e}")
        if 'conn' in locals():
            conn.close()
        raise


def save_attribution_results(attribution_results, db_path):
    """Save attribution results to database
    Args:
        attribution_results (dict): Attribution results from IHC API
        db_path (str): Path to SQLite database

    Returns:
        int: Number of records saved
    """
    logger.info("Saving attribution results to database")

    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("DELETE FROM attribution_customer_journey")
        rows = []
        for conv_id, sessions in attribution_results.items():
            for session_id, ihc_value in sessions.items():
                rows.append((conv_id, session_id, float(ihc_value)))

        ## Insert data
        cursor.executemany("""
        INSERT INTO attribution_customer_journey (conv_id, session_id, ihc)
        VALUES (?, ?, ?)
        """, rows)

        conn.commit()
        record_count = len(rows)
        conn.close()
        logger.info(f"Saved {record_count} attribution records to database")
        return record_count

    except Exception as e:
        logger.error(f"Error saving attribution results: {e}")
        if 'conn' in locals():
            conn.rollback()
            conn.close()
        raise


def generate_channel_reporting(db_path, start_date=None, end_date=None):
    """Generate channel reporting data and save to database

    Args:
        db_path (str): Path to SQLite database
        start_date (str, optional): Start date in format 'YYYY-MM-DD'
        end_date (str, optional): End date in format 'YYYY-MM-DD'

    Returns:
        pandas.DataFrame: Generated reporting data
    """
    logger.info(f"Generating channel reporting data (date range: {start_date} to {end_date})")

    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        ## Query to generate reporting data
        query = """
        WITH attribution_revenue AS (
            SELECT 
                acj.session_id, 
                acj.ihc,
                c.revenue,
                (acj.ihc * c.revenue) AS ihc_revenue
            FROM 
                attribution_customer_journey acj
            JOIN 
                conversions c ON acj.conv_id = c.conv_id
        )
        SELECT 
            s.channel_name,
            s.event_date AS date,
            SUM(COALESCE(sc.cost, 0)) AS cost,
            SUM(COALESCE(ar.ihc, 0)) AS ihc,
            SUM(COALESCE(ar.ihc_revenue, 0)) AS ihc_revenue
        FROM 
            session_sources s
        LEFT JOIN 
            session_costs sc ON s.session_id = sc.session_id
        LEFT JOIN 
            attribution_revenue ar ON s.session_id = ar.session_id
        """

        ## Add date range filter if provided
        params = []
        if start_date or end_date:
            query += " WHERE "

            if start_date:
                query += "s.event_date >= ?"
                params.append(start_date)

                if end_date:
                    query += " AND s.event_date <= ?"
                    params.append(end_date)
            elif end_date:
                query += "s.event_date <= ?"
                params.append(end_date)

        query += """
        GROUP BY 
            s.channel_name, s.event_date
        ORDER BY 
            s.channel_name, s.event_date
        """

        if params: ## Execute query
            report_df = pd.read_sql_query(query, conn, params=params)
        else:
            report_df = pd.read_sql_query(query, conn)

        cursor.execute("DELETE FROM channel_reporting")

        for _, row in report_df.iterrows(): ## Insert data
            cursor.execute("""
            INSERT INTO channel_reporting (channel_name, date, cost, ihc, ihc_revenue)
            VALUES (?, ?, ?, ?, ?)
            """, (
                row['channel_name'],
                row['date'],
                float(row['cost']),
                float(row['ihc']),
                float(row['ihc_revenue'])))

        conn.commit()
        conn.close()
        logger.info(f"Generated channel reporting with {len(report_df)} records")
        return report_df

    except Exception as e:
        logger.error(f"Error generating channel reporting: {e}")
        if 'conn' in locals():
            conn.rollback()
            conn.close()
        raise

def export_channel_reporting(db_path, output_dir, start_date=None, end_date=None):
    """Export channel reporting data to CSV with CPO and ROAS columns
    Args:
        db_path (str): Path to SQLite database
        output_dir (str): Directory to save output file
        start_date (str, optional): Start date in format 'YYYY-MM-DD'
        end_date (str, optional): End date in format 'YYYY-MM-DD'

    Returns:
        str: Path to generated CSV file
    """
    date_suffix = ""
    if start_date and end_date:
        date_suffix = f"_{start_date}_to_{end_date}"
    elif start_date:
        date_suffix = f"_from_{start_date}"
    elif end_date:
        date_suffix = f"_to_{end_date}"

    logger.info(f"Exporting channel reporting data with CPO and ROAS{date_suffix}")

    try:
        conn = sqlite3.connect(db_path)

        query = "SELECT * FROM channel_reporting ORDER BY channel_name, date"
        report_df = pd.read_sql_query(query, conn)
        conn.close()

        ## CPO (Cost Per Order) column: cost/ihc
        report_df['CPO'] = report_df.apply(
            lambda row: row['cost'] / row['ihc'] if row['ihc'] > 0 else 0,
            axis=1)

        ## ROAS (Return on Ad Spend) column: ihc_revenue/cost
        report_df['ROAS'] = report_df.apply(
            lambda row: row['ihc_revenue'] / row['cost'] if row['cost'] > 0 else 0,
            axis=1)

        os.makedirs(output_dir, exist_ok=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"channel_report_{timestamp}{date_suffix}.csv"
        output_path = os.path.join(output_dir, filename)
        report_df.to_csv(output_path, index=False)

        logger.info(f"Exported channel reporting data to {output_path}")
        return output_path

    except Exception as e:
        logger.error(f"Error exporting channel reporting: {e}")
        if 'conn' in locals():
            conn.close()
        raise