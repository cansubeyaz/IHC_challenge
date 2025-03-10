import json
import logging
import requests
import pandas as pd
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('ihc_pipeline.attribution')

IHC_API_URL = "https://api.ihc-attribution.com/v1/compute_ihc"
IHC_API_TOKEN = "01995935-e304-4aaa-a674-42342551c490" ## API KEY

def build_customer_journeys(sessions_df, conversions_df):
    """Build customer journeys for IHC API
    Args:
        sessions_df (pandas.DataFrame): Session data
        conversions_df (pandas.DataFrame): Conversion data

    Returns:
        dict: Customer journeys in format expected by IHC API
    """
    logger.info("Building customer journeys")

    try:
        users_data = {}
        all_users = set(sessions_df['user_id'].unique()) & set(conversions_df['user_id'].unique()) ## Get all unique user ID
        logger.info(f"Found {len(all_users)} users with both sessions and conversions")

        for user_id in all_users:
            user_sessions = sessions_df[sessions_df['user_id'] == user_id].copy() ## Get user's sessions and conversions
            user_conversions = conversions_df[conversions_df['user_id'] == user_id].copy()

            if len(user_sessions) == 0 or len(user_conversions) == 0:  # Skip users with no data
                continue

            sessions_list = []
            for _, session in user_sessions.iterrows():
                session_dict = {
                    'session_id': session['session_id'],
                    'timestamp': session['timestamp'].strftime('%Y-%m-%d %H:%M:%S'),
                    'channel': session['channel_name'],
                    'holder_engagement': bool(session['holder_engagement']),
                    'closer_engagement': bool(session['closer_engagement']),
                    'impression_interaction': bool(session['impression_interaction']),
                    'cost': float(session['cost']) if not pd.isna(session['cost']) else 0.0}
                sessions_list.append(session_dict)

            conversions_list = []
            for _, conversion in user_conversions.iterrows():
                conv_time = conversion['timestamp']
                if len(user_sessions[user_sessions['timestamp'] < conv_time]) > 0: ## Only include conversions with sessions before them
                    conversion_dict = {
                        'conv_id': conversion['conv_id'],
                        'timestamp': conv_time.strftime('%Y-%m-%d %H:%M:%S'),
                        'revenue': float(conversion['revenue'])}
                    conversions_list.append(conversion_dict)

            if sessions_list and conversions_list:
                users_data[str(user_id)] = {
                    'sessions': sessions_list,
                    'conversions': conversions_list}

        journey_payload = {'users': users_data}
        user_count = len(users_data)
        session_count = sum(len(user_data['sessions']) for user_data in users_data.values())
        conversion_count = sum(len(user_data['conversions']) for user_data in users_data.values())

        logger.info(
            f"Built customer journeys with {user_count} users, {session_count} sessions, and {conversion_count} conversions")
        return journey_payload

    except Exception as e:
        logger.error(f"Error building customer journeys: {e}")
        raise

def call_ihc_api_test(journey_payload, api_token=IHC_API_TOKEN):
    """Test version of the IHC API
    Args:
        journey_payload (dict): Customer journeys data
        api_token (str): IHC API token (not used in test)

    Returns:
        dict: Simulated attribution results
    """
    logger.info("Using test IHC API")
    attribution_results = {}

    for user_id, user_data in journey_payload.get('users', {}).items():
        sessions = user_data.get('sessions', [])
        conversions = user_data.get('conversions', [])

        if not sessions or not conversions:
            continue

        for conversion in conversions:
            conv_id = conversion['conv_id']
            conv_timestamp = datetime.strptime(conversion['timestamp'], '%Y-%m-%d %H:%M:%S')

            if conv_id not in attribution_results:
                attribution_results[conv_id] = {}

            eligible_sessions = []
            for session in sessions:
                session_timestamp = datetime.strptime(session['timestamp'], '%Y-%m-%d %H:%M:%S')
                if session_timestamp < conv_timestamp:
                    eligible_sessions.append(session)

            if not eligible_sessions:
                continue

            total_sessions = len(eligible_sessions)

            for i, session in enumerate(eligible_sessions):
                session_id = session['session_id']
                recency_weight = (i + 1) / total_sessions
                channel = session['channel']
                channel_weight = 1.0
                if 'Email' in channel:
                    channel_weight = 1.2
                elif 'Social' in channel:
                    channel_weight = 1.1
                elif 'Search' in channel:
                    channel_weight = 1.3
                elif 'Direct' in channel:
                    channel_weight = 0.8

                engagement_weight = 1.0
                if session['holder_engagement']:
                    engagement_weight += 0.5
                if session['closer_engagement']:
                    engagement_weight += 0.7

                raw_weight = recency_weight * channel_weight * engagement_weight
                attribution_results[conv_id][session_id] = raw_weight

            ## Normalize attribution values to sum to 1.0
            total_weight = sum(attribution_results[conv_id].values())
            for session_id in attribution_results[conv_id]:
                attribution_results[conv_id][session_id] /= total_weight

    logger.info(f"Generated mock attribution for {len(attribution_results)} conversions")
    return attribution_results

def call_ihc_api_batched(journey_payload, api_token=IHC_API_TOKEN, max_journeys=100):
    """Call IHC API with customer journey data in batches
    Args:
        journey_payload (dict): Customer journeys data
        api_token (str): IHC API token
        max_journeys (int): Maximum journeys per batch

    Returns:
        dict: Combined attribution results
    """
    logger.info("Calling IHC API with batched requests in IHC format")

    flattened_journeys = []
    for user_id, user_data in journey_payload.get('users', {}).items():
        sessions = user_data.get('sessions', [])
        conversions = user_data.get('conversions', [])

        if not sessions or not conversions:  # Skip users without sessions or conversions
            continue

        for conversion in conversions:
            conv_id = conversion['conv_id']
            conv_timestamp = datetime.strptime(conversion['timestamp'], '%Y-%m-%d %H:%M:%S')

            for session in sessions:
                session_timestamp = datetime.strptime(session['timestamp'], '%Y-%m-%d %H:%M:%S')

                if session_timestamp < conv_timestamp: ## Only include sessions before the conversion
                    flattened_journey = {
                        'conversion_id': conv_id,
                        'session_id': session['session_id'],
                        'timestamp': session['timestamp'],
                        'channel_label': session['channel'],
                        'holder_engagement': 1 if session['holder_engagement'] else 0,
                        'closer_engagement': 1 if session['closer_engagement'] else 0,
                        'conversion': 0,  # Not the conversion session
                        'impression_interaction': 1 if session['impression_interaction'] else 0}
                    flattened_journeys.append(flattened_journey)

            # Add the conversion session itself
            conversion_session = {
                'conversion_id': conv_id,
                'session_id': f"conversion_{conv_id}",
                'timestamp': conversion['timestamp'],
                'channel_label': "Conversion",
                'holder_engagement': 0,
                'closer_engagement': 0,
                'conversion': 1,
                'impression_interaction': 0}
            flattened_journeys.append(conversion_session)

    logger.info(f"Formatted {len(flattened_journeys)} session-conversion pairs for IHC API")

    ## Group by conversion_id to create batches
    journeys_by_conversion = {}
    for journey in flattened_journeys:
        conv_id = journey['conversion_id']
        if conv_id not in journeys_by_conversion:
            journeys_by_conversion[conv_id] = []
        journeys_by_conversion[conv_id].append(journey)

    conversion_ids = list(journeys_by_conversion.keys())
    total_conversions = len(conversion_ids)
    logger.info(f"Total conversions to process: {total_conversions}")

    redistribution_parameter = {
        'initializer': {
            'direction': 'earlier_sessions_only',
            'receive_threshold': 0,
            'redistribution_channel_labels': [],},
        'holder': {
            'direction': 'any_session',
            'receive_threshold': 0,
            'redistribution_channel_labels': [],},
        'closer': {
            'direction': 'later_sessions_only',
            'receive_threshold': 0,
            'redistribution_channel_labels': [],}}

    combined_results = {}
    batch_size = max_journeys

    for i in range(0, total_conversions, batch_size):
        batch_conversion_ids = conversion_ids[i:i + batch_size]

        batch_journeys = []
        for conv_id in batch_conversion_ids:
            batch_journeys.extend(journeys_by_conversion[conv_id])

        logger.info(
            f"Processing batch {i // batch_size + 1}: {len(batch_conversion_ids)} conversions, {len(batch_journeys)} sessions")

        batch_payload = {
            'customer_journeys': batch_journeys,
            'redistribution_parameter': redistribution_parameter}

        # Use empty conv_type_id for now (might need to be provided)
        conv_type_id = 'default'
        api_url = f"https://api.ihc-attribution.com/v1/compute_ihc?conv_type_id={conv_type_id}"

        try: ## Call API
            headers = {
                'Content-Type': 'application/json',
                'x-api-key': api_token}

            response = requests.post(
                api_url,
                data=json.dumps(batch_payload),
                headers=headers,
                timeout=60)

            response.raise_for_status()
            results = response.json()

            if 'statusCode' in results and results['statusCode'] != 200:
                logger.error(f"API error: {results}")
                if 'partialFailureErrors' in results:
                    logger.error(f"Partial failures: {results['partialFailureErrors']}")
                continue

            ## Extract IHC values
            if 'value' in results:
                attribution_values = results['value']

                ## Map back to format (conv_id -> session_id -> ihc)
                for conv_data in attribution_values:
                    conv_id = conv_data.get('conversion_id')
                    if not conv_id:
                        continue

                    if conv_id not in combined_results:
                        combined_results[conv_id] = {}

                    ## Add all session attributions
                    for session_data in conv_data.get('sessions', []):
                        session_id = session_data.get('session_id')
                        ihc_value = session_data.get('ihc', 0)

                        if session_id and not session_id.startswith('conversion_'):  # Skip the conversion session
                            combined_results[conv_id][session_id] = ihc_value

            else:
                logger.warning(f"Unexpected API response format: {results}")

        except requests.exceptions.RequestException as e:
            logger.error(f"Error in API batch call: {e}")
            if hasattr(e, 'response') and e.response:
                logger.error(f"Response status: {e.response.status_code}")
                logger.error(f"Response text: {e.response.text}")
            raise

    logger.info(f"Completed all batches. Received attribution for {len(combined_results)} conversions")
    return combined_results


def call_ihc_api(journey_payload, api_url=IHC_API_URL, api_token=IHC_API_TOKEN):
    """Call IHC API with customer journey data
    Args:
        journey_payload (dict): Customer journeys data
        api_url (str): IHC API URL
        api_token (str): IHC API token

    Returns:
        dict: Attribution results
    """
    logger.info("Calling IHC API")

    try:
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {api_token}'}

        payload_json = json.dumps(journey_payload)
        logger.info(f"API payload size: {len(payload_json)} bytes")

        response = requests.post(
            api_url,
            headers=headers,
            data=payload_json)

        response.raise_for_status()
        attribution_results = response.json()

        logger.info(f"Received attribution results for {len(attribution_results)} conversions")
        return attribution_results

    except requests.exceptions.RequestException as e:
        logger.error(f"Error calling IHC API: {e}")
        if hasattr(e, 'response') and e.response:
            logger.error(f"Response status: {e.response.status_code}")
            logger.error(f"Response text: {e.response.text}")
        raise