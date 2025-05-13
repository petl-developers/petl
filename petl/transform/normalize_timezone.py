from datetime import datetime
import pytz

def normalize_timezone(table, timestamp_col='timestamp', tz_col='timezone'):
    """
    Normalize timestamps to UTC while retaining original timezone.

    Args:
        table: petl table (iterable of rows/dicts)
        timestamp_col (str): column name with timestamp strings
        tz_col (str): column name with timezone name (e.g., 'America/New_York')

    Yields:
        Each row with two added fields: 'timestamp_utc' and 'timezone_original'
    """
    for row in table:
        try:
            original_ts = row[timestamp_col]
            original_tz = row[tz_col]

            # Parse the timestamp
            naive_dt = datetime.fromisoformat(original_ts)

            # Attach original timezone
            local_dt = pytz.timezone(original_tz).localize(naive_dt)

            # Convert to UTC
            utc_dt = local_dt.astimezone(pytz.UTC)

            # Create a new row with original + new fields
            new_row = dict(row)
            new_row['timestamp_utc'] = utc_dt.isoformat()
            new_row['timezone_original'] = original_tz

            yield new_row

        except Exception as e:
            raise ValueError(f"Failed to normalize row {row} due to error: {e}")
