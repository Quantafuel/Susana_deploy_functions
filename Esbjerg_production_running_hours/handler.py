# -*- coding: utf-8 -*-
"""
Created on Thu Mar  6 14:12:07 2025

@author: Espen.Nordsveen
"""


def handle(client):
    """


    Parameters
    ----------
    client : TYPE
        DESCRIPTION.

    Returns
    -------
    df : TYPE
        Returns the running time of line 1 and 2 since last existing datapoint in CDF.

    """
    from datetime import datetime
    from zoneinfo import ZoneInfo

    import pandas as pd

    latest_1 = client.time_series.data.retrieve_latest(external_id="line1_runningtime")
    latest_2 = client.time_series.data.retrieve_latest(external_id="line2_runningtime")

    # Fetch the data for dps1
    dps1 = client.time_series.data.retrieve_dataframe(
        external_id='EAB:s="F110-M1_IW3_Strom"',
        start=latest_1.timestamp[0],
        end=datetime.now(tz=ZoneInfo("Europe/Oslo")),
        aggregates="average",
        granularity="1m",
    )

    dps2 = client.time_series.data.retrieve_dataframe(
        external_id='EAB:s="F410-M1_IW3_Strom"',
        start=latest_1.timestamp[0],
        end=datetime.now(tz=ZoneInfo("Europe/Oslo")),
        aggregates="average",
        granularity="1m",
    )

    # Function to calculate accumulated running time and add it as a new column
    def calculate_accumulated_running_time(df, line):
        # Initialize variables
        running_time = pd.Timedelta(0)  # Start with 0 time
        accumulated_times = []  # List to store accumulated running times
        column_name = df.columns[0]  # The first column is the data we check (assuming it's the relevant data column)

        for index, row in df.iterrows():
            # Check if the value in the row is above 100 to count as "running"
            if row[column_name] > 100:
                running_time += pd.Timedelta(minutes=1)  # Add 1 minute for each "running" condition met
            # Append the current accumulated time to the list
            accumulated_times.append(running_time.total_seconds())

        # Add the accumulated times as a new column in the dataframe
        df[f"line{line}_runningtime"] = accumulated_times

        return df

    # Apply the function to dps1 dataframe
    dps1_with_running_time = calculate_accumulated_running_time(dps1, 1)
    dps2_with_running_time = calculate_accumulated_running_time(dps2, 2)
    df_final1 = pd.DataFrame(dps1_with_running_time["line1_runningtime"]) + latest_1.value[0]
    df_final2 = pd.DataFrame(dps2_with_running_time["line2_runningtime"]) + latest_2.value[0]

    client.time_series.data.insert_dataframe(df_final1)
    client.time_series.data.insert_dataframe(df_final2)
