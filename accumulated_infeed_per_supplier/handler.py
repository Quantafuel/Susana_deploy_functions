# -*- coding: utf-8 -*-
"""
Created on Mon Sep 23 12:08:20 2024

@author: Espen.Nordsveen
"""

# from cog_client import client


def handle(client):
    # """
    # [requirements]
    # pandas
    # datetime
    # [/requirements]

    # """

    import time

    from datetime import datetime

    import pandas as pd

    from cognite.client.data_classes import TimeSeriesWrite

    def delete_datapoints(supplier):
        client.time_series.data.delete_range(
            start=datetime(2024, 4, 16), end="now", external_id="accumulated_infeed_for_supplier_" + supplier
        )

    # local_timezone = pytz.timezone("Europe/Oslo")

    # events = client.events.list(data_set_ids=53597011926936, start_time={"min": int(datetime.now().timestamp()*1000)}, limit=None)
    events = client.events.list(
        data_set_ids=53597011926936, start_time={"min": int(datetime(2024, 4, 16).timestamp() * 1000)}, limit=None
    )

    supplier_list = [
        "AFLD",
        "MP_Nomi",
        "DKK_Lager",
        "DKK_Norfors",
        "DKK_SamAqua",
        "DKK_Vejle",
        "Horsens",
        "L&T",
        "Marius_Pedersen",
        "Renovest",
        "HCS",
        "Ribe_Flaskecentral",
        "Ribe_Flaskecentral_-_Esbjerg_kommune",
        "DKK_–_Vestforbrænding_Argo_Kbh",
        "Marius_Pedersen_Industriaffald",
    ]

    for supplier in supplier_list:
        try:
            ts_name = "Accumulated infeed for supplier " + supplier
            ts_xid = "accumulated_infeed_for_supplier_" + supplier
            client.time_series.create(TimeSeriesWrite(name=ts_name, external_id=ts_xid, data_set_id=53597011926936))
            print(f"Time series for {supplier} created!")
        except Exception as e:
            print(f"Failure {e}. Time series for {supplier} already created")
            # delete_datapoints(supplier)
            # print(f"Datapoints for {supplier} deleted")

    for event in events:
        start_time = event.start_time
        latest_dp = client.time_series.data.retrieve_latest(
            external_id="accumulated_infeed_for_supplier_" + event.type, before="now"
        )

        if event.end_time is None:
            if latest_dp and latest_dp.timestamp:
                start_time = latest_dp.timestamp[0]
            else:
                print(f"No latest timestamp found for event {event.type}, skipping...")
                continue  # Skip this event if there's no valid start_time

            end_time = int(time.mktime(datetime.now().timetuple()) * 1000)
        else:
            end_time = event.end_time

        df = client.time_series.data.retrieve_dataframe(
            external_id=["H109_band_weight_total_infeed", "H409_band_weight_total_infeed"],
            start=start_time,
            end=end_time,
        )

        if df.empty:
            print(f"No data found for event {event.type} in the given time range.")
            continue  # Skip to the next event

        start_values = df.iloc[0]
        df = df - start_values
        df.columns = ["H109", "H409"]

        df["Sum"] = df["H109"] + df["H409"]
        # Compute the change between consecutive values
        df["H109_change"] = df["H109"].diff().abs()
        df["H409_change"] = df["H409"].diff().abs()

        # Filter out changes that are too small (0 < Δvalue < 1)
        df = df[
            ~((df["H109_change"] > 0) & (df["H109_change"] < 2) & (df["H409_change"] > 0) & (df["H409_change"] < 2))
        ]

        df = df.drop(columns=["H109_change", "H409_change"])  # Remove unnecessary columns

        df = df[df["Sum"] > 0.01]  # Filter out small values
        print(df)
        if not latest_dp or not latest_dp.value:
            last_dp = 0  # If no previous value, start fresh
        else:
            last_dp = latest_dp.value[0]  # Get latest stored accumulated value

        if df.empty:
            print(f"Skipping event {event.type} because df is empty after filtering.")
            continue  # Skip to the next event

        # Reset accumulation for each supplier to avoid exponential buildup
        df["Sum_diff"] = df["Sum"].diff().fillna(df["Sum"].iloc[0])
        df["CumSum"] = df["Sum_diff"].cumsum() + last_dp

        print("Last datapoint:", last_dp)
        print("Start time:", datetime.fromtimestamp(start_time / 1000))

        df_final = pd.DataFrame(df["CumSum"])
        df_final.columns = ["accumulated_infeed_for_supplier_" + event.type]

        client.time_series.data.insert_dataframe(df_final)
        print(df_final)
        time.sleep(2)
        # else:
        #     print("No new events")
        #     pass


# function_name = "Minute average infeed per supplier"
# function_xid = "minute_average_infeed_per_supplier"

# try:
#     client.functions.delete(external_id = function_xid)
#     print("Function deleted")
# except Exception as e:
#     print("No function to delete")

# func = client.functions.create(
#     external_id = function_xid,
#     name = function_name,
#     function_handle = handle)

# print("Function created")
