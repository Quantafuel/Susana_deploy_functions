def handle(client):

    from datetime import datetime
    from zoneinfo import ZoneInfo

    import pandas as pd

    latest_dp_1 = client.time_series.data.retrieve_latest(external_id="line1_status")
    latest_dp_2 = client.time_series.data.retrieve_latest(external_id="line2_status")

    dps1 = client.time_series.data.retrieve_dataframe(
        external_id='EAB:s="F110-M1_IW3_Strom"',
        start=latest_dp_1.timestamp[0],
        end=datetime.now(tz=ZoneInfo("Europe/Oslo")),
        aggregates="average",
        granularity="1m",
        include_aggregate_name=False,
    )

    dps2 = client.time_series.data.retrieve_dataframe(
        external_id='EAB:s="F410-M1_IW3_Strom"',
        start=latest_dp_2.timestamp[0],
        end=datetime.now(tz=ZoneInfo("Europe/Oslo")),
        aggregates="average",
        granularity="1m",
        include_aggregate_name=False,
    )

    def status(value):
        if value < 100:
            return 0
        else:
            return 1

    dps1["status"] = dps1['EAB:s="F110-M1_IW3_Strom"'].apply(lambda x: status(x))
    dps2["status"] = dps2['EAB:s="F410-M1_IW3_Strom"'].apply(lambda x: status(x))

    ts_xid1 = "line1_status"
    ts_xid2 = "line2_status"

    df1 = pd.DataFrame({ts_xid1: dps1["status"]}, index=dps1.index)
    df2 = pd.DataFrame({ts_xid2: dps2["status"]}, index=dps2.index)

    client.time_series.data.insert_dataframe(df1)
    client.time_series.data.insert_dataframe(df2)

    return "Sucessfully ran the function"
