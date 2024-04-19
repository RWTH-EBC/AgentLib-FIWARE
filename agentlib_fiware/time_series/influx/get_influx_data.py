import numpy as np

import pandas as pd
from digital_twin_services.utils.Influx_Client import HttpsClient
import logging
import datetime
from ebcpy import TimeSeriesData
from typing import Union
from filip.models.base import FiwareHeader

logger = logging.getLogger(__name__)


def get_data_from_influx(
        fiware_header: FiwareHeader,
        entity_name_attributes: list,
        interval: float,
        to_date: datetime.datetime,
        token: str,
        organization: str,
        bucket: str = "Fiware_Test",
        influx_url: str = 'https://ebc-tick-stack.westeurope.cloudapp.azure.com:8086',
):

    client = HttpsClient(org=organization,
                         token=token,
                         url=influx_url,
                         timezone='CET')

    date_format = '%Y-%m-%d %H:%M:%S'

    from_date = to_date - datetime.timedelta(seconds=interval)

    from_date_str = datetime.datetime.strftime(from_date, date_format)
    to_date_str = datetime.datetime.strftime(to_date, date_format)

    topics = list()
    for entity_name_attribute in entity_name_attributes:
        split_entity = entity_name_attribute.split("/")
        topics.append([
            f'/fiware_to_influx/{fiware_header.service}{fiware_header.service_path}/{split_entity[0]}',
            f'{split_entity[1]}_value'
        ])

    config = {
                "structure": "Fiware",
                "bucket": bucket,
                "params": {"interval":  "10s",
                           "all_topics": False,
                           "query_by_interval": True
                           },
                "topics": topics
            }
    try:
        ts_data = client.get_timeseries(config=config, start_time=from_date_str, end_time=to_date_str)
    except Exception as err:
        print(f"Could not get data: {err}")
        return pd.DataFrame()
    # Todo implement for multiple configs (buckets)
    # for config in configs:
    #     this_ts_data = client.get_timeseries(config_name=config, start_time=from_date_str, end_time=to_date_str)
    #     # this_ts_data = this_ts_data.droplevel(2, axis=1).droplevel(1, axis=1)
    #     # this_ts_data = this_ts_data.fillna(method="ffill")
    #     ts_data = pd.concat([ts_data, this_ts_data], axis=1)
    ts_data = ts_data.dropna()
    ts_data = ts_data.astype(np.float64)
    return TimeSeriesData(ts_data)


if __name__ == "__main__":

    date_string = '2023-08-17 07:45:00'
    date_format = '%Y-%m-%d %H:%M:%S'

    to_date = datetime.datetime.strptime(date_string, date_format)

    data = get_data_from_influx(interval=3600,
                                to_date=to_date,
                                fiware_header={
                                                "service_path": "/fiware_to_influx/optihorstservice",
                                                "service": "Optihorst"
                                              },
                                entity_name_attributes=["urn:ngsi-ld:evaporator:temperatureSensor:02/temperature_value"]
                                )

    a = 1
