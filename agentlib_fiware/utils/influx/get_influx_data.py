import numpy as np

import pandas as pd
import logging
import datetime
from ebcpy import TimeSeriesData
from typing import Union
from filip.models.base import FiwareHeader

from agentlib_fiware.utils.influx.client import HttpsClient

logger = logging.getLogger(__name__)


def get_data_from_influx(
        fiware_header: FiwareHeader,
        entity_name_attributes: list,
        interval: float,
        to_date: datetime.datetime,
        token: str,
        organization: str,
        bucket: str,
        influx_url: str,
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

    ts_data = ts_data.dropna()
    ts_data = ts_data.astype(np.float64)
    return TimeSeriesData(ts_data)
