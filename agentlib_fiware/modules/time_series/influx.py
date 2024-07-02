import logging
import datetime
import os
from typing import Union, Optional
import math
import requests
from dotenv import load_dotenv

from pydantic import Field, AnyUrl
import pandas as pd

from filip.models.base import FiwareHeader
from filip.utils.validators import AnyHttpUrl
from filip.models.base import FiwareHeader
from filip.clients.ngsi_v2.quantumleap import QuantumLeapClient

from agentlib_fiware.modules.time_series.base import BaseTimeSeriesAcquisition, BaseTimeSeriesAcquisitionConfig

logger = logging.getLogger(__name__)


class InfluxDataAcquisitionConfig(BaseTimeSeriesAcquisitionConfig):
    influx_url: AnyUrl = Field(
        description="URL to Influx"
    )
    entity_name_attributes: List[str] = Field(
        title="Mapping of entity/attribute combinations to extract from influx"
    )
    fiware_header: FiwareHeader = Field(
        description="service and service path in fiware header"
    )
    organisation: str = Field(
        title="Influx Organisation"
    )
    token_file: str = Field(
        description=".env file with the environment variable `INFLUX_TOKEN` (or token_env_name) in it."
    )
    token_env_name: Optional[str] = Field(
        description="Name of the environment variable to be used",
        default="INFLUX_TOKEN"
    )
    bucket: str = Field(
        title="Influx Bucket"
    )
    constant_to_date: Optional[datetime.datetime] = Field(
        description="For development: Constant time until the data is to be extracted.",
        default=None
    )


class InfluxDataAcquisition(BaseTimeSeriesAcquisition):
    config: InfluxDataAcquisitionConfig

    def register_callbacks(self):
        pass

    def process(self):
        load_dotenv(self.config.token_file)
        while True:
            # Start extraction
            self.logger.debug("Getting data")

            # Extract data from Influx
            if self.config.constant_to_date is not None:
                to_date = self.config.constant_to_date
            else:
                to_date = datetime.datetime.now()
            tsd = get_data_from_influx(
                interval=self.config.time_period,
                to_date=to_date,
                fiware_header=self.config.fiware_header,
                entity_name_attributes=self.config.entity_name_attributes,
                influx_url=self.config.influx_url,
                token=os.environ[self.config.token_env_name],
                bucket=self.config.bucket,
                organization=self.config.organisation
            )

            tsd_json = tsd.to_json(orient="split")

            # Trigger the sending over the data_broker
            self.logger.debug("Retrieved data and sending into DataBroker")
            self.set("time_series_data", tsd_json)

            yield self.env.timeout(self.config.interval)


def get_data_from_ql(
        entity_name_attributes: list,
        interval: float,
        to_date: datetime.datetime,
        fiware_header: Union[FiwareHeader, dict],
        ql_url: Union[AnyHttpUrl, str],
        chunk_size: int = 10000
):
    """
    Gets data (sim and meas) from the CrateDB. First checks if the all the needed
    Data are saved in CrateDB, then send a query request using the
    QuantumLeap to get the needed data. Afterwards the data is
    converted to a dataframe

    :param list entity_name_attributes:
        List of entries like "entity_name/attribute_name"
    :param float interval:
        The Interval in seconds to extract
    :param datetime.datetime to_date:
        Will extract the data from (to_date - interval) -> to_date
    :param (FiwareHeader, dict) fiware_header:
        FiwareHeader to extract the data from
    :param (AnyHttpUrl, str) ql_url:
        The URL to QuantumLeap
    :param int chunk_size:
        Chunk size to extract the data with. Maximum and default is 10000.

    :return:
    """
    _max_chunk_size = 10000
    if chunk_size > _max_chunk_size:
        logger.error("Maximal allowed chunk size is %s. Using the maximum.",
                     _max_chunk_size)
        chunk_size = _max_chunk_size

    # calculate number of necessary loops (Fiware can just extract 10.000 data at a time)
    loops = interval / chunk_size
    loops = math.ceil(loops)

    from_date = to_date - datetime.timedelta(seconds=interval)

    tsd_data = None

    for j in range(loops):
        j += 1
        if j == loops:
            _interval_to_get = interval % chunk_size
        else:
            _interval_to_get = chunk_size
        to_date = from_date + datetime.timedelta(seconds=_interval_to_get)

        i = 0
        logger.debug("Extracting data from %s to %s", from_date, to_date)
        # extract measured and simulated data
        tsd_data_short = None

        for entity_name_attr in entity_name_attributes:
            entity_name, attr_name = entity_name_attr.split("/")
            with QuantumLeapClient(url=ql_url,
                                   fiware_header=fiware_header) as ql_client:
                try:
                    entity_tsd = ql_client.get_entity_attr_values_by_id(
                        entity_id=entity_name,
                        attr_name=attr_name,
                        from_date=str(from_date),
                        to_date=str(to_date)
                    )
                    i += 1
                    entity_tsd_df = entity_tsd.to_pandas()
                    entity_tsd_df = entity_tsd_df.rename(columns={entity_name: entity_name_attr})
                    if i == 1:
                        tsd_data_short = entity_tsd_df
                    else:
                        tsd_data_short = tsd_data_short.join(entity_tsd_df, how="outer")
                except requests.exceptions.HTTPError:
                    logger.error("Could not retrieve data for entity/attr='%s' in interval %s-%s",
                                 entity_name_attr, from_date, to_date)

        if tsd_data_short is not None:
            if tsd_data is None:
                tsd_data = tsd_data_short
            else:
                tsd_data = tsd_data.append(tsd_data_short)

        from_date = to_date

    if tsd_data is None:
        # No data found
        return pd.DataFrame({})

    # removing multi-column
    tsd_data = tsd_data.droplevel(2, axis=1).droplevel(1, axis=1)
    tsd_data = tsd_data.fillna(method="ffill")

    tsd_data.index.name = None
    return tsd_data
