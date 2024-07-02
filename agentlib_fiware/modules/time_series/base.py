from pydantic import Field
from agentlib import AgentVariable, BaseModule, BaseModuleConfig


class BaseTimeSeriesAcquisitionConfig(BaseModuleConfig):
    time_series_data: AgentVariable = AgentVariable(
        name="time_series_data",
        type="str",
        shared=True
    )
    interval: float = Field(
        default=86400 * 30
    )


class BaseTimeSeriesAcquisition(BaseModule):
    config_type = BaseTimeSeriesAcquisitionConfig
