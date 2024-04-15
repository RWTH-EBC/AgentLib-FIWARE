from datetime import datetime

from filip.models.ngsi_v2.context import NamedMetadata, ContextAttribute

from agentlib import Environment


def extract_time_from_attribute(attribute: ContextAttribute, env: Environment, time_format: str):
    # Extract time information:
    if env.config.rt and env.config.factor == 1 and "TimeInstant" in attribute.metadata:
        time_unix = (datetime.strptime(
            attribute.metadata['TimeInstant'].value,
            time_format
        ) - datetime(1970, 1, 1)).total_seconds()
    else:
        # This case means we simulate faster than real time.
        # In this case, using the time from FIWARE makes no sense
        # as it would result in bad control behaviour, i.e. in a
        # PID controller.
        time_unix = env.time
    return time_unix


def update_attribute_time_instant(attribute: ContextAttribute, timestamp: float, time_format: str):
    if "TimeInstant" in attribute.metadata:
        attribute.metadata["TimeInstant"] = NamedMetadata(
            name="TimeInstant",
            type="DateTime",
            value=datetime.fromtimestamp(timestamp).strftime(time_format)
        )
    return attribute
