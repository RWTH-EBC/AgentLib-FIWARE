import logging

from pydantic import (
    Field,
    field_validator,
    FieldValidationInfo
)
from filip.models.ngsi_v2.context import ContextEntity
from filip.clients.ngsi_v2 import ContextBrokerClient

from agentlib import AgentVariables, AgentVariable

import agentlib_fiware.utils
from agentlib_fiware.modules.context_broker import base

logger = logging.getLogger(__name__)


class ScheduledAttributesContextBrokerConfig(base.BaseContextBrokerConfig):
    read_interval: float = Field(
        default=5,
        title="Read Interval",
        description="Interval in which the service "
                    "reads the attributes from the context broker"
    )

    read_entity_attributes: AgentVariables = Field(
        title="Specify which attributes to listen to.",
        default=[],
        description="List of AgentVariables. "
                    "The name is an entity_name/attr_name combination to listen to."
    )

    @field_validator("read_entity_attributes")
    @classmethod
    def check_read_entity_attrs(cls, entity_attrs, info: FieldValidationInfo):
        return cls.check_entity_attrs(entity_attrs=entity_attrs, info=info)


class ScheduledAttributesContextBroker(base.BaseContextBroker):
    config: ScheduledAttributesContextBrokerConfig

    def process(self):
        while True:
            get_entity_attributes(
                module=self,
                entity_attributes=self.config.read_entity_attributes,
                http_client=self._httpc
            )
            yield self.env.timeout(self.config.read_interval)


def get_entity_attributes(
        module: base.BaseContextBroker,
        entity_attributes: list,
        http_client: ContextBrokerClient
):
    unique_entities = base.get_unique_entities(entity_attributes)
    for entity_id, attributes_variables in unique_entities.items():
        entity = http_client.get_entity(entity_id=entity_id)
        for attr_name, variable in attributes_variables:
            process_entity_attribute_and_send_to_databroker(
                module=module, entity=entity, attr_name=attr_name, variable=variable
            )


def process_entity_attribute_and_send_to_databroker(
        module: base.BaseContextBroker,
        entity: ContextEntity,
        attr_name: str,
        variable: AgentVariable
):
    try:
        attr = entity.get_attribute(attr_name)
    except KeyError as err:
        module.logger.error("Attribute '%s' not in entity '%s'. Error: %s",
                            attr_name, entity.id, err)
        return
    time_unix = agentlib_fiware.utils.extract_time_from_attribute(
        attribute=attr, env=module.env, time_format=module.config.time_format
    )
    module.set(
        name=variable.name,
        value=attr.value,
        timestamp=time_unix
    )
    module.logger.debug(
        "Send variable '%s=%s' at time '%s' into data_broker",
        variable.alias, attr.value, time_unix
    )
