import logging

from pydantic import (
    Field,
    field_validator,
    FieldValidationInfo
)

from agentlib import AgentVariables

from agentlib_fiware.modules.scheduled_context_broker import base

logger = logging.getLogger(__name__)


class AttributesScheduledServiceToContextBrokerConfig(base.BaseScheduledServiceToContextBrokerConfig):
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


class AttributesScheduledServiceToContextBroker(base.BaseScheduledServiceToContextBroker):
    config: AttributesScheduledServiceToContextBrokerConfig

    def process(self):
        while True:
            unique_entities = base.get_unique_entities(self.config.read_entity_attributes)
            for entity_id, attributes_variables in unique_entities:
                entity = self._httpc.get_entity(entity_id=entity_id)
                for attr_name, variable in attributes_variables:
                    try:
                        attr = entity.get_attribute(attr_name)
                    except KeyError as err:
                        self.logger.error("Attribute '%s' not in entity '%s'. Error: %s",
                                          attr_name, entity_id, err)
                        continue
                    time_unix = base.extract_time_from_attribute(
                        attribute=attr, env=self.env, time_format=self.config.time_format
                    )
                    self.set(
                        name=variable.name,
                        value=attr.value,
                        timestamp=time_unix
                    )
                    self.logger.debug(
                        "Send variable '%s=%s' at time '%s' into data_broker",
                        variable.alias, attr.value, time_unix
                    )

                self._process_entity_and_send_to_databroker(entity=entity)
            yield self.env.timeout(self.config.read_interval)
