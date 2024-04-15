"""
Custom module to only change the value being send
by the service_to_cb module.
"""
import logging

from filip.clients.ngsi_v2 import ContextBrokerClient

from pydantic import (
    Field,
    field_validator,
    FieldValidationInfo
)

from agentlib import AgentVariables

from agentlib_fiware.modules.scheduled_context_broker import base

logger = logging.getLogger(__name__)


class EntitiesScheduledServiceToContextBrokerConfig(base.BaseScheduledServiceToContextBrokerConfig):
    read_entities: AgentVariables = Field(
        title="Specify which attributes to listen to.",
        default=[],
        description="List of AgentVariables. "
                    "The name is a entity_id to listen to."
    )

    @field_validator("read_entities")
    @classmethod
    def check_entities(cls, entities, info: FieldValidationInfo):
        with ContextBrokerClient(
                url=info.data["cb_url"],
                fiware_header=info.data["fiware_header"]
        ) as httpc:
            # Check if the data even exists.
            for entity_var in entities:
                httpc.get_entity(entity_id=entity_var.name)

        return entity_var


class EntitiesScheduledServiceToContextBroker(base.BaseScheduledServiceToContextBroker):
    config: EntitiesScheduledServiceToContextBrokerConfig

    def process(self):
        while True:
            for entity_variable in self.config.read_entities:
                try:
                    entity = self._httpc.get_entity(entity_id=entity_variable.name)
                except KeyError:
                    self.logger.error("Entity '%s' not in fiware header '%s'",
                                      entity_variable.name, self.config.fiware_header)
                    return
                self.set(
                    name=entity_variable.name,
                    value=entity
                )
                self.logger.info(
                    "Send entity '%s' with alias %s into data_broker",
                    entity.id, entity_variable.alias
                )
            yield self.env.timeout(self.config.read_interval)
