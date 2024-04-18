import logging
from typing import List, Dict, Tuple

import numpy as np
from filip.clients.ngsi_v2 import ContextBrokerClient
from filip.models.base import FiwareHeader
from pydantic import (
    AnyHttpUrl, Field, ConfigDict,
    field_validator, FieldValidationInfo
)

from agentlib import Agent, AgentVariable, AgentVariables, BaseModule, BaseModuleConfig, Environment

from agentlib_fiware import utils

logger = logging.getLogger(__name__)


class BaseContextBrokerConfig(BaseModuleConfig):
    model_config = ConfigDict(extra="forbid")

    fiware_header: FiwareHeader = Field(
        default=None,
        title="FIWARE Header",
        description="Meta information for FIWARE's multi tenancy mechanism"
    )
    cb_url: AnyHttpUrl = Field(
        title="Context Broker",
        description="Url of the FIWARE's Context Broker"
    )
    update_entity_attributes: AgentVariables = Field(
        title="Specify which attributes to update in the CB.",
        default=[],
        description="List of AgentVariables. "
                    "The name is an entity_name/attr_name combination to update."
    )
    time_format: str = Field(
        default="%Y-%m-%dT%H:%M:%S.%fZ",
        title="The format to convert fiware "
              "datetime into unix time"
    )
    skip_update_after_x_seconds: float = Field(
        default=np.inf,
        title="Skip update if x seconds too old",
        description="Skip attribute update if the variable was too "
                    "long in the data-broker queue. "
                    "This is a sign of bad connection or bad FIWARE performance."
    )

    @field_validator("update_entity_attributes")
    @classmethod
    def check_entity_attrs(cls, entity_attrs, info: FieldValidationInfo):
        unique_entities = get_unique_entities(entity_attrs)
        with ContextBrokerClient(
                url=info.data["cb_url"],
                fiware_header=info.data["fiware_header"]
        ) as httpc:
            # Check if the data even exists.
            for entity_id, attrs in unique_entities.items():
                entity = httpc.get_entity(entity_id=entity_id)
                for attr_name, _ in attrs:
                    entity.get_attribute(attr_name)

        return entity_attrs


class BaseContextBroker(BaseModule):
    config: BaseContextBrokerConfig

    def __init__(self, config: dict, agent: Agent):
        super().__init__(config=config, agent=agent)
        self._httpc = ContextBrokerClient(
            url=self.config.cb_url,
            fiware_header=self.config.fiware_header
        )
        self.logger.debug("HTTPC version is %s", self._httpc.get_version())

    def register_callbacks(self):
        """
        Registers the callbacks for data stream from other agents.
        """
        for entity_attr in self.config.update_entity_attributes:
            self.agent.data_broker.register_callback(
                alias=entity_attr.alias,
                source=entity_attr.source,
                callback=self._update_callback,
                # Set kwargs to later access without needing
                # to loop over devices and attributes again
                name=entity_attr.name
            )
            self.logger.debug("Registered callback for alias '%s', source %s "
                              "and entity_id/attribute_name='%s'",
                              entity_attr.alias, entity_attr.source, entity_attr.name)

    def _update_callback(
            self,
            variable: AgentVariable,
            name: str
    ):
        """
        Receive the attribute callback from the AgentLib and
        update the attribute in the ContextBroker.
        """
        time_start_update = self.env.time
        time_delay = time_start_update - variable.timestamp
        if time_delay > self.config.skip_update_after_x_seconds:
            self.logger.error("The update of '%s' is %s seconds out of sync, skipping the update",
                              name, time_delay)

        entity_id, attr_name = name.split("/")
        try:
            entity = self._httpc.get_entity(entity_id=entity_id)
            attribute = entity.get_attribute(attribute_name=attr_name)
        except KeyError as err:
            logger.error("Entity-attribute combination %s not found, can't update it."
                         "Error-message: %s", name, err)
            return

        attribute.value = variable.value
        attribute = utils.update_attribute_time_instant(
            attribute=attribute, time_format=self.config.time_format, timestamp=variable.timestamp
        )
        self._httpc.update_entity_attribute(
            entity_id=entity_id,
            entity_type=entity.type,
            attr=attribute,
            override_metadata=True
        )
        self.logger.info(
            "Successfully updated entity attribute %s to %s for variable %s. Took %s seconds",
            attribute.model_dump_json(),
            entity.model_dump_json(include={'service', 'service_path', 'id', 'type'}),
            variable.alias,
            self.env.time - time_start_update
        )


def get_unique_entities(entity_attrs: List[AgentVariable]) -> Dict[str, List[Tuple[str, AgentVariable]]]:
    """
    Get unique entity to avoid duplicate subscription in case of,
    e.g. self.config.read_entity_attributes = ["id1/attr_1", "id1/attr_2"]
    """
    unique_entities = {}
    for entity_attr in entity_attrs:
        entity_id, attr_name = entity_attr.name.split("/")
        if entity_id not in unique_entities:
            unique_entities[entity_id] = [(attr_name, entity_attr)]
        else:
            unique_entities[entity_id].append((attr_name, entity_attr))
    return unique_entities
