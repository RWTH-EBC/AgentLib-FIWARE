"""

"""
import logging
from typing import List
from datetime import datetime

from filip.clients.ngsi_v2 import ContextBrokerClient
from filip.models.base import FiwareHeader
from pydantic import (
    AnyHttpUrl, Field,
    field_validator, FieldValidationInfo
)

from agentlib import Agent, AgentVariable, AgentVariables, BaseModule, BaseModuleConfig

logger = logging.getLogger(__name__)


class ScheduledServiceToContextBrokerCommunicatorConfig(BaseModuleConfig):

    fiware_header: FiwareHeader = Field(
        default=None,
        title="FIWARE Header",
        description="Meta information for FIWARE's multi tenancy mechanism"
    )
    cb_url: AnyHttpUrl = Field(
        title="Context Broker",
        description="Url of the FIWARE's Context Broker"
    )
    read_entity_attributes: AgentVariables = Field(
        title="Specify which attributes to listen to.",
        default=[],
        description="List of AgentVariables. "
                    "The name is an entity_name/attr_name combination to listen to."
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
    read_interval: float = Field(
        default=5,
        title="Read Interval",
        description="Interval in which the service "
                    "reads the attributes from the context broker"
    )

    @field_validator("read_entity_attributes", "update_entity_attributes")
    @classmethod
    def check_entity_attrs(cls, entity_attrs, info: FieldValidationInfo):
        unique_entities = {}
        # Get unique entity to avoid duplicate subscription in case of,
        # e.g. self.config.read_entity_attributes = ["id1/attr_1", "id1/attr_2"]
        for entity_attr in entity_attrs:
            entity_id, attr_name = entity_attr.name.split("/")
            if entity_id not in unique_entities:
                unique_entities[entity_id] = [attr_name]
            else:
                unique_entities[entity_id].append(attr_name)

        with ContextBrokerClient(
            url=info.data["cb_url"],
            fiware_header=info.data["fiware_header"]
        ) as httpc:
            # Check if the data even exists.
            for entity_id, attrs in unique_entities.items():
                print("Getting entity", entity_id)
                entity = httpc.get_entity(entity_id=entity_id)
                for attr_name in attrs:
                    entity.get_attribute(attr_name)

        return entity_attrs


class ScheduledServiceToContextBrokerCommunicator(BaseModule):
    config: ScheduledServiceToContextBrokerCommunicatorConfig

    def __init__(self, config: dict, agent: Agent):
        super().__init__(config=config, agent=agent)
        self._httpc = ContextBrokerClient(
            url=self.config.cb_url,
            fiware_header=self.config.fiware_header
        )
        self.logger.error(self._httpc.get_version())

    def process(self):
        while True:
            # Check if the data even exists.
            # TODO: Which option is best to get the entities
            #   entity_ids = [entity_attr.name.split("/")[0] for entity_attr in self.config.read_entity_attributes]
            #   self._httpc.get_entity_list()
            for entity_attr in self.config.read_entity_attributes:
                entity_id, attr_name = entity_attr.name.split("/")
                entity = self._httpc.get_entity(entity_id=entity_id)
                self._process_entity_and_send_to_databroker(entity=entity)
            yield self.env.timeout(self.config.read_interval)

    def _process_entity_and_send_to_databroker(self, entity):
        entity_id = entity.id
        for entity_attr in self.config.read_entity_attributes:
            if entity_attr.name.startswith(entity_id):
                attr_name = entity_attr.name.split("/")[-1]
                try:
                    attr = entity.get_attribute(attr_name)
                    time_unix = self._extract_time(attr)
                    self.set(
                        name=entity_attr.name,
                        value=attr.value,
                        timestamp=time_unix
                    )
                    self.logger.debug(
                        "Send variable '%s=%s' at time '%s' into data_broker",
                        entity_attr.alias, attr.value, time_unix
                    )
                except KeyError as err:
                    self.logger.error("Attribute '%s' not in entity '%s'. Error: %s",
                                      attr_name, entity_id, err)

    def _extract_time(self, attr):
        # Extract time information:
        if self.env.config.rt and self.env.config.factor == 1 and "TimeInstant" in attr.metadata:
            time_unix = (datetime.strptime(
                attr.metadata['TimeInstant'].value,
                self.config.time_format
            ) - datetime(1970, 1, 1)).total_seconds()
        else:
            # This case means we simulate faster than real time.
            # In this case, using the time from FIWARE makes no sense
            # as it would result in bad control behaviour, i.e. in a
            # PID controller.
            time_unix = self.env.time
        return time_unix

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
        entity_id, attr_name = name.split("/")
        try:
            entity = self._httpc.get_entity(entity_id=entity_id)
            attr = entity.get_attribute(attribute_name=attr_name)
        except KeyError as err:
            logger.error("Entity-attribute combination %s not found, can't update it."
                         "Error-message: %s", name, err)
            return
        attr.value = variable.value
        self._httpc.update_entity_attribute(
            entity_id=entity_id,
            entity_type=entity.type,
            attr=attr,
            override_metadata=False
        )
        self.logger.debug(
            "Successfully updated entity attribute %s to %s for variable %s! ",
            attr.json(),
            entity.json(include={'service', 'service_path', 'id', 'type'}),
            variable.alias
        )

    def terminate(self):
        """Disconnect subscription ids"""
        for sud_id in self.subscription_ids:
            self._httpc.delete_subscription(subscription_id=sud_id)
        super().terminate()
