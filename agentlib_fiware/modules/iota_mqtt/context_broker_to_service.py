"""

"""
import logging
from typing import List, Union, Optional
from pathlib import Path

from filip.clients.ngsi_v2 import ContextBrokerClient
from filip.models.ngsi_v2.context import ContextEntity, NamedCommand
from filip.models.ngsi_v2.subscriptions import \
    EntityPattern, \
    Message, \
    Mqtt, \
    Notification, \
    Subject, \
    Subscription
from paho.mqtt.client import Client as \
    PahoMQTTClient
from pydantic import (
    ValidationInfo, field_validator,
    AnyHttpUrl, Field,
    PrivateAttr
)
from agentlib import Agent, AgentVariable

from agentlib_fiware.modules.iota_mqtt.base import (
    BaseIoTACommunicatorConfig,
    BaseIoTACommunicator
)
from agentlib_fiware import utils

logger = logging.getLogger(__name__)


class ContextBrokerCommunicatorConfig(BaseIoTACommunicatorConfig):
    cb_url: AnyHttpUrl = Field(
        title="Context Broker",
        description="Url of the FIWARE's Context Broker"
    )
    entities: Union[List[ContextEntity], Union[Path, str]] = Field(
        title="Context Entities",
        description="List of Context Entities in the Context Broker that the "
                    "communicator interacts with"
    )
    # Has to be defined after entities to avoid the root validator
    alias_routing: Optional[str] = Field(
        title="Which routing to use for the AgentVariables alias",
        default=None,
        description="Refer to the docstring of the automatically_select_routing validator",
        validate_default=True
    )
    _routing_options: tuple = PrivateAttr(
        default=(
            'attribute',
            'entity',
            'service_path',
            'service',
        )
    )
    time_format: str = Field(
        default="%Y-%m-%dT%H:%M:%S.%fZ",
        title="The format to convert fiware "
              "datetime into unix time"
    )

    @field_validator("subtopics")
    @classmethod
    def check_subtopics(cls, subtopics):
        """
        Overwrite default subtopics behaviour and
        allow only one subtopic for the cb subscription.
        """
        if isinstance(subtopics, str):
            return [subtopics]
        if subtopics is None:
            return []
        if len(subtopics) > 1:
            raise IndexError(f"{cls.__name__} only allows for a "
                             f"single subtopic to define the notifications.")
        if len(subtopics) == 1:
            return subtopics
        return []

    @field_validator("entities")
    def parse_device_list(cls, entities, info: ValidationInfo):
        if isinstance(entities, (Path, str)):
            entities = utils.parse_file_as(List[ContextEntity], entities)

        with ContextBrokerClient(
                url=info.data["cb_url"],
                fiware_header=info.data["fiware_header"]
        ) as httpc:
            # sync with context broker
            for entity in entities:
                entity.add_attributes(
                    httpc.get_entity_attributes(
                        entity_id=entity.id,
                        entity_type=entity.type)
                )

        return entities

    @field_validator("alias_routing")
    @classmethod
    def validate_alias_routing(cls, alias_routing, info: ValidationInfo):
        """
        Trigger parent class to avoid root validator
        """
        return cls.super_check_alias_routing(
            alias_routing=alias_routing,
            values=info.data
        )

    @classmethod
    def automatically_select_routing(cls, values):
        """
        Rules applied here:
        1. alias_routing='attribute':
        If the devices contain only non-conflicting attribute names
        2. alias_routing='entity':
        If the devices contain only non-conflicting entity_names
        3. alias_routing='service_path':
        Never, we assume only one ContextBrokerCommunicator is present
        4. alias_routing='service':
        If the entities contain conflicting entity_names and attr combinations
        """
        # If not, automatically select one:
        entities = values['entities']
        # Get all attr_names and device_ids
        attr_names = []
        entitiy_ids = []
        for entity in entities:
            for attr in entity.get_properties():
                if attr.name == "TimeInstant":
                    continue
                attr_names.append(attr.name)
                entitiy_ids.append(entity.id + "/" + attr.name)
        if len(attr_names) == len(set(attr_names)):
            alias_routing = 'attribute'
        elif len(entitiy_ids) == len(set(entitiy_ids)):
            alias_routing = 'entity'
        else:
            alias_routing = 'service'
        logger.info("Automatically selected alias_routing='%s' based on devices config.",
                    alias_routing)
        return alias_routing

    def get_alias_for_attribute_name(
            self,
            name: str,
            entity_name: str
    ) -> str:
        """
        Based on the routing specified in the config,
        return the alias for the given attribute and device.
        See alias routing doc for more information.
        """
        entry_list = [
            self.fiware_header.service,
            self.fiware_header.service_path.strip('/'),
            entity_name,
            name,
        ]
        idx = self.get_routing_index(self.alias_routing)
        # Use short function to avoid if-else branches
        return "/".join(entry_list[(-1 - idx):])

    def get_topic(self):
        """Get the subscription topic"""
        if self.subtopics:
            return self.subtopics[0]
        else:
            return "/".join([
                self.prefix,
                self.fiware_header.service.strip("/"),
                self.fiware_header.service_path.strip("/"),
                "ContextBrokerSubscriptions"
            ])


class ContextBrokerCommunicator(BaseIoTACommunicator):
    config: ContextBrokerCommunicatorConfig
    mqttc_type = PahoMQTTClient

    def __init__(self, config: dict, agent: Agent):
        super().__init__(config=config, agent=agent)
        # Create entities map
        self.entities_map = {}
        for entity in self.config.entities:
            self.entities_map[(entity.id, entity.type)] = entity

        self._httpc = ContextBrokerClient(
            url=self.config.cb_url,
            fiware_header=self.config.fiware_header
        )
        self.subscription_ids: List[str] = []
        self.create_subscription()

    def get_all_topics(self):
        return [self.config.get_topic() + "/#"]

    def create_subscription(self):
        """
        Creates a subscription in the cb which will
        publish a message to the specified mqtt broker using
        the defined topic (in the config) each time
        something in the entity changes.
        """
        topic = self.config.get_topic()

        for entity in self.config.entities:
            entity_pattern = EntityPattern(**entity.model_dump())
            sub = Subscription(
                description=f"{self.source}",
                subject=Subject(
                    entities=[entity_pattern]
                ),
                notification=Notification(
                    mqtt=Mqtt(url=self.config.mqtt_url,
                              topic=topic)
                )
            )
            self.subscription_ids.append(
                self._httpc.post_subscription(
                    subscription=sub,
                    update=True)
            )

    def _message_callback(self, client, userdata, msg):
        """
        Receive a message from the mqtt broker and send it,
        as long as it matches entities attributes, to the
        data_broker.
        """
        payload = Message.model_validate_json(msg.payload.decode())
        if payload.subscriptionId not in self.subscription_ids:
            self.logger.debug("Received unregistered subscription! %s not in %s",
                              payload.subscriptionId,
                              self.subscription_ids)
            return
        for item in payload.data:
            entity = self.entities_map.get((item.id, item.type))
            if entity is None:
                self.logger.error("Received item for (%s, %s) does not match any entity.",
                                  item.id, item.type)
                return

            props = item.get_properties()
            cmds = item.get_commands()
            self.logger.debug("Found entity %s with properties %s",
                              entity.id, [prop.name for prop in props])
            for attr in [attr for attr in props if attr not in cmds]:
                alias = self.config.get_alias_for_attribute_name(
                    name=attr.name,
                    entity_name=entity.id
                )
                time_unix = utils.extract_time_from_attribute(
                    attribute=attr, time_format=self.config.time_format, env=self.env
                )
                self.agent.data_broker.send_variable(
                    AgentVariable(
                        name=alias,
                        value=attr.value,
                        source=self.source,
                        timestamp=time_unix
                    )
                )
                self.logger.debug(
                    "Send variable '%s=%s' at time '%s' into data_broker",
                    alias, attr.value, time_unix
                )

    def register_callbacks(self):
        """
        Registers the callbacks for data stream from other agents.
        """
        for entity in self.config.entities:
            for cmd in entity.get_commands():
                alias = self.config.get_alias_for_attribute_name(
                    name=cmd.name,
                    entity_name=entity.id
                )
                self.agent.data_broker.register_callback(
                    alias=alias,
                    callback=self._cmd_callback,
                    # Set kwargs to later access without needing
                    # to loop over devices and attributes again
                    name=cmd.name,
                    entity=entity
                )
                self.logger.debug("Registered callback for alias '%s', "
                                  "command '%s' of entity '%s'",
                                  alias, cmd.name, entity.id)

    def _cmd_callback(
            self,
            variable: AgentVariable,
            name: str,
            entity: ContextEntity
    ):
        """
        Receive the command callback from the AgentLib and
        send it to the ContextBroker.
        """
        cmd = NamedCommand(name=name, value=variable.value)
        self._httpc.post_command(entity_id=entity.id,
                                 entity_type=entity.type,
                                 command=cmd)
        self.logger.debug(
            "Successfully send command %s to %s for variable %s! ",
            cmd.model_dump_json(),
            entity.model_dump_json(include={'service', 'service_path', 'id', 'type'}),
            variable.alias
        )

    def terminate(self):
        """Disconnect subscription ids"""
        for sud_id in self.subscription_ids:
            self._httpc.delete_subscription(subscription_id=sud_id)
        super().terminate()
