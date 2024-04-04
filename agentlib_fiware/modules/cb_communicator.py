"""

"""
import json
import logging
from typing import List, Union, Dict, AnyStr, Tuple
from datetime import datetime

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
    AnyHttpUrl, Field,
    PrivateAttr, FilePath,
    parse_file_as, validator
)

from agentlib import Agent, AgentVariable
from agentlib.modules.iot.fiware.communicator import (
    FIWARECommunicatorConfig,
    FIWARECommunicator
)

logger = logging.getLogger(__name__)


class ContextBrokerCommunicatorConfig(FIWARECommunicatorConfig):
    cb_url: AnyHttpUrl = Field(
        title="Context Broker",
        description="Url of the FIWARE's Context Broker"
    )
    entities: Union[List[ContextEntity], FilePath] = Field(
        title="Context Entities",
        description="List of Context Entities in the Context Broker that the "
                    "communicator interacts with"
    )
    # Has to be defined after entities to avoid the root validator
    alias_routing: str = Field(
        title="Which routing to use for the AgentVariables alias",
        default=None,
        description="Refer to the docstring of the automatically_select_routing validator"
    )
    _routing_options: tuple = PrivateAttr(
        default=(
            'attribute',
            'entity',
            'service_path',
            'service',
        )
    )
    entities_map: Dict[Tuple[AnyStr, AnyStr], ContextEntity] = Field(
        description="Automatically generated map for "
                    "entities to identify them based on id and type",
        default=None
    )
    time_format: str = Field(
        default="%Y-%m-%dT%H:%M:%S.%fZ",
        title="The format to convert fiware "
              "datetime into unix time"
    )

    @validator("subtopics", always=True)
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

    @validator("entities")
    def parse_device_list(cls, entities, values):
        if isinstance(entities, FilePath):
            entities = parse_file_as(List[ContextEntity], entities)

        with ContextBrokerClient(
            url=values["cb_url"],
            fiware_header=values["fiware_header"]
        ) as httpc:
            # sync with context broker
            for entity in entities:
                entity.add_attributes(
                    httpc.get_entity_attributes(
                        entity_id=entity.id,
                        entity_type=entity.type)
                )

        return entities

    @validator("alias_routing", always=True)
    def validate_alias_routing(cls, alias_routing, values):
        """
        Trigger parent class to avoid root validator
        """
        if "entities" not in values:
            return alias_routing
        return cls.super_check_alias_routing(
            alias_routing=alias_routing,
            values=values
        )

    @validator("entities_map", always=True)
    def create_entities_map(cls, _, values):
        """
        Create the map to identify entities based on id and type.
        """
        if "entities" not in values:
            return {}
        entities_map = {}
        for entity in values["entities"]:
            entities_map[(entity.id, entity.type)] = entity
        return entities_map

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
        return "/".join(entry_list[(-1-idx):])

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


class ContextBrokerCommunicator(FIWARECommunicator):
    config_type = ContextBrokerCommunicatorConfig
    mqttc_type = PahoMQTTClient

    def __init__(self, config: dict, agent: Agent):
        super().__init__(config=config, agent=agent)
        self._httpc = ContextBrokerClient(
            url=self.config.cb_url,
            fiware_header=self.config.fiware_header
        )
        self.subscription_ids: List[str] = []
        self.create_subscription()
        self.register_cb_cmd_callbacks()

    def _connect_callback(self, client, userdata, flags, reasonCode,
                          properties):
        """
        The callback for when the client receives a CONNACK response from the
        server.
        """
        super()._connect_callback(client=client,
                                  userdata=userdata,
                                  flags=flags,
                                  reasonCode=reasonCode,
                                  properties=properties)
        topic = self.config.get_topic()
        self.logger.error("Subscribed to topic %s", topic)
        self._mqttc.subscribe(topic=topic,
                              qos=self.config.qos)

    def create_subscription(self):
        """
        Creates a subscription in the cb which will
        send a message to the specified mqtt broker using
        the defined topic (in the config) each time
        some entity changes.
        """
        topic = self.config.get_topic()

        for entity in self.config.entities:
            entity_pattern = EntityPattern(**entity.dict())
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
                    update=False)
            )

    def _message_callback(self, client, userdata, msg):
        """
        Receive a message from the mqtt broker and send it,
        as long as it matches entities attributes, to the
        data_broker.
        """
        if self.env.t_start is None:
            return  # Not started yet
        payload = Message.parse_raw(msg.payload.decode())
        if payload.subscriptionId not in self.subscription_ids:
            self.logger.debug("Received unregistered subscription! %s not in %s",
                             payload.subscriptionId,
                             self.subscription_ids)
            return
        for item in payload.data:
            entity = self.config.entities_map.get((item.id, item.type))
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
                # Extract time information:
                if self.env.config.rt and self.env.config.factor == 1:
                    # TODO: Remove if fiware enables TimeInstant faster as real-time.
                    if attr.name == "TimeInstant":
                        time_unix = (datetime.strptime(
                            attr.value,
                            self.config.time_format
                        ) - datetime(1970, 1, 1)).total_seconds()
                    elif "TimeInstant" in attr.metadata:
                        time_unix = (datetime.strptime(
                            attr.metadata['TimeInstant'].value,
                            self.config.time_format
                        ) - datetime(1970, 1, 1)).total_seconds()
                    else:
                        time_unix = self.env.time
                else:
                    # This case means we simulate faster than real time.
                    # In this case, using the time from FIWARE makes no sense
                    # as it would result in bad control behaviour, i. e. in a
                    # PID controller.
                    time_unix = self.env.time
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

    def register_cb_cmd_callbacks(self):
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
            cmd.json(),
            entity.json(include={'service', 'service_path', 'id', 'type'}),
            variable.alias
        )

    def terminate(self):
        """Disconnect subscription ids"""
        for sud_id in self.subscription_ids:
            self._httpc.delete_subscription(subscription_id=sud_id)
        super().terminate()
