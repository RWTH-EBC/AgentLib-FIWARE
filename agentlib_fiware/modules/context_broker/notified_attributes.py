import logging

from typing import List

from agentlib.modules.communicator.communicator import CommunicationDict
from paho.mqtt.client import MQTT_CLEAN_START_FIRST_ONLY
from pydantic import (
    Field,
    field_validator,
    ValidationInfo
)

from agentlib import AgentVariables, Agent
from filip.custom_types import AnyMqttUrl
from filip.models.ngsi_v2.subscriptions import \
    EntityPattern, \
    Message, \
    Mqtt, \
    Notification, \
    Subject, \
    Subscription

from agentlib.modules.communicator.mqtt import \
    BaseMqttClient, \
    BaseMQTTClientConfig
from agentlib_fiware.modules.context_broker import base, scheduled_attributes

logger = logging.getLogger(__name__)


class NotifiedAttributesContextBrokerConfig(base.BaseContextBrokerConfig, BaseMQTTClientConfig):
    read_entity_attributes: AgentVariables = Field(
        title="Specify which attributes to listen to.",
        default=[],
        description="List of AgentVariables. "
                    "The name is an entity_name/attr_name combination to listen to."
    )

    mqtt_url: AnyMqttUrl = Field(
        default=None,
        title="MQTT Broker",
        description="Host if the MQTT Broker for IoT Agent communication"
    )

    @field_validator("read_entity_attributes")
    @classmethod
    def check_read_entity_attrs(cls, entity_attrs, info: ValidationInfo):
        return cls.check_entity_attrs(entity_attrs=entity_attrs, info=info)


class NotifiedAttributesContextBroker(base.BaseContextBroker, BaseMqttClient):
    """
    This communicator enables the communication between
    modules of the AgentLib (i.e. Services) and the
    context broker.

    The Module can:
    - Update attributes based on a callback in the AgentLib
    - Receive a changed attribute status in the context broker.
    """

    config: NotifiedAttributesContextBrokerConfig

    def __init__(self, config: dict, agent: Agent):
        super().__init__(config=config, agent=agent)
        self._unique_entities = base.get_unique_entities(self.config.read_entity_attributes)
        self.subscription_ids: List[str] = []
        self.create_subscription()

    @property
    def url(self) -> AnyMqttUrl:
        return self.config.mqtt_url

    def connect(self):
        port = self.url.port
        if port is None:
            port = 1883
        else:
            port = int(port)
        self._mqttc.connect(
            host=self.url.host,
            port=port,
            keepalive=self.config.keepalive,
            bind_address="",
            bind_port=0,
            clean_start=MQTT_CLEAN_START_FIRST_ONLY,
            properties=None,
        )

    def _send(self, payload: CommunicationDict):
        pass

    def get_topic(self):
        """Get the subscription topic"""
        if self.config.subtopics:
            return self.config.subtopics[0]
        return "/".join([
            self.config.prefix,
            self.config.fiware_header.service.strip("/"),
            self.config.fiware_header.service_path.strip("/"),
            "ContextBrokerSubscriptions",
            self.agent.id + "_" + self.id
        ])

    def get_all_topics(self):
        return [self.get_topic() + "/#"]

    def create_subscription(self):
        """
        Creates a subscription in the cb which will
        send a message to the specified mqtt broker using
        the defined topic (in the config) each time
        some entity changes.
        """
        topic = self.get_topic()
        for entity_id, attrs in self._unique_entities.items():
            entity = self._httpc.get_entity(entity_id=entity_id)
            entity_pattern = EntityPattern(**entity.model_dump())
            # Post new subscription
            sub = Subscription(
                description=f"{self.source}",
                subject=Subject(
                    entities=[entity_pattern],
                    condition={"attrs": [attr_tuple[0] for attr_tuple in attrs]}
                ),
                notification=Notification(
                    mqtt=Mqtt(url=self.config.mqtt_url,
                              topic=topic + "/" + entity.id)
                )
            )
            self.logger.info("Posting subscription to topic '%s' with sub'=%s'",
                             topic, sub.model_dump_json())
            self.subscription_ids.append(
                self._httpc.post_subscription(
                    subscription=sub, update=True)
            )

    def process(self):
        # Get current value
        scheduled_attributes.get_entity_attributes(
            module=self,
            entity_attributes=self.config.read_entity_attributes,
            http_client=self._httpc
        )
        yield self.env.event()

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
        for entity in payload.data:
            for attr_name, variable in self._unique_entities[entity.id]:
                scheduled_attributes.process_entity_attribute_and_send_to_databroker(
                    module=self,
                    entity=entity,
                    variable=variable,
                    attr_name=attr_name
                )
