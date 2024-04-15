import logging
from typing import List

from filip.clients.ngsi_v2 import ContextBrokerClient
from filip.models.base import FiwareHeader
from filip.models.ngsi_v2.subscriptions import \
    EntityPattern, \
    Message, \
    Mqtt, \
    Notification, \
    Subject, \
    Subscription
from filip.types import AnyMqttUrl
from paho.mqtt.client import Client as \
    PahoMQTTClient
from pydantic import (
    AnyHttpUrl, Field,
    validator, FieldValidationInfo, ConfigDict
)
# TODO: Inherit from scheduled broker
from agentlib import Agent, AgentVariable, AgentVariables
from agentlib.modules.iot.fiware.communicator import FIWARECommunicator
from agentlib.modules.communicator.mqtt import BaseMQTTClientConfig

logger = logging.getLogger(__name__)


class ServiceToContextBrokerCommunicatorConfig(BaseMQTTClientConfig):

    model_config = ConfigDict(extra="forbid")

    mqtt_url: AnyMqttUrl = Field(
        default=None,
        title="MQTT Broker",
        description="Host if the MQTT Broker for IoT Agent communication"
    )

    def get_topic(self):
        """Get the subscription topic"""
        if self.subtopics:
            return self.subtopics[0]
        return "/".join([
            self.prefix,
            self.fiware_header.service.strip("/"),
            self.fiware_header.service_path.strip("/"),
            "ContextBrokerSubscriptions",
            self.agent_id + "_" + self.module_id
        ])



class ServiceToContextBrokerCommunicator(FIWARECommunicator):
    """
    This communicator enables the communication between
    modules of the AgentLib (i.e. Services) and the
    context broker.

    The Module can:
    - Update attributes based on a callback in the AgentLib
    - Receive a changed attribute status in the context broker.
    """
    config_type = ServiceToContextBrokerCommunicatorConfig
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
        self.logger.info("Subscribing to %s", self.config.get_topic() + "/#")
        self._mqttc.subscribe(topic=self.config.get_topic() + "/#",
                              qos=self.config.qos)

    def create_subscription(self):
        """
        Creates a subscription in the cb which will
        send a message to the specified mqtt broker using
        the defined topic (in the config) each time
        some entity changes.
        """
        topic = self.config.get_topic()

        unique_entities = {}
        # Get unique entity to avoid duplicate subscription in case of,
        # e.g. self.config.read_entity_attributes = ["id1/attr_1", "id1/attr_2"]
        for entity_attr in self.config.read_entity_attributes:
            entity_id, attr_name = entity_attr.name.split("/")
            if entity_id not in unique_entities:
                unique_entities[entity_id] = [attr_name]
            else:
                unique_entities[entity_id].append(attr_name)
        for entity_id, attrs in unique_entities.items():
            entity = self._httpc.get_entity(entity_id=entity_id)
            entity_pattern = EntityPattern(**entity.dict())
            # Post new subscription
            sub = Subscription(
                description=f"{self.source}",
                subject=Subject(
                    entities=[entity_pattern],
                    condition={"attrs": attrs}
                ),
                notification=Notification(
                    mqtt=Mqtt(url=self.config.mqtt_url,
                              topic=topic + "/" + entity.id)
                )
            )
            self.logger.info("Posting subscription to topic '%s' with sub'=%s'",
                             topic, sub.json())
            self.subscription_ids.append(
                self._httpc.post_subscription(
                    subscription=sub, update=False)
            )

    def process(self):
        with ContextBrokerClient(
            url=self.config.cb_url,
            fiware_header=self.config.fiware_header
        ) as httpc:
            # Check if the data even exists.
            for entity_attr in self.config.read_entity_attributes:
                entity_id, attr_name = entity_attr.name.split("/")
                entity = httpc.get_entity(entity_id=entity_id)
                self._process_entity_and_send_to_databroker(entity=entity)
        yield self.env.event()

    def _message_callback(self, client, userdata, msg):
        """
        Receive a message from the mqtt broker and send it,
        as long as it matches entities attributes, to the
        data_broker.
        """
        if self.env.t_start is None:
            return  # Agent has not started yet

        payload = Message.parse_raw(msg.payload.decode())
        if payload.subscriptionId not in self.subscription_ids:
            self.logger.debug("Received unregistered subscription! %s not in %s",
                              payload.subscriptionId,
                              self.subscription_ids)
            return
        for item in payload.data:
            self._process_entity_and_send_to_databroker(entity=item)

    def terminate(self):
        """Disconnect subscription ids"""
        for sud_id in self.subscription_ids:
            self._httpc.delete_subscription(subscription_id=sud_id)
        super().terminate()
