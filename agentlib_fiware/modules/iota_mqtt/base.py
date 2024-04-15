import warnings
from abc import abstractmethod

from filip.clients.mqtt import IoTAMQTTClient
from filip.models import FiwareHeader

from filip.types import AnyMqttUrl
from paho.mqtt.client import MQTT_CLEAN_START_FIRST_ONLY
from pydantic import ConfigDict, Field, PrivateAttr

from agentlib.modules.communicator.mqtt import \
    AgentVariable, \
    BaseMqttClient, \
    BaseMQTTClientConfig


class FIWARECommunicatorConfig(BaseMQTTClientConfig):
    model_config = ConfigDict(extra="forbid")

    mqtt_url: AnyMqttUrl = Field(
        default=None,
        title="MQTT Broker",
        description="Host if the MQTT Broker for IoT Agent communication"
    )
    fiware_header: FiwareHeader = Field(
        default=None,
        title="FIWARE Header",
        description="Meta information for FIWARE's multi tenancy mechanism"
    )
    fiware_header_digital: FiwareHeader = Field(
        default=None,
        title="FIWARE Header Digital",
        description="Meta information for FIWARE's digital multi tenancy mechanism"
    )
    _routing_options: tuple = PrivateAttr()

    @classmethod
    def super_check_alias_routing(cls, alias_routing, values):
        """
        If no alias_routing is given, automatically select based
        on the given devices a fitting option for the alias_routing parameter.
        """
        # Run automatic selection either way:
        alias_routing_auto = cls.automatically_select_routing(values=values)

        if alias_routing is None:
            alias_routing = alias_routing_auto

        alias_routing = alias_routing.lower()
        routing_idx = cls.get_routing_index(alias_routing)
        if routing_idx < cls.get_routing_index(alias_routing_auto):
            warnings.warn(
                "You selected an alias_routing inferior to the "
                "automatically selected one based on your devices. "
                "Carefully monitor your IoT system to check if every message "
                "and value is correctly mapped between the AgentLib and FIWARE.",
                UserWarning
            )
        if routing_idx == -1:
            raise KeyError(
                f"Given alias_routing '{alias_routing}' is not valid. "
                f"Valid options are: {' ,'.join(cls._routing_options)}"
            )
        return alias_routing

    @classmethod
    def get_routing_index(cls, routing: str):
        """
        Return the index for the given routing option.
        Args:
            routing str: The routing option

        Returns:
            int: The index based on cls._routing_options
        """
        return cls.__private_attributes__["_routing_options"].default.index(routing)

    @abstractmethod
    def get_alias_for_attribute_name(
            self,
            **kwargs
    ) -> str:
        """
        Based on the routing specified in the config,
        return the alias for the given attribute and device.
        See alias_routing doc for more information.
        """
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def automatically_select_routing(cls, values):
        """
        Overwrite this method to automatically select
        routing options based on the values dict from
        the validator.
        """
        raise NotImplementedError


class FIWARECommunicator(BaseMqttClient):
    config: FIWARECommunicatorConfig
    mqttc_type = IoTAMQTTClient

    def connect(self):
        """Connect to the mqtt client"""
        self._mqttc.connect(
            host=self.config.mqtt_url.host,
            port=int(self.config.mqtt_url.port) or 1883,
            keepalive=self.config.keepalive,
            bind_address="",
            bind_port=0,
            clean_start=MQTT_CLEAN_START_FIRST_ONLY,
            properties=None
        )

    def register_callbacks(self):
        """
        Overwrite default Communicator behaviour as we deal with
        callbacks in custom functions after super().__init__ is called..
        """
        pass

    def _callback(self, variable: AgentVariable):
        """
        This callback has to be overwritten by any communicator.
        As we handle specific callbacks in the _fiware_callback
        function, we just don't need this.
        Additionally, we overwrite the register_callbacks function
        so this will never be called.
        """
        pass
