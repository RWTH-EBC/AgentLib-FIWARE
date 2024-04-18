import logging
from typing import List, Union, Optional
from pathlib import Path

from filip.clients.mqtt import IoTAMQTTClient
from filip.models.ngsi_v2.iot import \
    Device, \
    ServiceGroup, \
    PayloadProtocol, \
    DeviceAttribute
from pydantic import (
    ValidationInfo,
    field_validator,
    AnyHttpUrl,
    Field,
    PrivateAttr,
    FilePath
)

from agentlib.modules.communicator.mqtt import \
    Agent, \
    AgentVariable

from agentlib_fiware.modules.iota_mqtt.base import (
    BaseIoTACommunicatorConfig,
    BaseIoTACommunicator
)
from agentlib_fiware.utils import parse_file_as

logger = logging.getLogger(__name__)


class IoTAMQTTConfig(BaseIoTACommunicatorConfig):

    iota_url: AnyHttpUrl = Field(
        default=None,
        title="IoT Agent",
        description="Host of the IoT Agent"
    )
    devices: Union[List[Device], Path] = Field(
        default=[],
        title="FIWARE IoT devices",
        description="List of FIWARE IoT device configurations"
    )
    payload_protocol: PayloadProtocol = Field(
        default=PayloadProtocol.IOTA_JSON
    )
    service_groups: List[ServiceGroup] = Field(
        title="FIWARE IoT device groups",
        description="List of FIWARE IoT device group configurations"
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
            'device',
            'service_path',
            'service'
        )
    )

    @field_validator("devices")
    @classmethod
    def parse_device_list(cls, value):
        if isinstance(value, (Path, str)):
            return parse_file_as(List[Device], value)
        return value

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

    @field_validator("subtopics")
    @classmethod
    def check_subtopics(cls, _):
        """
        Overwrite default subtopics behaviour and
        allow no subtopics for the iotagent.
        """
        logger.warning("%s can not use subtopics. We won't use it.", cls.__name__)
        return []

    @classmethod
    def automatically_select_routing(cls, values):
        """
        Rules applied here:
        1. alias_routing='attribute':
        If the devices contain only non-conflicting attribute and command names
        2. alias_routing='device':
        If the devices contain only non-conflicting device_ids
        3. alias_routing='service_path':
        Never, we assume only one DeviceIoTAMQTTCommunicator is present
        4. alias_routing='service':
        If the devices contain conflicting device_id and attr combinations
        """
        # If not, automatically select one:
        devices = values["devices"]
        # Get all attr_names and device_ids
        attr_names = []
        device_ids = []
        for device in devices:
            for attr in device.attributes + device.commands:
                attr_names.append(attr.name)
                device_ids.append(device.device_id + "/" + attr.name)
        if len(attr_names) == len(set(attr_names)):
            alias_routing = 'attribute'
        elif len(device_ids) == len(set(device_ids)):
            alias_routing = 'device'
        else:
            alias_routing = 'service'
        logger.info("Automatically selected alias_routing='%s' based on devices config.",
                    alias_routing)
        return alias_routing

    def get_alias_for_attribute_name(
            self,
            name: str,
            device_id: str
    ) -> str:
        """
        Based on the routing specified in the config,
        return the alias for the given attribute and device.
        See alias routing doc for more information.
        """
        entry_list = [
            self.fiware_header.service,
            self.fiware_header.service_path.strip('/'),
            device_id,
            name,
        ]
        idx = self.get_routing_index(self.alias_routing)
        # Use short function to avoid if-else branches
        return "/".join(entry_list[(-1-idx):])


class DeviceIoTAMQTTCommunicator(BaseIoTACommunicator):
    config: IoTAMQTTConfig
    mqttc_type = IoTAMQTTClient

    def __init__(self, config: dict, agent: Agent):
        super().__init__(config=config, agent=agent)
        # Register devices and service groups in the own mqtt client
        for group in self.config.service_groups:
            self._mqttc.add_service_group(group)
        for device in self.config.devices:
            self._mqttc.add_device(device)

    def get_all_topics(self):
        subtopics = self.config.subtopics
        for device in self.config.devices:
            topic = f"{device.apikey}/{device.device_id}/cmd"
            if topic not in subtopics:
                subtopics.append(topic)
        return subtopics

    def register_callbacks(self):
        """
        Register all outputs to the callback function.
        """
        for device in self.config.devices:
            for attr in device.attributes:
                alias = self.config.get_alias_for_attribute_name(
                    name=attr.name,
                    device_id=device.device_id
                )
                self.agent.data_broker.register_callback(
                    source=None, alias=alias,
                    callback=self._fiware_callback,
                    # Set kwargs to later access without needing
                    # to loop over devices and attributes again
                    attribute=attr,
                    device_id=device.device_id
                )
                self.logger.debug("Registered callback for alias '%s', "
                                  "attribute '%s' of device '%s'",
                                  alias, attr.name, device.device_id)

    def _fiware_callback(
            self,
            variable: AgentVariable,
            attribute: DeviceAttribute,
            device_id: str
    ):
        """
        Publish the given output to IoTA-Agent
        """
        self.logger.debug("Publishing attribute %s with value %s to mqtt.",
                          attribute.name, variable.value)
        payload = {attribute.object_id: variable.value}
        self._mqttc.publish(device_id=device_id,
                            payload=payload)

    def _message_callback(self, client, userdata, msg):
        """
        Receive an MQTT callback, decode the message and
        send the payload as an AgentVariable into the DataBroker.
        Afterwards, publish the cmd_exe to fiware to indicate if
        the command was a success.
        """
        _, device_id, payload = self._mqttc.get_encoder(
            self.config.payload_protocol).decode_message(msg=msg)
        cmd_name, value = payload.popitem()
        alias = self.config.get_alias_for_attribute_name(
            name=cmd_name,
            device_id=device_id
        )
        variable = AgentVariable(
            name=alias,
            value=value,
            source=self.source,
            timestamp=self.env.time
        )
        self.logger.debug("Received command and sending variable "
                          "with alias '%s' and value '%s' to data_broker.",
                          alias, value)
        self.agent.data_broker.send_variable(variable)

        client.publish(device_id=device_id,
                       command_name=cmd_name,
                       payload={cmd_name: value})
