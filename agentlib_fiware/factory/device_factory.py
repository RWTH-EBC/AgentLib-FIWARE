import json
import logging
from pathlib import Path
from pydantic import \
    field_validator, Field, \
    FilePath, \
    FieldValidationInfo

from typing import Dict, List, Union, Tuple, Optional
from filip.models.base import DataType
from filip.models.ngsi_v2.iot import \
    Device, \
    DeviceAttribute, \
    DeviceCommand, \
    PayloadProtocol, \
    TransportProtocol, \
    ServiceGroup
from filip.models.ngsi_v2.context import ContextEntity
from filip.clients.ngsi_v2.iota import IoTAClient
from rdflib import URIRef

from agentlib.core.module import BaseModuleConfig
from agentlib import AgentVariable

from agentlib_fiware.modules.iota_mqtt.device_to_iotagent import DeviceIoTAMQTTCommunicator
from agentlib_fiware.utils import parse_file_as

logger = logging.getLogger(__name__)


class FiwareIoTADeviceFactoryConfig(ServiceGroup):
    payload_protocol: PayloadProtocol = Field(
        default=PayloadProtocol.IOTA_JSON,
        validate_default=True
    )
    device_filename: Union[FilePath, str] = Field(
        default=None,
        description="File where to store device configurations"
    )
    resource: str = Field(
        default="/iot/json",
        description="string representing the Southbound resource that will be "
                    "used to assign a type to a device  (e.g.: pathname in the "
                    "southbound port)."
    )

    @field_validator('device_filename')
    @classmethod
    def check_nonexisting_device_file(cls, device_filename):
        """Check if the device_filename is a .json file."""
        if device_filename:
            path = Path(device_filename)
            if not path.suffix == '.json':
                raise TypeError(f'Given device_filename ends with '
                                f'{path.suffix} '
                                f'but should be a .json file.')
            return path


class FiwareIoTADeviceFactory:
    """
    Class to add a device factory to FIWARE modules
    """
    def __init__(self, config: Union[Dict, FiwareIoTADeviceFactoryConfig]):
        self.logger = logging.getLogger(
            f"({self.__class__.__module__}.{self.__class__.__name__})"
        )
        self.config = config
        self._devices: Dict[str, Device] = {}

    @property
    def config(self) -> FiwareIoTADeviceFactoryConfig:
        """
        The current config.

        Returns:
            BaseModuleConfigClass: Config of type self.config_type
        """
        return self._config

    @config.setter
    def config(self, config: Union[FiwareIoTADeviceFactoryConfig, dict, str]):
        """Set a new config"""
        if isinstance(config, FiwareIoTADeviceFactoryConfig):
            self._config = config.copy()
        elif isinstance(config, str):
            self._config = FiwareIoTADeviceFactoryConfig.parse_raw(config)
        else:
            self._config = \
                FiwareIoTADeviceFactoryConfig.parse_obj(config.copy())

    @property
    def devices(self):
        """
        Returns as list of all registered device configurations
        Returns:

        """
        return list(self._devices.values())

    def to_file(self, filename: str = None, update=False):
        """
        Writes device list to file

        Args:
            update: if `True` current file content will be overwritten

        Returns:

        """
        path = filename or self.config.device_filename
        path = Path(path)
        path.touch(exist_ok=True)
        data = self.devices

        if not update:
            data = parse_file_as(List[Device], path)
            device_file_ids = [device.device_id for device in data]

            for device in self.devices:
                if device.device_id in device_file_ids:
                    self.logger.warning("Duplicate device found! Will not "
                                        "update %s", device.device_id)
                else:
                    data.append(device)

        with path.open('w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)

    def create_device(
            self,
            device_id: str = None,
            *,
            attribute_variables: List[AgentVariable] = None,
            command_variables: List[AgentVariable] = None,
            apikey: str = None,
            entity_name: str = None,
            entity_type: str = None,
            payload_protocol: Union[PayloadProtocol, str] = None,
            rdf_class: URIRef = None,
            service_group: ServiceGroup = None):

        if not service_group:
            service_group = ServiceGroup(**self.config.dict())
        if not device_id:
            if service_group.defaultEntityNameConjunction:
                pass
            else:
                raise AttributeError("Missing device_id!")

        attributes = []
        if attribute_variables:
            attributes: List[DeviceAttribute] = \
                [DeviceAttribute(name=var.alias,
                                 object_id=var.alias,
                                 type=var.type or DataType.NUMBER)
                 for var in attribute_variables]

        commands = []
        if command_variables:
            commands: List[DeviceCommand] = \
                [DeviceCommand(name=var.alias,
                               type=var.type or DataType.NUMBER)
                 for var in command_variables]

        device = Device(
            device_id=device_id,
            entity_name=entity_name or device_id,
            entity_type=entity_type or rdf_class or service_group.entity_type,
            attributes=attributes or service_group.attributes,
            commands=commands or service_group.commands,
            transport=TransportProtocol.MQTT,
            protocol=payload_protocol or self.config.payload_protocol,
            apikey=apikey or service_group.apikey)

        self._devices.update({device.device_id: device})


def generate_emulator_agent(
        iotagent_cfg: Union[str, DeviceIoTAMQTTCommunicator],
        module_cfg: Union[str, BaseModuleConfig],
        device_factory_commands: FiwareIoTADeviceFactoryConfig,
        device_factory_attributes: FiwareIoTADeviceFactoryConfig,
        commands_field: str = "inputs",
        attributes_field: str = "outputs",
        agent_id: str = "FiwareEmulator",
        filepath: str = None,
        create_cb_communicator: bool = True,
        filepath_cb_communicator: str = None,
        yes_to_user_input: bool = False
) -> Tuple[Dict, Dict]:
    """
    Function to generate an emulator agent.

    Args:
        iotagent_cfg (str, dict):
            Path to or config of the FIWAREIoTAMQTT module
        module_cfg (str, dict):
            Path to or config of any valid AgentLib module
        device_factory_commands FiwareIoTADeviceFactoryConfig:
            Config for the device factory class for all commands
        device_factory_attributes FiwareIoTADeviceFactoryConfig:
            Config for the device factory class for all attributes
        commands_field str:
            Name of the field in the ``module_cfg`` to be used as commands
            Example is "inputs"
        attributes_field str:
            Name of the field in the ``module_cfg`` to be used as attributes.
            Example is "outputs"
        agent_id str:
            ID of the Emulator agent being created
        filepath:
            If given, config of the emulator agent will be stored in the
            given path
        create_cb_communicator bool:
            Whether to create a module config for the cb_communicator.
            You will need this config to use it in other agents in order
            to communicate to the Emulator agent.
        filepath_cb_communicator str:
            If given, config of the cb_communicator will be stored in the
            given path
        yes_to_user_input bool:
            If False, the user is asked via `input` if existing objects should
            be updated.
    Returns:
        Dict: Config of the agent
        Dict: Config of the ContextBroker-Communicator module.
              Only created if create_cb_communicator=True

    Examples:
    See ``agentlib\examples\multi-agent-systems\iot\device_factory.py``
    """

    if isinstance(module_cfg, str):
        with open(module_cfg, "r") as fp:
            module_cfg = json.load(fp)
    elif isinstance(module_cfg, BaseModuleConfig):
        module_cfg = module_cfg.dict()
    else:
        raise TypeError("Given module_cfg is neither str nor a BaseModuleConfig.")

    attribute_factory = FiwareIoTADeviceFactory(
        config=device_factory_commands
    )

    for var in module_cfg[attributes_field]:
        var = AgentVariable(**var)
        attribute_factory.create_device(
            device_id=var.alias,
            entity_type="Sensor",
            attribute_variables=[var]
        )

    commands_factory = FiwareIoTADeviceFactory(
        config=device_factory_attributes)

    for var in module_cfg[commands_field]:
        var = AgentVariable(**var)
        var.type = "command"
        commands_factory.create_device(
            device_id=var.alias,
            entity_type="Actuator",
            command_variables=[var]
        )

    service_groups = [ServiceGroup(**sp.dict(include={"apikey", "resource"}))
                      for sp in [device_factory_commands, device_factory_attributes]]
    if service_groups[0] == service_groups[1]:
        service_groups = [service_groups[0]]
    devices = commands_factory.devices + attribute_factory.devices

    # Post devices:
    # First create dummy agent:
    if isinstance(iotagent_cfg, str):
        with open(iotagent_cfg, "r") as fp:
            iotagent_cfg = json.load(fp)
        if "module_id" not in iotagent_cfg:
            iotagent_cfg["module_id"] = "iotagent"
        if "type" not in iotagent_cfg:
            iotagent_cfg["type"] = "agentlib_fiware.iotamqtt.device"
    elif isinstance(iotagent_cfg, FIWAREIoTAMQTTConfig):
        iotagent_cfg = iotagent_cfg.dict()
    else:
        raise TypeError("Given iotagent_cfg is neither str nor FIWAREIoTAMQTTConfig.")
    iotagent_cfg["devices"] = []
    iotagent_cfg["service_groups"] = []
    httpc = IoTAClient(url=iotagent_cfg["iota_url"],
                       fiware_header=iotagent_cfg["fiware_header"])

    # Provision groups:
    try:
        httpc.post_groups(service_groups=service_groups, update=False)
    except IOError as err:
        if not yes_to_user_input:
            msg = f"Could not post service groups due to error: {err}. " \
                  f"Do you want to update existing service groups? (y/n)"
            user_inp = input(msg)
            _update = user_inp.lower() == "y"
        else:
            _update = True
        if _update:
            httpc.post_groups(service_groups=service_groups, update=True)
        else:
            logging.info("Taking input as a no.")
            for group in service_groups:
                logger.info(
                    "The following group config will be used:\n %s",
                    httpc.get_group(
                        resource=group.resource,
                        apikey=group.apikey).model_dump_json(indent=2)
                )

    # Provision devices:
    try:
        httpc.post_devices(devices=devices, update=False)
    except IOError as err:
        msg = f"Could not post devices due to error: {err}. " \
              f"Do you want to update existing devices? (y/n)"
        user_inp = input(msg)
        if user_inp.lower() == "y":
            httpc.post_devices(devices=devices, update=True)
        else:
            logger.info("Taking input as a no.")
            for dev in devices:
                logging.info(
                    "The following device config will be used:\n %s",
                    httpc.get_device(
                        device_id=dev.device_id).model_dump_json(indent=2)
                )

    # Update configs
    iotagent_cfg["devices"] = [d.dict() for d in devices]
    iotagent_cfg["service_groups"] = [s.dict() for s in service_groups]

    # Generate agent config
    agent_cfg = {"id": agent_id,
                 "modules": [
                     module_cfg,
                     iotagent_cfg
                 ]}
    # Check if to save.
    if filepath is not None:
        with open(filepath, "w+") as file:
            json.dump(agent_cfg, file, indent=4)

    if not create_cb_communicator:
        return agent_cfg, {}

    cb_cfg = {
        "module_id": "agentlib_fiware.iotamqtt.context_broker",
        "type": "agentlib_fiware.iotamqtt.context_broker"
    }
    # Add fiware_header
    cb_cfg["fiware_header"] = iotagent_cfg["fiware_header"]
    # Add default cb_url
    cb_cfg["cb_url"] = ":".join(iotagent_cfg["iota_url"].split(":")[:-1] + ["1026"])
    # Add default mqtt_url
    cb_cfg["mqtt_url"] = iotagent_cfg["mqtt_url"]
    # Add entities
    cb_cfg["entities"] = []
    for device in devices:
        cb_cfg["entities"].append(
            ContextEntity(
                id=device.entity_name,
                type=device.entity_type
            ).dict()
        )

    # Check if to save.
    if filepath_cb_communicator is not None:
        with open(filepath_cb_communicator, "w+") as file:
            json.dump(cb_cfg, file, indent=4)

    return agent_cfg, cb_cfg
