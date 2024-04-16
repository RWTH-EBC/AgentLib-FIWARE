"""
This package contains modules to
use the agentlib together with FIWARE
"""
from agentlib.utils.plugin_import import ModuleImport


MODULE_TYPES = {
    'iotamqtt.device': ModuleImport(
        import_path="agentlib_fiware.modules.iota_mqtt.device_to_iotagent",
        class_name="DeviceIoTAMQTTCommunicator"
    ),
    'iotamqtt.context_broker': ModuleImport(
        import_path="agentlib_fiware.modules.iota_mqtt.context_broker_to_service",
        class_name="ContextBrokerCommunicator"
    ),
    'context_broker.scheduled_attributes': ModuleImport(
        import_path="agentlib_fiware.modules.context_broker.scheduled_attributes",
        class_name="ScheduledAttributesContextBroker"
    ),
    'context_broker.scheduled_entities': ModuleImport(
        import_path="agentlib_fiware.modules.context_broker.scheduled_entities",
        class_name="ScheduledEntitiesContextBroker"
    ),
    'context_broker.notified_attributes': ModuleImport(
        import_path="agentlib_fiware.modules.context_broker.notified_attributes",
        class_name="NotifiedAttributesContextBroker"
    ),
}
