"""
This package contains modules to
use the agentlib together with FIWARE
"""
from agentlib.utils.plugin_import import ModuleImport

MODULE_TYPES = {
    'service_to_cb': ModuleImport(
        import_path="digital_twin_services.communicator.service_to_cb",
        class_name="ContextBrokerCommunicator"
    ),
    'scheduled_service_to_cb': ModuleImport(
        import_path="digital_twin_services.communicator.scheduled_service_to_cb",
        class_name="ScheduledContextBrokerCommunicator"
    ),
    'fiware_data_acquisition': ModuleImport(
        import_path="digital_twin_services.data_acquisition.fiware",
        class_name="FiwareDataAcquisition"
    ),
    'scheduled_service_to_cb': ModuleImport(
        import_path="watchdogs.utils.scheduled_service_to_cb",
        class_name="ScheduledContextBrokerCommunicator"
    ),
    'scheduled_service_to_cb_entity': ModuleImport(
        import_path="watchdogs.utils.custom_service_to_cb",
        class_name="ScheduledContextBrokerCommunicatorWholeEntity"
    )
}
