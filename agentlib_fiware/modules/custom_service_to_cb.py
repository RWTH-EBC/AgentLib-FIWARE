"""
Custom module to only change the value being send
by the service_to_cb module.
"""

from .scheduled_service_to_cb import ScheduledServiceToContextBrokerCommunicator


class ScheduledServiceToContextBrokerCommunicatorWholeEntity(ScheduledServiceToContextBrokerCommunicator):

    def _process_entity_and_send_to_databroker(self, entity):
        process_entity_and_send_to_databroker(module=self, entity=entity)


def process_entity_and_send_to_databroker(module, entity):
    """Send the whole entity and not just the value"""
    entity_id = entity.id
    for entity_attr in module.config.read_entity_attributes:
        if entity_attr.name.startswith(entity_id):
            attr_name = entity_attr.name.split("/")[-1]
            try:
                attr = entity.get_attribute(attr_name)
                module.set(
                    name=entity_attr.name,
                    value=entity
                )
                module.logger.info(
                    "Send entity '%s' with alias %s into data_broker",
                    entity.id, entity_attr.alias
                )
            except KeyError:
                module.logger.error("Attribute '%s' not in entity '%s'",
                                  attr_name, entity_id)
                return
