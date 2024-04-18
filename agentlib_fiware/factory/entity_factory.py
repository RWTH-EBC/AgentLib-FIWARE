import json
import logging
from datetime import datetime
from typing import Dict, Union, Tuple

import requests.exceptions
from filip.models.ngsi_v2.context import ContextEntity, NamedContextAttribute
from filip.clients.ngsi_v2 import ContextBrokerClient
from filip.models.ngsi_v2.base import NamedMetadata
from filip.models.ngsi_v2.subscriptions import Subscription
from agentlib import AgentVariable, BaseModuleConfig, Source

from agentlib_fiware.modules.context_broker.base import BaseContextBrokerConfig

logger = logging.getLogger(__name__)


def generate_service_to_cb_config_and_entities(
        context_broker_config: Union[str, dict, BaseContextBrokerConfig],
        service_config: Union[str, dict, BaseModuleConfig],
        update_fields: str = None,
        read_fields: str = None,
        filepath: str = None,
        with_ql_subscription: bool = True,
        ql_url: str = None,
        create_entities_for_read_fields: bool = False,
        yes_to_user_input: bool = False
) -> Tuple[Dict, Dict]:
    if isinstance(service_config, str):
        with open(service_config, "r") as fp:
            service_config = json.load(fp)
    elif isinstance(service_config, BaseModuleConfig):
        service_config = service_config.model_dump()
    elif isinstance(service_config, dict):
        pass
    else:
        raise TypeError("Given module_cfg is neither str, dict, nor a BaseModuleConfig.")

    if isinstance(context_broker_config, str):
        with open(context_broker_config, "r") as fp:
            context_broker_config = json.load(fp)
    elif isinstance(context_broker_config, BaseContextBrokerConfig):
        context_broker_config = context_broker_config.model_dump()
    elif isinstance(context_broker_config, dict):
        pass
    else:
        raise TypeError("Given context_broker_config is neither str, dict, nor BaseContextBrokerConfig.")

    entities = []
    _type_map = {
        "str": "Text"
    }

    read_field_items = service_config.get(read_fields, [])
    update_field_items = service_config.get(update_fields, [])
    if not isinstance(read_field_items, list):
        read_field_items = [read_field_items]
    if not isinstance(update_field_items, list):
        update_field_items = [update_field_items]

    fields_to_create_entities = update_field_items.copy()
    if create_entities_for_read_fields:
        fields_to_create_entities.extend(read_field_items)
    _unsupported_types = ["pd.Series", "soft_constraint", "list"]
    for var in fields_to_create_entities:
        ag_var = AgentVariable(**var)
        if ag_var.type in _unsupported_types:
            _type = "Text"
        else:
            _type = _type_map.get(ag_var.type, "Number")
        entity_id, attr_name = ag_var.alias.split("/")
        attr = NamedContextAttribute(
            name=attr_name,
            value=ag_var.value,
            type=_type,
            metadata=NamedMetadata(
                name="TimeInstant",
                type="DateTime",
                value=datetime.now().strftime(context_broker_config.get("time_format", "%Y-%m-%dT%H:%M:%S.%fZ"))
            )
        )
        entity = ContextEntity(id=entity_id, type="sensor")
        entity.add_attributes([attr])
        logger.info(entity.model_dump_json(indent=2))
        entities.append(entity)

    # Post entities:
    # Pop possible extra fields
    if 'iota_url' in context_broker_config:
        context_broker_config.pop('iota_url')

    _update = None if not yes_to_user_input else True
    # Provision entity:
    with ContextBrokerClient(url=context_broker_config["cb_url"],
                                fiware_header=context_broker_config["fiware_header"]) as httpc:
        for entity in entities:
            try:
                httpc.post_entity(entity=entity, update=False)
            except requests.exceptions.HTTPError as err:
                if _update is None:
                    msg = f"Could not post devices due to error: {err}. " \
                          f"Do you want to update existing devices? (y/n)"
                    user_inp = input(msg)
                    if user_inp.lower() == "y":
                        _update = True
                    else:
                        _update = False
                        logger.info("Taking input as a no.")
                if _update:
                    httpc.post_entity(entity=entity, update=True)

    # Update configs
    context_broker_config["read_entity_attributes"] = []
    context_broker_config["update_entity_attributes"] = []
    for var in read_field_items:
        ag_var = AgentVariable(**var)
        ag_var.name = ag_var.alias
        context_broker_config["read_entity_attributes"].append(
            ag_var.dict()
        )
    for var in update_field_items:
        ag_var = AgentVariable(**var)
        ag_var.name = ag_var.alias
        context_broker_config["update_entity_attributes"].append(
            ag_var.dict()
        )

    # Check if to save.
    context_broker_config["type"] = "agentlib_fiware.context_broker.notified_attributes"
    if filepath is not None:
        with open(filepath, "w+") as file:
            json.dump(context_broker_config, file, indent=4)

    if with_ql_subscription:
        for entity in entities:
            subscription = Subscription(**{
                "description": f"Notification of changes from entity {entity.id}",
                "subject": {
                    "entities": [{"id": entity.id, "type": entity.type}],
                    "condition": {"attrs": []}
                },
                "notification": {
                    "http": {"url": f"{ql_url}/v2/notify"},
                    "metadata": ["dateCreated", "dateModified"]
                },
                "throttling": 0
            })
            httpc.post_subscription(subscription=subscription, update=True)
    return context_broker_config
