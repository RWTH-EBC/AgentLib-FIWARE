import json
import os
import logging
from agentlib.utils.multi_agent_system import LocalMASAgency
from typing import Optional

from agentlib_fiware.factory import entity_factory

from pydantic import Field
from agentlib import AgentVariable, BaseModule, BaseModuleConfig

logger = logging.getLogger(__name__)


class ContextBrokerPingPongConfig(BaseModuleConfig):
    interval: float = Field(
        default=1,
        description="Interval in which to send data."
    )
    ping: Optional[AgentVariable] = Field(
        default=None,
        description="AgentVariable for the 'ping' message send to the context broker"
    )
    pong: Optional[AgentVariable] = Field(
        default=None,
        description="AgentVariable for the 'pong' message received by the context broker"
    )


class ContextBrokerPingPong(BaseModule):
    """
    A simple module which updates an entity in the context broker
    every other second and, if a callback of a changed entity or
    attribute is triggered, prints this changed object.
    """

    config: ContextBrokerPingPongConfig

    def process(self):
        if self.config.ping is None:
            yield self.env.event()
        else:
            counter = 0
            while True:
                self.logger.info("Sending 'ping' with counter %s", counter)
                self.set(name=self.config.ping.name, value=counter)
                yield self.env.timeout(self.config.interval)
                counter += 1

    def register_callbacks(self):
        if self.config.pong is not None:
            self.agent.data_broker.register_callback(
                alias=self.config.pong.alias, source=self.config.pong.source, callback=self._callback
            )

    def _callback(self, variable: AgentVariable):
        self.logger.info("Received: %s==%s with timestamp %s",
                         variable.name, variable.value, variable.timestamp)


def run_example(until, log_level=logging.INFO, yes_to_user_input: bool = False):
    # Set the log-level
    logging.basicConfig(level=log_level)
    # Change the working directly so that relative paths work
    os.chdir(os.path.abspath(os.path.dirname(__file__)))

    context_broker_config = {
        "cb_url": "http://134.130.56.157:1026",
        "fiware_header": {
            "service_path": "/myEmulatorExample",
            "service": "agentlib"
        },
        "log_level": "DEBUG"
    }
    ping_service = {
        "type": {"file": __file__, "class_name": "ContextBrokerPingPong"},
        "interval": 1,
        "ping": {"name": "ping", "alias": "ping_entity/ping_attribute"},
    }
    # We have several options for context_broker communication.
    # All options change the attribute on callbacks via HTTP.
    # To get the latest attribute changes, several options exist.
    # To get all updates, use notified_attributes. Here, a callback
    # is triggered everytime the attribute changes. Contrary, you can
    # use the scheduled versions, to only get the update every other second.
    # For the scheduled version, we also have to option to send the whole entity
    # instead of just the attribute and value.
    # We can also create the configs using the entity factory,

    ping_config = {
        "id": "ping",
        "modules": [
            ping_service,
            {
                **context_broker_config,
                "type": "agentlib_fiware.context_broker.scheduled_attributes",
                "update_entity_attributes": [
                    {
                        "alias": "ping_entity/ping_attribute",
                        "name": "ping_entity/ping_attribute",
                    }
                ],
            }
        ]
    }
    pong_config = {
        "id": "pong",
        "modules": [
            {
                "type": {"file": __file__, "class_name": "ContextBrokerPingPong"},
                "pong": {"name": "pong", "alias": "ping_entity/ping_attribute"}
            },
            {
                **context_broker_config,
                "type": "agentlib_fiware.context_broker.notified_attributes",
                # We need to specify a mqtt-url to get the notifications
                "mqtt_url": "mqtt://134.130.56.157:1883",
                "read_entity_attributes": [
                    {
                        "name": "ping_entity/ping_attribute",
                        "alias": "ping_entity/ping_attribute",
                    }
                ]
            },
            {
                **context_broker_config,
                "type": "agentlib_fiware.context_broker.scheduled_attributes",
                "read_interval": 5,
                "read_entity_attributes": [
                    {
                        "name": "ping_entity/ping_attribute",
                        "alias": "ping_entity/ping_attribute",
                    }
                ]
            },
            {
                **context_broker_config,
                "type": "agentlib_fiware.context_broker.scheduled_entities",
                "read_interval": 8,
                "read_entities": [
                    # The name will be used to send the entity `ping_entity`
                    # In order to send the entity to the pong service, the alias must match
                    # the one in the pong config:
                    {
                        "name": "ping_entity",
                        "alias": "ping_entity/ping_attribute",
                    }
                ]
            }
        ]
    }
    # which will create the entities in fiware, as well.
    generated_config = entity_factory.generate_service_to_cb_config_and_entities(
        context_broker_config=context_broker_config,
        service_config=ping_service,
        # We want to create entities for all variables in the field "ping", in this case, only one.
        update_fields="ping",
        with_ql_subscription=False,
        yes_to_user_input=yes_to_user_input
    )

    # Create agent config
    logger.info("Automatically generated config would be: %s",
                json.dumps(generated_config, indent=2))

    mas = LocalMASAgency(
        agent_configs=[
            ping_config,
            pong_config
        ],
        env={"rt": True, "factor": 1},
        variable_logging=True)
    mas.run(until=until)


if __name__ == "__main__":
    run_example(
        until=60, log_level="INFO"
    )
