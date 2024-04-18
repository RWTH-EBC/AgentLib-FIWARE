import json
import logging
import os

from agentlib_fiware.factory import device_factory


def run_example(yes_to_user_input: bool = False):
    logging.basicConfig(level="INFO")
    # Change the working directly so that relative paths work
    os.chdir(os.path.abspath(os.path.dirname(__file__)))
    # create device factory
    factory_config = device_factory.FiwareIoTADeviceFactoryConfig(
        device_filename='./devices.json',
        apikey="agentlib_fiware_123456789"
        )

    agent_cfg, cb_cfg = device_factory.generate_emulator_agent(
        iotagent_cfg=r"configs\iotagent.json",
        module_cfg=r"configs\simulator.json",
        device_factory_attributes=factory_config,
        device_factory_commands=factory_config,
        agent_id="MyFiwareEmulator",
        yes_to_user_input=yes_to_user_input,
        # filepath="emulator_agent.json"
    )
    print(json.dumps(agent_cfg, indent=2))
    print(json.dumps(cb_cfg, indent=2))


if __name__ == "__main__":
    run_example()
