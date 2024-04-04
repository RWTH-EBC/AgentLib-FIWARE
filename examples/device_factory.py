import logging

from agentlib.modules.iot.fiware.utils import device_factory


def run_example():
    logging.basicConfig(level="INFO")
    # create device factory
    factory_config = device_factory.FiwareIoTADeviceFactoryConfig(
        device_filename='./devices.json',
        apikey="agentlib_fiware_123456789"
        )

    device_factory.generate_emulator_agent(
        iotagent_cfg=r"configs\iotagent.json",
        module_cfg=r"configs\simulator.json",
        device_factory_attributes=factory_config,
        device_factory_commands=factory_config,
        agent_id="MyFiwareEmulator",
        filepath="emulator_agent.json"
    )


if __name__ == "__main__":
    run_example()
