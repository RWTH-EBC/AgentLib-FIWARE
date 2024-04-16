import os
import logging
from agentlib.utils.multi_agent_system import LocalMASAgency

from agentlib_fiware.factory import device_factory


def run_example(until, with_plots=True, log_level=logging.INFO, t_sample=60):
    # Set the log-level
    logging.basicConfig(level=log_level)
    # Change the working directly so that relative paths work
    os.chdir(os.path.abspath(os.path.dirname(__file__)))
    # Provision utils devices
    factory_config = device_factory.FiwareIoTADeviceFactoryConfig(
        apikey="agentlib_fiware_123456789"
    )

    emulator_agent_cfg, cb_module_cfg = device_factory.generate_emulator_agent(
        iotagent_cfg=r"configs\iotagent.json",
        module_cfg=r"configs\simulator.json",
        device_factory_attributes=factory_config,
        device_factory_commands=factory_config,
        agent_id="SiL",
        filepath="configs/emulator_agent.json",
        filepath_cb_communicator="configs/cb_module.json"
    )
    # Insert Outdoor air temperature
    emulator_agent_cfg["modules"].append(r"configs\try_module.json")

    # Create agent configs
    ag_config_virtual = {
        "id": "agent_pid",
        "modules": [
            "configs/pid.json",
            cb_module_cfg
        ]
    }

    mas = LocalMASAgency(agent_configs=[emulator_agent_cfg, ag_config_virtual],
                         env={"rt": True, "factor": 1 / t_sample, "t_sample": t_sample},
                         variable_logging=True)
    mas.run(until=until)
    results = mas.get_results()

    df_ro_pid = results["agent_pid"]["AgentLogger"]
    df_ro_sil = results['SiL']["AgentLogger"]

    if not with_plots:
        return results
    import matplotlib.pyplot as plt
    fig, axes = plt.subplots(3, 1, sharex=True)
    # Plot Room agent data for PID controlled zone
    axes[0].plot(df_ro_pid["T_air"], color="blue", label="RoomAgent")
    axes[1].plot(df_ro_pid["Q_flow_heat"], color="blue", label="RoomAgent")
    # Plot SiL agent data
    axes[0].plot(df_ro_sil["T_air"], color="red", label="Simulation")
    axes[1].plot(df_ro_sil["Q_flow_heat"], color="red", label="Simulation")
    axes[2].plot(df_ro_sil["T_oda"], color="red", label="Simulation")
    # Legend, titles etc:
    axes[0].set_ylabel("Room Temperature / K")
    axes[1].set_ylabel("Q_flow to Room / W")
    axes[2].set_ylabel("Outdoor Air Temperature / K")
    axes[2].set_xlabel("Time / s")
    plt.show()
    return results


if __name__ == "__main__":
    run_example(until=86400/20,
                with_plots=True,
                log_level="DEBUG")
