"""This will be the example runner eventually."""

import unittest
import os
import subprocess
import logging
import pathlib
import pandas as pd

from agentlib.utils import custom_injection
from agentlib.utils.local_broadcast_broker import LocalBroadcastBroker


class TestExamples(unittest.TestCase):
    """Test all examples inside the agentlib"""

    def setUp(self) -> None:
        self.timeout = 10  # Seconds which the script is allowed to run
        self.main_cwd = os.getcwd()

    def tearDown(self) -> None:
        broker = LocalBroadcastBroker()
        broker.delete_all_clients()
        # Change back cwd:
        os.chdir(self.main_cwd)

    def _run_example(self, example, timeout=None):
        if timeout is None:
            timeout = self.timeout
        ex_py = (
            pathlib.Path(__file__).absolute().parents[1].joinpath("examples", example)
        )
        try:
            subprocess.check_output(
                ["python", ex_py], stderr=subprocess.STDOUT, timeout=timeout
            )
        except subprocess.TimeoutExpired:
            pass
        except subprocess.CalledProcessError as proc_err:
            raise Exception(proc_err.output.decode("utf-8")) from proc_err

    def _run_example_with_return(
        self, file: str, func_name: str, **kwargs
    ) -> dict[str, dict[str, pd.DataFrame]]:
        file = pathlib.Path(__file__).absolute().parents[1].joinpath("examples", file)

        # Custom file import
        test_func = custom_injection({"file": file, "class_name": func_name})
        results = test_func(**kwargs)
        return results

    def test_context_broker_communications(self):
        results = self._run_example_with_return(
            file="context_broker_communications.py",
            func_name="run_example",
            until=5,
            yes_to_user_input=True,
            log_level=logging.FATAL,
        )
        self.assertIsNone(results)

    def test_pid_single_room(self):
        results = self._run_example_with_return(
            file="pid_single_room.py",
            func_name="run_example",
            with_plots=False,
            until=8640 / 2,
            log_level=logging.FATAL,
            yes_to_user_input=True
        )
        self.assertIsInstance(results, dict)

    def test_device_factory(self):
        self._run_example_with_return(
            file="device_factory.py",
            func_name="run_example",
            yes_to_user_input=True
        )
