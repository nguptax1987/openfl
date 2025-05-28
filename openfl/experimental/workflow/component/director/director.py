# Copyright 2020-2024 Intel Corporation
# SPDX-License-Identifier: Apache-2.0


"""Experimental Director module."""

import asyncio
import logging
import time
from collections import defaultdict
from pathlib import Path
from typing import Any, AsyncGenerator, Callable, Dict, Iterable, Optional, Tuple, Union

import dill

from openfl.experimental.workflow.component.director.experiment import (
    Experiment,
    ExperimentsRegistry,
    Status,
)
from openfl.experimental.workflow.transport.grpc.exceptions import EnvoyNotFoundError

logger = logging.getLogger(__name__)


class Director:
    """Director class for managing experiments and envoys.

    Attributes:
        tls (bool): A flag indicating if TLS should be used for connections.
        root_certificate (Optional[Union[Path, str]]): The path to the root certificate
            for TLS.
        private_key (Optional[Union[Path, str]]): The path to the private key for TLS.
        certificate (Optional[Union[Path, str]]): The path to the certificate for TLS.
        director_config (Optional[Path]): Path to director_config file
        install_requirements (bool): A flag indicating if the requirements
            should be installed.
        _flow_status (Queue): Stores the flow status
        experiments_registry (ExperimentsRegistry): An object of
            ExperimentsRegistry to store the experiments.
        col_exp (dict): A dictionary to store the experiments for
            collaborators.
        col_exp_queues (defaultdict): A defaultdict to store the experiment
            queues for collaborators.
        _envoy_registry (dict): A dcitionary to store envoy info
        envoy_health_check_period (int): The period for health check of envoys
            in seconds.
        authorized_cols (list): A list of authorized envoys
        review_callback (Optional[Callable]): A callback function for reviewing experiments.
        review_responses (defaultdict): A dictionary to store review responses
            from envoys.
        _review_decision_event (asyncio.Event): An event to signal the review decision.
        review_consensus (Optional[bool]): A flag indicating if the review consensus
            is reached.
    """

    def __init__(
        self,
        *,
        tls: bool = True,
        root_certificate: Optional[Union[Path, str]] = None,
        private_key: Optional[Union[Path, str]] = None,
        certificate: Optional[Union[Path, str]] = None,
        director_config: Optional[Path] = None,
        envoy_health_check_period: int = 60,
        install_requirements: bool = True,
        review_callback: Optional[Callable] = None,
    ) -> None:
        """Initialize a Director object.

        Args:
            tls (bool, optional): A flag indicating if TLS should be used for
                connections. Defaults to True.
            root_certificate (Optional[Union[Path, str]]): The path to the
                root certificate for TLS. Defaults to None.
            private_key (Optional[Union[Path, str]]): The path to the private
                key for TLS. Defaults to None.
            certificate (Optional[Union[Path, str]]): The path to the
                certificate for TLS. Defaults to None.
            director_config (Optional[Path]): Path to director_config file
            envoy_health_check_period (int): The period for health check of envoys
            in seconds.
            install_requirements (bool, optional): A flag indicating if the
                requirements should be installed. Defaults to True.
        """
        self.tls = tls
        self.root_certificate = root_certificate
        self.private_key = private_key
        self.certificate = certificate
        self.director_config = director_config
        self.install_requirements = install_requirements
        self._flow_status = asyncio.Queue()
        self.review_callback = review_callback
        self.experiments_registry = ExperimentsRegistry()
        self.col_exp = {}
        self.col_exp_queues = defaultdict(asyncio.Queue)
        self._envoy_registry = {}
        self.envoy_health_check_period = envoy_health_check_period
        # authorized_cols refers to envoy & collaborator pair (one to one mapping)
        self.authorized_cols = []
        self.review_responses = defaultdict(dict)
        self._review_decision_event = asyncio.Event()
        self.review_consensus = None

    def _cleanup_experiment(self, experiment) -> None:
        """Reset director state and clean up experiment resources.

        Clears the experiment from the registry, resets collaborator states,
        review responses, and consensus flag.

        Args:
            experiment (Experiment): The experiment to clean up.
        """
        if experiment.name in self.review_responses:
            self.review_responses.clear()
        self.col_exp = dict.fromkeys(self.col_exp, None)
        self.review_consensus = False
        self._review_decision_event.set()

    async def _review_phase(self, experiment) -> bool:
        """Coordinates director and envoy reviews.

        Args:
            experiment (Experiment): The experiment to be reviewed.

        Returns:
            bool: True if the review consensus is reached, False otherwise.
        """
        review_approved = consensus_reached = True
        if self.review_callback:
            # Director review
            review_approved = experiment.review_experiment(self.review_callback)

        if review_approved:
            consensus_reached = await self._envoy_review(experiment)
            if not consensus_reached:
                experiment.status = Status.REJECTED
                logger.info(
                    f"Consensus not reached. Experiment '{experiment.name} "
                    "is rejected - skipping execution."
                )
        else:
            experiment.status = Status.REJECTED
        self.review_consensus = review_approved and consensus_reached

    async def _envoy_review(self, experiment) -> bool:
        """Notifies envoys and waits for their consensus.

        Args:
            experiment (Experiment): The experiment to be reviewed.

        Returns:
            bool: True if all envoys approve the experiment, False otherwise.
        """
        for col_name in experiment.collaborators:
            await self.col_exp_queues[col_name].put(experiment.name)

        logger.info("Waiting for envoy reviews...")
        return await self.wait_for_all_envoy_reviews(experiment)

    async def _execution_phase(self, experiment) -> None:
        """Handles the execution phase of the experiment."""
        loop = asyncio.get_event_loop()
        run_aggregator_future = loop.create_task(
            experiment.start(
                root_certificate=self.root_certificate,
                certificate=self.certificate,
                private_key=self.private_key,
                tls=self.tls,
                director_config=self.director_config,
                install_requirements=False,
            )
        )
        # Notify waiting participants that plan is approved
        # and experiment is started
        self._review_decision_event.set()
        flow_status = await run_aggregator_future
        await self._flow_status.put(flow_status)
        logger.info(f"Experiment '{experiment.name}' completed successfully.")

    async def _wait_for_authorized_envoys(self) -> None:
        """Wait until all authorized envoys are connected"""
        while not all(envoy in self.get_envoys().keys() for envoy in self.authorized_cols):
            connected_envoys = len(
                [envoy for envoy in self.authorized_cols if envoy in self.get_envoys().keys()]
            )
            logger.info(
                f"Waiting for {connected_envoys}/{len(self.authorized_cols)} "
                "authorized envoys to connect..."
            )
            await asyncio.sleep(10)

    async def start_experiment_execution_loop(self) -> None:
        """Main loop that waits for and runs tasks and experiments here."""
        while True:
            try:
                logger.info("Waiting for an experiment to run...")
                async with self.experiments_registry.get_next_experiment() as experiment:
                    await self._wait_for_authorized_envoys()
                    await self._review_phase(experiment)
                    if not self.review_consensus:
                        continue
                    await self._execution_phase(experiment)
            except Exception as e:
                logger.error(f"Error executing experiment '{experiment.name}': {e}")
                experiment.status = Status.FAILED
                raise
            finally:
                self._cleanup_experiment(experiment)
                self._review_decision_event.clear()

    async def get_flow_state(self) -> Tuple[bool, bytes]:
        """Wait until the experiment flow status indicates completion
        and return the status along with a serialized FLSpec object.

        Returns:
            status (bool): The flow status.
            flspec_obj (bytes): A serialized FLSpec object (in bytes) using dill.
        """
        status, flspec_obj = await self._flow_status.get()
        return status, dill.dumps(flspec_obj)

    async def wait_experiment(self, envoy_name: str) -> str:
        """Waits for an experiment to be ready for a given envoy.

        Args:
            envoy_name (str): The name of the envoy.

        Returns:
            str: The name of the experiment on the queue.
        """
        experiment_name = self.col_exp.get(envoy_name)
        # If any envoy gets disconnected
        if experiment_name and experiment_name in self.experiments_registry:
            experiment = self.experiments_registry[experiment_name]
            if experiment.aggregator.current_round < experiment.aggregator.rounds_to_train:
                return experiment_name

        self.col_exp[envoy_name] = None
        queue = self.col_exp_queues[envoy_name]
        experiment_name = await queue.get()
        self.col_exp[envoy_name] = experiment_name

        return experiment_name

    async def set_new_experiment(
        self,
        experiment_name: str,
        sender_name: str,
        collaborator_names: Iterable[str],
        experiment_archive_path: Path,
    ) -> bool:
        """Set new experiment and optionally review experiment .

        Args:
            experiment_name (str): Identifier for the new experiment.
            sender_name (str): Initiator of the experiment.
            collaborator_names (Iterable[str]): Participating collaborators.
            experiment_archive_path (Path): Path to the experiment archive.

        Returns:
            bool: True if the experiment is accepted and registered; False otherwise.
        """
        experiment = Experiment(
            name=experiment_name,
            archive_path=experiment_archive_path,
            collaborators=collaborator_names,
            users=[sender_name],
            sender=sender_name,
        )

        # Add the experiment to the registry
        self.authorized_cols = collaborator_names
        self.experiments_registry.add(experiment)

        # Waiting for experiment review plan decision
        await self._review_decision_event.wait()
        return experiment.status != Status.REJECTED

    async def stream_experiment_stdout(
        self, experiment_name: str, caller: str
    ) -> AsyncGenerator[Optional[Dict[str, Any]], None]:
        """Stream stdout from the aggregator.

        This method takes next stdout dictionary from the aggregator's queue
        and returns it to the caller.

        Args:
            experiment_name (str): String id for experiment.
            caller (str): String id for experiment owner.

        Yields:
            Optional[Dict[str, str]]: A dictionary containing the keys
            'stdout_origin', 'task_name', and 'stdout_value' if the queue is not empty,
            or None if the queue is empty but the experiment is still running.
        """
        if (
            experiment_name not in self.experiments_registry
            or caller not in self.experiments_registry[experiment_name].users
        ):
            raise Exception(
                f'No experiment name "{experiment_name}" in experiments list, or caller "{caller}"'
                f" does not have access to this experiment"
            )

        while not self.experiments_registry[experiment_name].aggregator:
            await asyncio.sleep(5)
        aggregator = self.experiments_registry[experiment_name].aggregator
        while True:
            if not aggregator.stdout_queue.empty():
                # Yield the next item from the queue
                yield aggregator.stdout_queue.get()
            elif aggregator.all_quit_jobs_sent():
                # Stop Iteration if all jobs have quit and the queue is empty
                break
            else:
                # Yield none if the queue is empty but the experiment is still running.
                yield None

    async def wait_for_all_envoy_reviews(self, experiment: Experiment) -> bool:
        """Wait for all envoys to respond with APPROVE or REJECT.

        Args:
            experiment (Experiment): The experiment being reviewed.
        Returns:
            bool: True if all envoys approve the experiment, False otherwise.
        """
        expected_count = len(self.authorized_cols)
        while True:
            responses = self.review_responses.get(experiment.name, {})
            if len(responses) == expected_count:
                all_approve = all(status == "APPROVE" for status in responses.values())
                logger.info(f"All envoys have responded for experiment '{experiment.name}'.")
                return all_approve

            await asyncio.sleep(1)  # Waits for 1 second before the next check.

    async def process_review_response(
        self, envoy_name: str, experiment_name: str, review_status: str
    ) -> bool:
        """Process a review response from an envoy. Collects review responses and
            check if consensus is achieved.
        Args:
             envoy_name (str): Envoy sending the response.
             experiment_name (str): The name of the experiment being reviewed.
             review_status (str): "APPROVE" or "REJECT".
        Returns:
            bool: True if all envoys approve the experiment, False otherwise.
        """
        self.review_responses[experiment_name][envoy_name] = review_status
        await self._review_decision_event.wait()
        return self.review_consensus

    def get_experiment_data(self, experiment_name: str) -> Path:
        """Get experiment data.

        Args:
            experiment_name (str): String id for experiment.

        Returns:
            str: Path of archive.
        """
        return self.experiments_registry[experiment_name].archive_path

    def ack_envoy_connection_request(self, envoy_name: str) -> bool:
        """Save the envoy info into _envoy_registry

        Args:
            envoy_name (str): Name of the envoy

        Returns:
            bool: Always returns True to indicate the envoy
                has been successfully acknowledged.
        """
        self._envoy_registry[envoy_name] = {
            "name": envoy_name,
            "is_online": True,
            "is_experiment_running": False,
            "last_updated": time.time(),
            "valid_duration": 2 * self.envoy_health_check_period,
        }
        # Currently always returns True, indicating the envoy was added successfully.
        # Future logic might change this to handle conditions.
        return True

    def get_envoys(self) -> Dict[str, Any]:
        """Gets list of connected envoys

        Returns:
            dict: Dictionary with the status information about envoys.
        """
        logger.debug("Envoy registry: %s", self._envoy_registry)
        for envoy in self._envoy_registry.values():
            envoy["is_online"] = time.time() < envoy.get("last_updated", 0) + envoy.get(
                "valid_duration", 0
            )
            envoy["experiment_name"] = self.col_exp.get(envoy["name"], "None")

        return self._envoy_registry

    def update_envoy_status(
        self,
        *,
        envoy_name: str,
        is_experiment_running: bool,
    ) -> int:
        """Accept health check from envoy.

        Args:
            envoy_name (str): String id for envoy.
            is_experiment_running (bool): Boolean value for the status of the
                experiment.

        Raises:
            EnvoyNotFoundError: When Unknown envoy {envoy_name}.

        Returns:
            int: Value of the envoy_health_check_period.
        """
        envoy_info = self._envoy_registry.get(envoy_name)
        if not envoy_info:
            logger.error(f"Unknown envoy {envoy_name}")
            raise EnvoyNotFoundError(f"Unknown envoy {envoy_name}")

        envoy_info.update(
            {
                "is_online": True,
                "is_experiment_running": is_experiment_running,
                "valid_duration": 2 * self.envoy_health_check_period,
                "last_updated": time.time(),
            }
        )

        return self.envoy_health_check_period
