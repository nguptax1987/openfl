# Copyright 2020-2024 Intel Corporation
# SPDX-License-Identifier: Apache-2.0


"""Experimental Director module."""

import asyncio
import logging
import time
from collections import defaultdict
from pathlib import Path
from typing import Any, AsyncGenerator, Dict, Iterable, Optional, Tuple, Union

import dill

from openfl.experimental.workflow.component.director.experiment import (
    Experiment,
    ExperimentsRegistry,
)
from openfl.experimental.workflow.transport.grpc.exceptions import EnvoyNotFoundError
from openfl.experimental.workflow.component.director.experiment import Status

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
        review_callback = None,  # Add review_callback parameter
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
            review_callback (Optional[Callable]): A callback function for reviewing experiments.
        """
        self.tls = tls
        self.root_certificate = root_certificate
        self.private_key = private_key
        self.certificate = certificate
        self.director_config = director_config
        self.install_requirements = install_requirements
        self._flow_status = asyncio.Queue()
        self.review_callback = review_callback  # Store the review_callback
        self.experiments_registry = ExperimentsRegistry()
        self.col_exp = {}
        self.col_exp_queues = defaultdict(asyncio.Queue)
        self._envoy_registry = {}
        self.envoy_health_check_period = envoy_health_check_period
        # authorized_cols refers to envoy & collaborator pair (one to one mapping)
        self.authorized_cols = []
        self.review_responses = defaultdict(dict)  # Initialize the shared dictionary as a defaultdict of dicts

    async def start_experiment_execution_loop(self) -> None:
        """Run tasks and experiments here"""
        loop = asyncio.get_event_loop()
        while True:
            try:
                async with self.experiments_registry.get_next_experiment() as experiment:
                    await self._wait_for_authorized_envoys()
                    # add experiment to collaborator queues so that enovys  can review the experiment
                    # Adding the experiment to collaborators queues
                    for col_name in experiment.collaborators:
                        queue = self.col_exp_queues[col_name]
                        await queue.put(experiment.name)

                    # Wait for all envoys to approve the experiment
                    logger.info("Waiting for envoy reviews...")

                    consensus_reached = await self.wait_for_envoys_consensus(experiment)
                    logger.info(f"consensus_reached: {consensus_reached}")

                    if not consensus_reached:
                        logger.warning(f"Experiment '{experiment.name}' rejected - skipping execution")
                        experiment.status = Status.REJECTED
                        await self._flow_status.put((False, experiment))
                        logger.info(f"Experiment '{experiment.name}' marked as rejected and flow status updated.")
                        
                        continue # Skip to the next experiment if rejected

                    # Start the experiment
                    logger.info(f"All participants approved - starting experiment '{experiment.name}'")
                    
                    try:

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
                        # Wait for the experiment to complete
                        flow_status = await run_aggregator_future
                        await self._flow_status.put(flow_status)
                        logger.info(f" Experiment '{experiment.name}' completed successfully")

                    except Exception as e:
                        logger.error(f" Error executing experiment '{experiment.name}': {e}")
                        experiment.status = Status.FAILED
                        raise
                    # Adding the experiment to collaborators queues
                    #for col_name in experiment.collaborators:
                        #queue = self.col_exp_queues[col_name]
                        #await queue.put(experiment.name)
                    # Wait for the experiment to complete and save the result
            except Exception as e:
                logger.error(f"Error while executing experiment: {e}")
                raise
            #finally:
                # Always reset the review responses for this experiment
                #if experiment.name in self.review_responses:
                    #del self.review_responses[experiment.name]
                    #logger.info(f"✅ Cleared previous review responses for experiment '{experiment.name}'")

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
        # Check if review callback is enabled
        if self.review_callback:
            review_approved = await experiment.review_experiment(self.review_callback)
            if not review_approved:
                logger.warning(f"Experiment '{experiment_name}' was rejected? by the Director Admin.")
                return False # Experiment rejected

        # Add the experiment to the registry
        self.authorized_cols = collaborator_names
        self.experiments_registry.add(experiment)
        logger.info(f"Experiment '{experiment_name}' was approved by Director and added to the registry.")
        return True # Experiment approved

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
    async def wait_for_envoys_consensus(self, experiment: Experiment) -> bool:
        """
        wait for all envoys to respond with APPROVE or  REJCT
        Returns True if all envoys approve, False if any reject.
        """
        # max_wait_time = 300  # seconds (5 minutes)
        # start_time = asyncio.get_event_loop().time()
        while True:
            responses = self.review_responses.get(experiment.name, {})
            # If all envoys have responded
            if len(responses) == len(self.authorized_cols):
                # Check if all responses are "APPROVE"
                all_approve = all(status == "APPROVE" for status in responses.values())
                return all_approve
            
            
            # --- Timeout logic commented for future iteration ---
            # if asyncio.get_event_loop().time() - start_time > max_wait_time:
            #     logger.warning(f"Timeout waiting for envoy consensus on experiment '{experiment.name}'")
            #     return False
            await asyncio.sleep(1) #Waits for 1 second before the next check.



    async def process_review_response(self, envoy_name: str, experiment_name: str, review_status: str) -> bool:
        """Process a review response from an envoy. Collects review responses and checks if consensus is achieved.
        Args:
             envoy_name (str): The name of the envoy sending the response.
             experiment_name (str): The name of the experiment being reviewed.
             review_status (str): The review status ("APPROVE" or "REJECT").
        Returns:
            bool: True if all envoys have responded and all responses are "APPROVE", False otherwise.
        """

        self.review_responses[experiment_name][envoy_name] = review_status

        # Only check consensus when all responses are in
        while not len(self.review_responses[experiment_name]) == len(self.authorized_cols):
            await asyncio.sleep(1)
        #check if all envoys have responded 
        all_approve = all(status == "APPROVE" for status in self.review_responses[experiment_name].values())
        return all_approve
        

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
