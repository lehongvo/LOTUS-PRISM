#!/usr/bin/env python
"""
Script to submit LOTUS-PRISM batch job to Databricks.

This script interacts with the Databricks API to submit the ETL batch job,
monitors execution, and handles results.
"""

import os
import sys
import time
import json
import argparse
import requests
import logging
from datetime import datetime

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DatabricksJobSubmitter:
    """
    Submits and monitors jobs on Databricks.
    """
    
    def __init__(self, workspace_url, token, cluster_id=None):
        """
        Initialize the job submitter with Databricks workspace details.
        
        Args:
            workspace_url: URL of the Databricks workspace
            token: Authentication token for Databricks API
            cluster_id: ID of an existing cluster to use (optional)
        """
        self.workspace_url = workspace_url.rstrip('/')
        self.token = token
        self.cluster_id = cluster_id
        self.headers = {
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json'
        }
    
    def submit_job(self, job_config):
        """
        Submit a job to Databricks using the provided configuration.
        
        Args:
            job_config: Dictionary containing job configuration
            
        Returns:
            Job run ID if successful, None otherwise
        """
        logger.info("Submitting job to Databricks")
        
        try:
            # Prepare the request
            url = f"{self.workspace_url}/api/2.0/jobs/runs/submit"
            
            # If using an existing cluster, override the cluster settings
            if self.cluster_id:
                job_config["existing_cluster_id"] = self.cluster_id
                if "new_cluster" in job_config:
                    del job_config["new_cluster"]
            
            # Submit the job
            response = requests.post(url, headers=self.headers, json=job_config)
            response.raise_for_status()
            
            # Get the run ID
            run_id = response.json().get('run_id')
            logger.info(f"Job submitted successfully with run ID: {run_id}")
            
            return run_id
        except requests.exceptions.RequestException as e:
            logger.error(f"Error submitting job: {str(e)}")
            if hasattr(e, 'response') and e.response:
                logger.error(f"Response content: {e.response.text}")
            return None
    
    def check_job_status(self, run_id):
        """
        Check the status of a running job.
        
        Args:
            run_id: ID of the job run
            
        Returns:
            Dictionary with job status information or None if request failed
        """
        try:
            url = f"{self.workspace_url}/api/2.0/jobs/runs/get?run_id={run_id}"
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Error checking job status: {str(e)}")
            return None
    
    def wait_for_job_completion(self, run_id, check_interval=30, timeout=7200):
        """
        Wait for a job to complete, checking its status at regular intervals.
        
        Args:
            run_id: ID of the job run
            check_interval: Seconds to wait between status checks
            timeout: Maximum seconds to wait before timing out
            
        Returns:
            Final job status dictionary or None if timed out or failed
        """
        logger.info(f"Waiting for job {run_id} to complete...")
        start_time = time.time()
        
        while True:
            # Check if we've exceeded the timeout
            if time.time() - start_time > timeout:
                logger.error(f"Timeout waiting for job {run_id} to complete")
                return None
            
            # Check the job status
            status = self.check_job_status(run_id)
            if not status:
                logger.error(f"Failed to get status for job {run_id}")
                return None
            
            # Get the life cycle state
            state = status.get('state', {}).get('life_cycle_state')
            result_state = status.get('state', {}).get('result_state')
            
            logger.info(f"Job {run_id} state: {state}, result: {result_state}")
            
            # Check if the job has completed
            if state in ['TERMINATED', 'SKIPPED', 'INTERNAL_ERROR']:
                return status
            
            # Wait before checking again
            time.sleep(check_interval)
    
    def get_job_logs(self, run_id):
        """
        Get the logs for a completed job run.
        
        Args:
            run_id: ID of the job run
            
        Returns:
            String containing job logs or None if request failed
        """
        try:
            url = f"{self.workspace_url}/api/2.0/jobs/runs/get-output?run_id={run_id}"
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Error getting job logs: {str(e)}")
            return None

def create_batch_job_config(
    notebook_path,
    params=None,
    cluster_name="LOTUS-PRISM-BatchETL",
    spark_version="11.3.x-scala2.12",
    node_type_id="Standard_F4s",
    driver_node_type_id=None,
    min_workers=2,
    max_workers=4,
    libraries=None,
    timeout_seconds=3600
):
    """
    Create a configuration for a Databricks batch job.
    
    Args:
        notebook_path: Path to the notebook in Databricks workspace
        params: Dictionary of parameters to pass to the notebook
        cluster_name: Name to assign to the cluster
        spark_version: Spark version to use
        node_type_id: Type of worker nodes
        driver_node_type_id: Type of driver node (defaults to node_type_id)
        min_workers: Minimum number of workers
        max_workers: Maximum number of workers
        libraries: List of libraries to install on the cluster
        timeout_seconds: Job timeout in seconds
        
    Returns:
        Dictionary containing job configuration
    """
    if driver_node_type_id is None:
        driver_node_type_id = node_type_id
        
    if libraries is None:
        libraries = [
            {"pypi": {"package": "delta-spark==2.3.0"}},
            {"pypi": {"package": "pyyaml"}}
        ]
    
    # Create the job configuration
    config = {
        "run_name": f"LOTUS-PRISM Batch ETL {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "new_cluster": {
            "cluster_name": cluster_name,
            "spark_version": spark_version,
            "node_type_id": node_type_id,
            "driver_node_type_id": driver_node_type_id,
            "autoscale": {
                "min_workers": min_workers,
                "max_workers": max_workers
            },
            "spark_conf": {
                "spark.databricks.delta.preview.enabled": "true",
                "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
                "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog"
            }
        },
        "notebook_task": {
            "notebook_path": notebook_path,
            "base_parameters": params or {}
        },
        "libraries": libraries,
        "timeout_seconds": timeout_seconds
    }
    
    return config

def main():
    """
    Main entry point for the script.
    """
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Submit LOTUS-PRISM batch job to Databricks")
    
    parser.add_argument("--workspace-url", required=True, help="Databricks workspace URL")
    parser.add_argument("--token", required=True, help="Databricks API token")
    parser.add_argument("--cluster-id", help="ID of existing cluster to use")
    parser.add_argument("--notebook-path", default="/Shared/LOTUS-PRISM/batch_etl_demo", 
                       help="Path to the notebook in Databricks workspace")
    parser.add_argument("--config-path", default="../config/batch_config.yaml",
                       help="Path to the batch configuration file (will be passed to the notebook)")
    parser.add_argument("--timeout", type=int, default=7200, 
                       help="Maximum time to wait for job completion (seconds)")
    parser.add_argument("--wait", action="store_true", 
                       help="Wait for job completion")
    
    args = parser.parse_args()
    
    try:
        # Create job submitter
        submitter = DatabricksJobSubmitter(
            args.workspace_url,
            args.token,
            args.cluster_id
        )
        
        # Create job configuration
        job_config = create_batch_job_config(
            notebook_path=args.notebook_path,
            params={"config_path": args.config_path}
        )
        
        # Submit the job
        run_id = submitter.submit_job(job_config)
        if not run_id:
            logger.error("Failed to submit job")
            sys.exit(1)
        
        # Wait for job completion if requested
        if args.wait:
            status = submitter.wait_for_job_completion(run_id, timeout=args.timeout)
            if not status:
                logger.error("Job monitoring failed or timed out")
                sys.exit(2)
            
            result_state = status.get('state', {}).get('result_state')
            if result_state != 'SUCCESS':
                logger.error(f"Job completed with status: {result_state}")
                
                # Get logs for debugging
                logs = submitter.get_job_logs(run_id)
                if logs:
                    logger.info("Job logs:")
                    notebook_output = logs.get('notebook_output', {}).get('result')
                    if notebook_output:
                        print(notebook_output)
                
                sys.exit(3)
            
            logger.info("Job completed successfully")
        else:
            logger.info(f"Job submitted with run ID: {run_id}. Not waiting for completion.")
        
        sys.exit(0)
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
