"""
Script to submit a streaming job to Databricks for LOTUS-PRISM.
"""

import subprocess

def main():
    # Databricks job ID for the streaming job
    job_id = 'YOUR_STREAM_JOB_ID'
    
    # Command to submit the job using Databricks CLI
    cmd = ['databricks', 'jobs', 'run-now', '--job-id', job_id]
    
    try:
        # Execute the command
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        print(f"Job submitted successfully: {result.stdout}")
    except subprocess.CalledProcessError as e:
        print(f"Error submitting job: {e.stderr}")

if __name__ == "__main__":
    main()
