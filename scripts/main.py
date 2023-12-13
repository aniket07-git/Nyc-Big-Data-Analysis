"""
Author Contribution:
- Saurabh Chaudhari
"""

import subprocess
import logging

# Configure logging for the application
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def run_script(script_name):
    try:
        # Attempt to run the specified script using 'python'
        subprocess.run(['python', script_name], check=True)
        # Log a message if the script runs successfully
        logger.info(f"Successfully ran {script_name} with python")
    except (subprocess.CalledProcessError, FileNotFoundError):
        # Log a warning if the script fails to run with 'python'
        logger.warning(f"Failed to run {script_name} with python, trying with python3...")
        try:
            # Attempt to run the script using 'python3' if the first attempt fails
            subprocess.run(['python3', script_name], check=True)
            # Log a message if the script runs successfully with 'python3'
            logger.info(f"Successfully ran {script_name} with python3")
        except (subprocess.CalledProcessError, FileNotFoundError) as e:
            # Log an error if the script fails to run with both 'python' and 'python3'
            logger.error(f"Failed to run {script_name} with both python and python3: {e}")
            raise

def start_jupyter():
    try:
        # Attempt to start Jupyter notebook using 'python'
        subprocess.run(['python', '-m', 'notebook'], check=True)
        # Log a message if Jupyter notebook starts successfully
        logger.info("Jupyter notebook started successfully.")
    except (subprocess.CalledProcessError, FileNotFoundError):
        # Log a warning if Jupyter fails to start with 'python'
        logger.warning(f"Failed to run Jupyter notebook with python, trying with python3...")
        try:
            # Attempt to start Jupyter notebook using 'python3' if the first attempt fails
            subprocess.run(['python3', '-m', 'notebook'], check=True)
            # Log a message if Jupyter notebook starts successfully with 'python3'
            logger.info("Jupyter notebook started successfully.")
        except (subprocess.CalledProcessError, FileNotFoundError) as e:
            # Log an error if Jupyter fails to start with both 'python' and 'python3'
            logger.error(f"Failed to run Jupyter notebook with both python and python3: {e}")
            raise

def main():
    scripts_to_run = [
        'scripts/file_conversion.py',
        'scripts/data_cleaning.py',
        'scripts/merge.py'
    ]

    # Iterate over a list of scripts and run each one
    for script in scripts_to_run:
        run_script(script)

    # Attempt to start a Jupyter notebook
    start_jupyter()

if __name__ == "__main__":
    # Execute the main function if the script is run directly
    main()
