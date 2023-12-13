"""
Author Contribution:
- Aniket Yadav
"""

import subprocess
import logging
import sys
from dotenv import load_dotenv
import os

# Load environment variables from a .env file
load_dotenv()

# Configure logging for the application
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def run_command(command):
    try:
        # Execute a system command passed as a string, with error checking enabled
        subprocess.run(command, check=True, shell=True)
        # Log success message if the command executes successfully
        logger.info(f"Successfully executed command: {command}")
        return True
    except subprocess.CalledProcessError as e:
        # Log an error message if the command fails to execute
        logger.error(f"Command failed with error: {e}")
        return False

def stop_and_start_hadoop_services():
    # Attempt to stop existing Hadoop services using the command from environment variables
    if not run_command(os.getenv('STOP_SERVICES')):
        # Log a warning if stopping the services fails, but continue to attempt to start services
        logger.warning("Failed to stop existing Hadoop services. Attempting to start services anyway.")
    
    # Attempt to start all Hadoop services using the command from environment variables
    if not run_command(os.getenv('START_SERVICES')):
        # Log an error and return False if starting the services fails
        logger.error("Failed to start Hadoop services.")
        return False

    # Return True if all services are started successfully
    return True

def main():
    # Attempt to stop and start Hadoop services
    if not stop_and_start_hadoop_services():
        # Log a critical error and exit the program if services cannot be started
        logger.critical("Unable to start Hadoop services. Exiting.")
        sys.exit(1)

    # Log information that Hadoop services have been started successfully
    logger.info("Hadoop services started successfully.")

if __name__ == "__main__":
    main()
