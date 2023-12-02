import subprocess
import logging
import sys

# Initialize logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def run_command(command):
    try:
        subprocess.run(command, check=True, shell=True)
        logger.info(f"Successfully executed command: {command}")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Command failed with error: {e}")
        return False

def stop_and_start_hadoop_services():
    # Stop existing Hadoop services
    if not run_command("/opt/homebrew/Cellar/hadoop/3.3.6/sbin/stop-all.sh"):
        logger.warning("Failed to stop existing Hadoop services. Attempting to start services anyway.")
    
    # Start all Hadoop services
    if not run_command("/opt/homebrew/Cellar/hadoop/3.3.6/sbin/start-all.sh"):
        logger.error("Failed to start Hadoop services.")
        return False

    return True

def main():
    if not stop_and_start_hadoop_services():
        logger.critical("Unable to start Hadoop services. Exiting.")
        sys.exit(1)

    logger.info("Hadoop services started successfully.")

if __name__ == "__main__":
    main()
