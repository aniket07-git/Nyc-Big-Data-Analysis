import subprocess
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def run_script(script_name):
    try:
        # Try running the script with 'python'
        subprocess.run(['python', script_name], check=True)
        logger.info(f"Successfully ran {script_name} with python")
    except (subprocess.CalledProcessError, FileNotFoundError):
        logger.warning(f"Failed to run {script_name} with python, trying with python3...")
        try:
            # If it fails, try running the script with 'python3'
            subprocess.run(['python3', script_name], check=True)
            logger.info(f"Successfully ran {script_name} with python3")
        except (subprocess.CalledProcessError, FileNotFoundError) as e:
            # If it still fails, log the error
            logger.error(f"Failed to run {script_name} with both python and python3: {e}")
            raise

def start_jupyter():
    try:
        # Start Jupyter notebook using python3
        subprocess.run(['python3', '-m', 'notebook'], check=True)
        logger.info("Jupyter notebook started successfully.")
    except (subprocess.CalledProcessError, FileNotFoundError) as e:
        # Log error if Jupyter fails to start
        logger.error(f"Failed to start Jupyter notebook: {e}")
        raise

def main():
    scripts_to_run = [
        'scripts/file_conversion.py',
        'scripts/data_cleaning.py',
        'scripts/merge.py'
    ]

    # Run the scripts
    for script in scripts_to_run:
        run_script(script)

    # Start Jupyter notebook
    start_jupyter()

if __name__ == "__main__":
    main()
