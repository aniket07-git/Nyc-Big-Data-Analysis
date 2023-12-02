import os
import shutil
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def rename_spark_output_csv(source_directory, new_filename):
    # Check if the source directory exists
    if not os.path.exists(source_directory):
        logger.error(f"Directory not found: {source_directory}")
        return False

    # Find the CSV file starting with 'part-'
    try:
        for file in os.listdir(source_directory):
            if file.startswith("part-") and file.endswith(".csv"):
                # Construct the full file paths
                old_file_path = os.path.join(source_directory, file)
                new_file_path = os.path.join(source_directory, new_filename)

                # Rename the file
                shutil.move(old_file_path, new_file_path)
                logger.info(f"File renamed to {new_file_path}")
                return True

        logger.warning(f"No CSV file starting with 'part-' found in directory {source_directory}")
        return False
    except Exception as e:
        logger.exception("An error occurred while renaming Spark output CSV.", exc_info=e)
        return False
