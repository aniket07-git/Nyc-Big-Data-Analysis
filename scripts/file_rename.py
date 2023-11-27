import os
import shutil

def rename_spark_output_csv(source_directory, new_filename):
    # Check if the source directory exists
    if not os.path.exists(source_directory):
        print(f"Directory not found: {source_directory}")
        return

    # Find the CSV file starting with 'part-'
    for file in os.listdir(source_directory):
        if file.startswith("part-") and file.endswith(".csv"):
            # Construct the full file paths
            old_file_path = os.path.join(source_directory, file)
            new_file_path = os.path.join(source_directory, new_filename)

            # Rename the file
            shutil.move(old_file_path, new_file_path)
            print(f"File renamed: {new_file_path}")
            return

    print("No CSV file found in the directory.")