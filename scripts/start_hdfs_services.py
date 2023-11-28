import subprocess

def run_command(command):
    try:
        subprocess.run(command, check=True, shell=True)
    except subprocess.CalledProcessError as e:
        print(f"Command failed: {e}")
        return False
    return True

def stop_and_start_hadoop_services():
    # Stop existing Hadoop services
    if not run_command("/opt/homebrew/Cellar/hadoop/3.3.6/sbin/stop-all.sh"):
        print(f"Failed to stop existing Hadoop services. Trying to start services anyway.")

    # Start all Hadoop services
    if not run_command("/opt/homebrew/Cellar/hadoop/3.3.6/sbin/start-all.sh"):
        print(f"Failed to start Hadoop services.")
        return False

    return True

if __name__ == "__main__":
    if not stop_and_start_hadoop_services():
        print(f"Unable to start Hadoop services. Exiting.")
        exit(1)

    print(f"Hadoop services started successfully.")