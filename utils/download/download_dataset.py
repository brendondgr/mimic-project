"""
MIMIC-IV Dataset Download Utility

Downloads MIMIC-IV dataset from PhysioNet using optimized parallel downloading.
Uses hardware detection to maximize download speeds with appropriate thread count.
"""

import sys
import os
import subprocess
import getpass
import shutil
from pathlib import Path
from typing import Tuple, Optional

# Add parent directory to path to import hardware module
sys.path.insert(0, str(Path(__file__).parent.parent))
from hardware.get_hardware import HardwareReporter


def get_user_credentials(logger=None) -> Tuple[str, str]:
    """
    Prompt user for PhysioNet credentials.
    
    Args:
        logger: Optional logger instance
    
    Returns:
        tuple: (username, password)
    """
    print("\n" + "="*80)
    print(" " * 15 + "MIMIC-IV Dataset Download from PhysioNet")
    print("="*80 + "\n")
    
    if logger:
        logger.info("Prompting for PhysioNet credentials")
    
    username = input("Enter PhysioNet username: ").strip()
    if not username:
        raise ValueError("Username cannot be empty")
    
    password = getpass.getpass("Enter PhysioNet password: ")
    if not password:
        raise ValueError("Password cannot be empty")
    
    if logger:
        logger.info("Credentials received successfully")
    
    return username, password


def get_optimal_thread_count(logger=None) -> int:
    """
    Retrieve CPU thread count and calculate optimal download threads.
    
    Args:
        logger: Optional logger instance
    
    Returns:
        int: Number of threads (total threads - 2, minimum 1)
    """
    try:
        reporter = HardwareReporter(logger)
        cpu_info = reporter.get_cpu_info()
        
        # Calculate threads: available threads - 2 for system overhead
        optimal_threads = max(1, cpu_info.threads - 2)
        
        print(f"Detected CPU with {cpu_info.threads} threads")
        print(f"Using {optimal_threads} threads for download optimization\n")
        
        if logger:
            logger.info(f"Optimal thread count calculated: {optimal_threads}")
        
        return optimal_threads
    except Exception as e:
        error_msg = f"Warning: Could not detect CPU threads: {e}"
        print(error_msg)
        if logger:
            logger.warning(error_msg)
        print("Defaulting to 2 threads\n")
        return 2


def check_physionet_directory(logger=None) -> bool:
    """
    Check if physionet.org directory exists in root and prompt user for deletion.
    
    Args:
        logger: Optional logger instance
    
    Returns:
        bool: True if directory doesn't exist or was successfully deleted, False if user declined
    """
    root_dir = Path.cwd()
    physionet_dir = root_dir / "physionet.org"
    
    if not physionet_dir.exists():
        if logger:
            logger.info("No existing physionet.org directory found")
        return True
    
    # Directory exists, prompt user
    print("\n" + "="*80)
    print("WARNING: physionet.org directory already exists in the current directory!")
    print(f"Path: {physionet_dir}")
    print("="*80 + "\n")
    
    if logger:
        logger.warning(f"Existing physionet.org directory detected at {physionet_dir}")
    
    response = input("Would you like to delete this directory and start fresh? (yes/no): ").strip().lower()
    
    if response == 'yes':
        try:
            print(f"\nDeleting {physionet_dir}...")
            shutil.rmtree(physionet_dir)
            print("Directory deleted successfully.\n")
            if logger:
                logger.info("physionet.org directory deleted successfully")
            return True
        except Exception as e:
            error_msg = f"Error deleting directory: {e}"
            print(error_msg)
            if logger:
                logger.error(error_msg)
            return False
    else:
        print("\nDownload cancelled. Please remove the physionet.org directory manually if you want to start fresh.\n")
        if logger:
            logger.info("User declined directory deletion. Download cancelled.")
        return False



def build_wget_command(username: str, password: str, threads: int, logger=None) -> list:
    """
    Build the wget command with parameters for parallel downloading.
    
    Args:
        username: PhysioNet username
        password: PhysioNet password
        threads: Number of parallel threads
        logger: Optional logger instance
    
    Returns:
        list: Command and arguments for subprocess
    """
    url = "https://physionet.org/files/mimiciv/3.1/"
    
    command = [
        "wget",
        "-r",              # Recursive download
        "-N",              # Only download newer files
        "-c",              # Continue partial downloads
        "-np",             # Don't ascend to parent directory
        f"--user={username}",
        f"--password={password}",
        f"-b {threads}",   # Background with # threads
        url
    ]
    
    if logger:
        logger.debug(f"wget command built for URL: https://physionet.org/files/mimiciv/3.1/ with {threads} threads")
    
    return command


def download_dataset(username: str, password: str, threads: int, logger=None):
    """
    Execute the dataset download with wget.
    
    Args:
        username: PhysioNet username
        password: PhysioNet password
        threads: Number of parallel threads
        logger: Optional logger instance
    """
    print("Building wget command...\n")
    
    # Build the command properly for shell execution
    url = "https://physionet.org/files/mimiciv/3.1/"
    cmd = (
        f'wget -r -N -c -np '
        f'--user={username} '
        f'--password={password} '
        f'-b {threads} '
        f'{url}'
    )
    
    if logger:
        logger.debug(f"Starting download with {threads} threads")
        # Log the command structure WITHOUT credentials
        logger.info("Executing wget download command for MIMIC-IV dataset")
    
    print(f"Executing: wget -r -N -c -np --user=*** --password=*** -b {threads} {url}\n")
    print("-" * 80)
    
    try:
        # Use shell=True to properly handle the -b parameter
        process = subprocess.Popen(
            cmd,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True
        )
        
        # Print output in real-time
        for line in process.stdout:
            print(line, end='')
        
        process.wait()
        
        if process.returncode == 0:
            print("\n" + "-" * 80)
            print("✓ Download completed successfully!")
            if logger:
                logger.info("Download completed successfully")
        else:
            print("\n" + "-" * 80)
            print(f"✗ Download completed with exit code: {process.returncode}")
            if logger:
                logger.warning(f"Download completed with exit code: {process.returncode}")
            
    except Exception as e:
        error_msg = f"Error during download: {e}"
        print(f"✗ {error_msg}")
        if logger:
            logger.error(error_msg)
        sys.exit(1)


def main(logger=None):
    """
    Main entry point
    
    Args:
        logger: Optional logger instance
    """
    try:
        if logger:
            logger.info("Starting MIMIC-IV dataset download process")
        
        # Check if physionet.org directory already exists
        if not check_physionet_directory(logger):
            sys.exit(0)
        
        # Get user credentials
        username, password = get_user_credentials(logger)
        
        # Get optimal thread count from hardware info
        threads = get_optimal_thread_count(logger)
        
        # Execute download
        download_dataset(username, password, threads, logger)
        
        print("\n" + "="*80)
        print("Download process initiated. Check wget-log for detailed progress.")
        print("="*80 + "\n")
        
        if logger:
            logger.info("Download process initiated. User should monitor wget-log for progress")
        
    except KeyboardInterrupt:
        msg = "Download cancelled by user"
        print(f"\n\n{msg}.")
        if logger:
            logger.info(msg)
        sys.exit(0)
    except ValueError as e:
        error_msg = f"Input error: {e}"
        print(error_msg)
        if logger:
            logger.error(error_msg)
        sys.exit(1)
    except Exception as e:
        error_msg = f"Unexpected error: {e}"
        print(error_msg)
        if logger:
            logger.error(error_msg)
        sys.exit(1)


if __name__ == "__main__":
    main()
