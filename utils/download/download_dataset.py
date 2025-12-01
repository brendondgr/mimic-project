"""
MIMIC-IV Dataset Download Utility

Downloads MIMIC-IV dataset from PhysioNet using optimized parallel downloading.
Uses hardware detection to maximize download speeds with appropriate thread count.
"""

import sys
import os
import subprocess
import getpass
from pathlib import Path
from typing import Tuple

# Add parent directory to path to import hardware module
sys.path.insert(0, str(Path(__file__).parent.parent))
from hardware.get_hardware import HardwareReporter


def get_user_credentials() -> Tuple[str, str]:
    """
    Prompt user for PhysioNet credentials.
    
    Returns:
        tuple: (username, password)
    """
    print("\n" + "="*80)
    print(" " * 15 + "MIMIC-IV Dataset Download from PhysioNet")
    print("="*80 + "\n")
    
    username = input("Enter PhysioNet username: ").strip()
    if not username:
        raise ValueError("Username cannot be empty")
    
    password = getpass.getpass("Enter PhysioNet password: ")
    if not password:
        raise ValueError("Password cannot be empty")
    
    return username, password


def get_optimal_thread_count() -> int:
    """
    Retrieve CPU thread count and calculate optimal download threads.
    
    Returns:
        int: Number of threads (total threads - 2, minimum 1)
    """
    try:
        reporter = HardwareReporter()
        cpu_info = reporter.get_cpu_info()
        
        # Calculate threads: available threads - 2 for system overhead
        optimal_threads = max(1, cpu_info.threads - 2)
        
        print(f"Detected CPU with {cpu_info.threads} threads")
        print(f"Using {optimal_threads} threads for download optimization\n")
        
        return optimal_threads
    except Exception as e:
        print(f"Warning: Could not detect CPU threads: {e}")
        print("Defaulting to 2 threads\n")
        return 2


def build_wget_command(username: str, password: str, threads: int) -> list:
    """
    Build the wget command with parameters for parallel downloading.
    
    Args:
        username: PhysioNet username
        password: PhysioNet password
        threads: Number of parallel threads
    
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
    
    return command


def download_dataset(username: str, password: str, threads: int):
    """
    Execute the dataset download with wget.
    
    Args:
        username: PhysioNet username
        password: PhysioNet password
        threads: Number of parallel threads
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
    
    print(f"Executing: {cmd}\n")
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
        else:
            print("\n" + "-" * 80)
            print(f"✗ Download completed with exit code: {process.returncode}")
            
    except Exception as e:
        print(f"✗ Error during download: {e}")
        sys.exit(1)


def main():
    """Main entry point"""
    try:
        # Get user credentials
        username, password = get_user_credentials()
        
        # Get optimal thread count from hardware info
        threads = get_optimal_thread_count()
        
        # Execute download
        download_dataset(username, password, threads)
        
        print("\n" + "="*80)
        print("Download process initiated. Check wget-log for detailed progress.")
        print("="*80 + "\n")
        
    except KeyboardInterrupt:
        print("\n\nDownload cancelled by user.")
        sys.exit(0)
    except ValueError as e:
        print(f"Input error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
