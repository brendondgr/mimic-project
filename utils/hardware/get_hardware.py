"""
Hardware Information Reporting System

Provides detailed information about system hardware including GPU, CPU, RAM, and Storage.
Useful for monitoring AI/ML, HPC, and other computationally intensive tasks.
"""

import os
import sys
import platform
import subprocess
import json
from dataclasses import dataclass
from typing import List, Dict, Optional
from pathlib import Path


def check_privileges(logger=None):
    """Check if script has administrative/sudo privileges and prompt if needed"""
    def log_message(msg):
        if logger:
            logger.warning(msg)
        else:
            print(msg)
    
    if sys.platform == "linux" or sys.platform == "darwin":
        # Unix-like systems - check for sudo
        if os.geteuid() != 0:
            log_message("⚠️  WARNING: This script requires sudo/root privileges for full hardware details")
            log_message("   (GPU temps, drive temps, memory type, etc. require elevated access)")
            response = input("Would you like to run this script with sudo? (y/n): ").strip().lower()
            if response == 'y':
                if logger:
                    logger.info("Re-running with sudo...")
                else:
                    print("\nRe-running with sudo...\n")
                os.execvp('sudo', ['sudo', sys.executable] + sys.argv)
            else:
                log_message("Continuing without sudo. Some hardware details will be unavailable.")
    elif sys.platform == "win32":
        # Windows - check for admin privileges
        try:
            import ctypes
            is_admin = ctypes.windll.shell.IsUserAnAdmin()
        except Exception:
            is_admin = False
        
        if not is_admin:
            log_message("⚠️  WARNING: This script performs better with administrator privileges")
            log_message("   (Some hardware details may be unavailable)")
            response = input("Would you like to re-run as administrator? (y/n): ").strip().lower()
            if response == 'y':
                if logger:
                    logger.info("Re-running as administrator...")
                else:
                    print("\nRe-running as administrator...\n")
                ctypes.windll.shell.ShellExecuteEx(
                    lpOperation='runas',
                    lpFile=sys.executable,
                    lpParameters=' '.join(sys.argv),
                    nShow=1
                )
                sys.exit(0)
            else:
                log_message("Continuing without admin privileges. Some hardware details will be unavailable.")


@dataclass
class GPUInfo:
    """Container for GPU information"""
    name: str
    vram_total_gb: float
    vram_used_gb: float
    vram_available_gb: float
    temperature: Optional[float] = None
    utilization_percent: Optional[float] = None
    power_draw_w: Optional[float] = None
    compute_capability: Optional[str] = None
    driver_version: Optional[str] = None


@dataclass
class CPUInfo:
    """Container for CPU information"""
    name: str
    cores: int
    threads: int
    frequency_ghz: float
    max_frequency_ghz: Optional[float] = None
    cache_mb: Optional[float] = None
    tdp_watts: Optional[float] = None
    architecture: Optional[str] = None


@dataclass
class RAMInfo:
    """Container for RAM information"""
    total_gb: float
    available_gb: float
    used_gb: float
    percent_used: float
    memory_type: Optional[str] = None
    speed_mhz: Optional[float] = None
    frequency: Optional[str] = None
    ecc_enabled: Optional[bool] = None


@dataclass
class DriveInfo:
    """Container for individual drive information"""
    path: str
    total_gb: float
    used_gb: float
    available_gb: float
    percent_used: float
    drive_type: Optional[str] = None
    model: Optional[str] = None
    serial: Optional[str] = None
    temp_celsius: Optional[float] = None


class HardwareReporter:
    """Main class for retrieving and reporting hardware information"""

    def __init__(self, logger=None):
        """Initialize the hardware reporter"""
        self.logger = logger
        self._gpu_info_cache: Optional[List[GPUInfo]] = None
        self._cpu_info_cache: Optional[CPUInfo] = None
        self._ram_info_cache: Optional[RAMInfo] = None
        self._drives_info_cache: Optional[List[DriveInfo]] = None

    def _log(self, level: str, message: str):
        """Log message using logger if available, otherwise print"""
        if self.logger:
            getattr(self.logger, level.lower())(message)
        else:
            print(message)

    def get_gpu_info(self) -> List[GPUInfo]:
        """Retrieve GPU information using nvidia-smi or alternative methods"""
        gpus = []
        
        try:
            # Try NVIDIA GPUs first
            result = subprocess.run(
                ['nvidia-smi', '--query-gpu=index,name,memory.total,memory.used,memory.free,temperature.gpu,utilization.gpu,power.draw,compute_cap,driver_version',
                 '--format=csv,noheader,nounits'],
                capture_output=True,
                text=True,
                timeout=5
            )
            
            if result.returncode == 0:
                for line in result.stdout.strip().split('\n'):
                    if line.strip():
                        parts = [p.strip() for p in line.split(',')]
                        try:
                            gpu = GPUInfo(
                                name=f"NVIDIA {parts[1]}",
                                vram_total_gb=float(parts[2]) / 1024,
                                vram_used_gb=float(parts[3]) / 1024,
                                vram_available_gb=float(parts[4]) / 1024,
                                temperature=float(parts[5]) if parts[5] else None,
                                utilization_percent=float(parts[6]) if parts[6] else None,
                                power_draw_w=float(parts[7].split()[0]) if parts[7] else None,
                                compute_capability=parts[8] if parts[8] else None,
                                driver_version=parts[9] if parts[9] else None
                            )
                            gpus.append(gpu)
                        except (ValueError, IndexError):
                            continue
        except (FileNotFoundError, subprocess.TimeoutExpired):
            pass

        # Fallback for AMD or other GPUs
        if not gpus and self._check_amd_gpu():
            gpus = self._get_amd_gpu_info()

        self._gpu_info_cache = gpus
        return gpus

    def _check_amd_gpu(self) -> bool:
        """Check if AMD GPU is available"""
        try:
            subprocess.run(['rocm-smi'], capture_output=True, timeout=5)
            return True
        except (FileNotFoundError, subprocess.TimeoutExpired):
            return False

    def _get_amd_gpu_info(self) -> List[GPUInfo]:
        """Retrieve AMD GPU information"""
        gpus = []
        try:
            result = subprocess.run(
                ['rocm-smi', '--json'],
                capture_output=True,
                text=True,
                timeout=5
            )
            if result.returncode == 0:
                data = json.loads(result.stdout)
                # Parse AMD GPU data
                for gpu_data in data.get('system_info_list', []):
                    gpu = GPUInfo(
                        name=f"AMD {gpu_data.get('gpu_model', 'Unknown')}",
                        vram_total_gb=gpu_data.get('vram_gib', 0),
                        vram_used_gb=0,  # Would need additional query
                        vram_available_gb=gpu_data.get('vram_gib', 0)
                    )
                    gpus.append(gpu)
        except (json.JSONDecodeError, subprocess.TimeoutExpired):
            pass
        
        return gpus

    def get_cpu_info(self) -> CPUInfo:
        """Retrieve CPU information"""
        try:
            import psutil
            cpu_freq = psutil.cpu_freq()
            
            # Try to get CPU model name
            cpu_name = platform.processor()
            
            # On Linux, get more detailed info
            if sys.platform == "linux":
                try:
                    with open('/proc/cpuinfo', 'r') as f:
                        for line in f:
                            if line.startswith('model name'):
                                cpu_name = line.split(':', 1)[1].strip()
                                break
                except FileNotFoundError:
                    pass
            
            # Get cache size on Linux
            cache_mb = None
            if sys.platform == "linux":
                try:
                    with open('/proc/cpuinfo', 'r') as f:
                        for line in f:
                            if 'cache size' in line:
                                cache_mb = float(line.split(':', 1)[1].strip().split()[0])
                                break
                except FileNotFoundError:
                    pass
            
            cpu = CPUInfo(
                name=cpu_name,
                cores=psutil.cpu_count(logical=False) or 1,
                threads=psutil.cpu_count(logical=True) or 1,
                frequency_ghz=cpu_freq.current / 1000 if cpu_freq else 0,
                max_frequency_ghz=cpu_freq.max / 1000 if cpu_freq else None,
                cache_mb=cache_mb,
                architecture=platform.machine()
            )
            self._cpu_info_cache = cpu
            return cpu
        except ImportError:
            raise ImportError("psutil is required. Install it with: pip install psutil")

    def get_ram_info(self) -> RAMInfo:
        """Retrieve RAM information"""
        try:
            import psutil
            mem = psutil.virtual_memory()
            
            # Get memory type and speed (Linux specific)
            memory_type = None
            speed_mhz = None
            ecc_enabled = None
            
            if sys.platform == "linux":
                try:
                    # Try dmidecode for detailed memory info
                    result = subprocess.run(
                        ['sudo', 'dmidecode', '--type', 'memory'],
                        capture_output=True,
                        text=True,
                        timeout=5
                    )
                    if result.returncode == 0:
                        output = result.stdout.lower()
                        if 'ddr5' in output:
                            memory_type = 'DDR5'
                        elif 'ddr4' in output:
                            memory_type = 'DDR4'
                        elif 'ddr3' in output:
                            memory_type = 'DDR3'
                        
                        # Extract speed
                        for line in result.stdout.split('\n'):
                            if 'speed:' in line.lower() and 'mhz' in line.lower():
                                try:
                                    speed_mhz = float(line.split(':')[1].strip().split()[0])
                                    break
                                except ValueError:
                                    continue
                except (subprocess.TimeoutExpired, FileNotFoundError):
                    pass
            
            ram = RAMInfo(
                total_gb=mem.total / (1024 ** 3),
                available_gb=mem.available / (1024 ** 3),
                used_gb=mem.used / (1024 ** 3),
                percent_used=mem.percent,
                memory_type=memory_type,
                speed_mhz=speed_mhz,
                ecc_enabled=ecc_enabled
            )
            self._ram_info_cache = ram
            return ram
        except ImportError:
            raise ImportError("psutil is required. Install it with: pip install psutil")

    def get_storage_info(self) -> List[DriveInfo]:
        """Retrieve storage information for all mounted drives"""
        try:
            import psutil
            drives = []
            
            partitions = psutil.disk_partitions()
            for partition in partitions:
                try:
                    usage = psutil.disk_usage(partition.mountpoint)
                    
                    # Try to get drive model (Linux specific)
                    model = None
                    drive_type = None
                    temp = None
                    
                    if sys.platform == "linux":
                        model = self._get_drive_model(partition.device)
                        drive_type = self._get_drive_type(partition.device)
                        temp = self._get_drive_temperature(partition.device)
                    
                    drive = DriveInfo(
                        path=partition.mountpoint,
                        total_gb=usage.total / (1024 ** 3),
                        used_gb=usage.used / (1024 ** 3),
                        available_gb=usage.free / (1024 ** 3),
                        percent_used=usage.percent,
                        drive_type=drive_type,
                        model=model,
                        temp_celsius=temp
                    )
                    drives.append(drive)
                except (PermissionError, OSError):
                    continue
            
            self._drives_info_cache = drives
            return drives
        except ImportError:
            raise ImportError("psutil is required. Install it with: pip install psutil")

    @staticmethod
    def _get_drive_model(device: str) -> Optional[str]:
        """Get drive model name from device path"""
        try:
            # Extract device name (e.g., /dev/sda from /dev/sda1)
            dev_name = Path(device).name.rstrip('0123456789')
            model_path = Path(f'/sys/block/{dev_name}/device/model')
            
            if model_path.exists():
                return model_path.read_text().strip()
        except (OSError, FileNotFoundError):
            pass
        return None

    @staticmethod
    def _get_drive_type(device: str) -> Optional[str]:
        """Determine if drive is SSD or HDD"""
        try:
            dev_name = Path(device).name.rstrip('0123456789')
            rotational_path = Path(f'/sys/block/{dev_name}/queue/rotational')
            
            if rotational_path.exists():
                is_rotational = int(rotational_path.read_text().strip())
                return "HDD" if is_rotational else "SSD"
        except (OSError, ValueError):
            pass
        return None

    @staticmethod
    def _get_drive_temperature(device: str) -> Optional[float]:
        """Get drive temperature using smartctl"""
        try:
            result = subprocess.run(
                ['sudo', 'smartctl', '-A', device],
                capture_output=True,
                text=True,
                timeout=5
            )
            if result.returncode == 0:
                for line in result.stdout.split('\n'):
                    if 'temperature' in line.lower():
                        parts = line.split()
                        if parts:
                            try:
                                return float(parts[-1])
                            except ValueError:
                                pass
        except (subprocess.TimeoutExpired, FileNotFoundError):
            pass
        return None

    def print_report(self):
        """Print a formatted hardware report"""
        print("\n" + "="*80)
        print(" " * 20 + "SYSTEM HARDWARE INFORMATION REPORT")
        print("="*80 + "\n")
        
        self._print_gpu_section()
        self._print_cpu_section()
        self._print_ram_section()
        self._print_storage_section()
        
        print("="*80 + "\n")

    def _print_gpu_section(self):
        """Print GPU information section"""
        print("GPU")
        print("-" * 80)
        
        try:
            gpus = self.get_gpu_info()
        except Exception as e:
            error_msg = f"Error retrieving GPU info: {e}"
            print(f"  {error_msg}")
            self._log("error", error_msg)
            return
        
        if not gpus:
            print("  No GPU detected\n")
            if self.logger:
                self.logger.info("No GPU detected")
            return
        
        for idx, gpu in enumerate(gpus):
            print(f"  GPU {idx}:")
            print(f"    Name:                      {gpu.name}")
            print(f"    VRAM Total:                {gpu.vram_total_gb:.2f} GB")
            print(f"    VRAM Used:                 {gpu.vram_used_gb:.2f} GB / {gpu.vram_total_gb:.2f} GB ({(gpu.vram_used_gb/gpu.vram_total_gb*100):.1f}%)")
            print(f"    VRAM Available:            {gpu.vram_available_gb:.2f} GB")
            
            if gpu.temperature is not None:
                print(f"    Temperature:               {gpu.temperature:.1f}°C")
            if gpu.utilization_percent is not None:
                print(f"    GPU Utilization:           {gpu.utilization_percent:.1f}%")
            if gpu.power_draw_w is not None:
                print(f"    Power Draw:                {gpu.power_draw_w:.1f}W")
            if gpu.compute_capability is not None:
                print(f"    Compute Capability:        {gpu.compute_capability}")
            if gpu.driver_version is not None:
                print(f"    Driver Version:            {gpu.driver_version}")
            print()
            
            if self.logger:
                self.logger.info(f"GPU {idx}: {gpu.name} - VRAM: {gpu.vram_used_gb:.2f}/{gpu.vram_total_gb:.2f} GB")

    def _print_cpu_section(self):
        """Print CPU information section"""
        print("CPU")
        print("-" * 80)
        
        try:
            cpu = self.get_cpu_info()
        except Exception as e:
            error_msg = f"Error retrieving CPU info: {e}"
            print(f"  {error_msg}\n")
            self._log("error", error_msg)
            return
        
        print(f"  Name:                      {cpu.name}")
        print(f"  Cores:                     {cpu.cores}")
        print(f"  Threads:                   {cpu.threads}")
        print(f"  Current Frequency:         {cpu.frequency_ghz:.2f} GHz")
        
        if cpu.max_frequency_ghz is not None:
            print(f"  Max Frequency:             {cpu.max_frequency_ghz:.2f} GHz")
        if cpu.cache_mb is not None:
            print(f"  L3 Cache:                  {cpu.cache_mb:.1f} MB")
        if cpu.architecture is not None:
            print(f"  Architecture:              {cpu.architecture}")
        if cpu.tdp_watts is not None:
            print(f"  TDP:                       {cpu.tdp_watts}W")
        
        print()
        
        if self.logger:
            self.logger.info(f"CPU: {cpu.name} - Cores: {cpu.cores}, Threads: {cpu.threads}, Freq: {cpu.frequency_ghz:.2f} GHz")

    def _print_ram_section(self):
        """Print RAM information section"""
        print("RAM")
        print("-" * 80)
        
        try:
            ram = self.get_ram_info()
        except Exception as e:
            error_msg = f"Error retrieving RAM info: {e}"
            print(f"  {error_msg}\n")
            self._log("error", error_msg)
            return
        
        print(f"  Total:                     {ram.total_gb:.2f} GB")
        print(f"  Used:                      {ram.used_gb:.2f} GB / {ram.total_gb:.2f} GB ({ram.percent_used:.1f}%)")
        print(f"  Available:                 {ram.available_gb:.2f} GB")
        
        if ram.memory_type is not None:
            print(f"  Type:                      {ram.memory_type}")
        if ram.speed_mhz is not None:
            print(f"  Speed:                     {ram.speed_mhz:.0f} MHz")
        if ram.ecc_enabled is not None:
            ecc_status = "Enabled" if ram.ecc_enabled else "Disabled"
            print(f"  ECC:                       {ecc_status}")
        
        print()
        
        if self.logger:
            self.logger.info(f"RAM: {ram.total_gb:.2f} GB total - Used: {ram.used_gb:.2f} GB ({ram.percent_used:.1f}%)")

    def _print_storage_section(self):
        """Print storage information section"""
        print("STORAGE")
        print("-" * 80)
        
        try:
            drives = self.get_storage_info()
        except Exception as e:
            error_msg = f"Error retrieving storage info: {e}"
            print(f"  {error_msg}\n")
            self._log("error", error_msg)
            return
        
        if not drives:
            print("  No drives detected\n")
            if self.logger:
                self.logger.info("No drives detected")
            return
        
        for idx, drive in enumerate(drives):
            print(f"  Drive {idx}: {drive.path}")
            print(f"    Total:                   {drive.total_gb:.2f} GB")
            print(f"    Used:                    {drive.used_gb:.2f} GB / {drive.total_gb:.2f} GB ({drive.percent_used:.1f}%)")
            print(f"    Available:               {drive.available_gb:.2f} GB")
            
            if drive.model:
                print(f"    Model:                   {drive.model}")
            if drive.drive_type:
                print(f"    Type:                    {drive.drive_type}")
            if drive.temp_celsius is not None:
                print(f"    Temperature:             {drive.temp_celsius:.1f}°C")
            
            print()
            
            if self.logger:
                self.logger.info(f"Drive {idx} ({drive.path}): {drive.total_gb:.2f} GB total - Used: {drive.used_gb:.2f} GB ({drive.percent_used:.1f}%)")


def main(logger=None):
    """Main entry point"""
    try:
        check_privileges(logger)
        reporter = HardwareReporter(logger)
        reporter.print_report()
        if logger:
            logger.info("Hardware report generated successfully")
    except ImportError as e:
        error_msg = f"Error: {e}"
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
