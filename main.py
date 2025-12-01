import argparse
import sys
from utils.logger import LoggerWrapper
from utils.hardware.get_hardware import main as run_hardware_specs

class Flags:
    def __init__(self):
        self.parser = argparse.ArgumentParser(description="Runner with flags")
        self.parser.add_argument('--task', type=str, help='Specify the task to run')
        self.parser.add_argument('--verbose', action='store_true', help='Enable verbose output')
        self.parser.add_argument('--pcspecs', action='store_true', help='Display PC specifications')
        # Add more flags as needed

    def parse(self):
        return self.parser.parse_args()

class Runner:
    def __init__(self, flags):
        self.flags = flags
        log_level = "DEBUG" if self.flags.verbose else "INFO"
        self.logger = LoggerWrapper(level=log_level)

    def run(self):
        if self.flags.pcspecs:
            self.run_pcspecs()
        elif self.flags.task == 'example_task':
            self.run_example_task()
        elif self.flags.task:
            self.logger.error(f"Unknown task: {self.flags.task}")
        else:
            self.logger.error("No task specified. Use --task or --pcspecs flag")

    def run_example_task(self):
        if self.flags.verbose:
            self.logger.debug("Running example task in verbose mode")
        else:
            self.logger.info("Running example task")

    def run_pcspecs(self):
        self.logger.info("Retrieving PC specifications...")
        run_hardware_specs(self.logger)

if __name__ == "__main__":
    flags = Flags()
    args = flags.parse()
    runner = Runner(args)
    runner.run()