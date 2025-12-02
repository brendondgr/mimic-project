import argparse
import sys
from utils.logger import LoggerWrapper
from utils.hardware.get_hardware import main as run_hardware_specs
from utils.download.download_dataset import main as run_download_dataset

class Flags:
    def __init__(self):
        self.parser = argparse.ArgumentParser(description="Runner with flags")
        self.parser.add_argument('--pcspecs', action='store_true', help='Display PC specifications')
        self.parser.add_argument('--download', action='store_true', help='Download MIMIC-IV dataset from PhysioNet')
        self.parser.add_argument('--app', type=str, choices=['data'], help='Run a Flask application (data)')
        # Add more flags as needed

    def parse(self):
        return self.parser.parse_args()

class Runner:
    def __init__(self, flags):
        self.flags = flags
        self.logger = LoggerWrapper(level="INFO")

    def run(self):
        if self.flags.pcspecs:
            self.run_pcspecs()
        elif self.flags.download:
            self.run_download()
        elif self.flags.app:
            self.run_app()
        else:
            self.logger.error("No task specified. Use --pcspecs, --download, or --app flag")

    def run_pcspecs(self):
        self.logger.info("Retrieving PC specifications...")
        run_hardware_specs(self.logger)

    def run_download(self):
        self.logger.info("Initiating MIMIC-IV dataset download...")
        run_download_dataset(self.logger)

    def run_app(self):
        """Run the selected Flask application."""
        if self.flags.app == 'data':
            self.logger.info("Starting Data Flask application...")
            from apps.data import create_data_app
            app = create_data_app()
            app.run(debug=True)
        else:
            self.logger.error(f"Unknown app: {self.flags.app}")

if __name__ == "__main__":
    flags = Flags()
    args = flags.parse()
    runner = Runner(args)
    runner.run()