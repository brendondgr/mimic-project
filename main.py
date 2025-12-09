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
        self.parser.add_argument('--app', type=str, choices=['data', 'bpm'], help='Run a Flask application (data, bpm)')
        self.parser.add_argument('--optimize-index', nargs='?', const='all', help='Generate byte-offset index for specified file (default: all)')
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
        elif self.flags.optimize_index:
            self.run_optimize_index()
        else:
            self.logger.error("No task specified. Use --pcspecs, --download, --app, or --optimize-index flag")

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
        elif self.flags.app == 'bpm':
            self.logger.info("Starting BPM Flask application...")
            from apps.bpm import create_bpm_app
            app = create_bpm_app()
            app.run(debug=True)
        else:
            self.logger.error(f"Unknown app: {self.flags.app}")

    def run_optimize_index(self):
        """Generate byte-offset index for specified file(s) and verify optimization."""
        target = self.flags.optimize_index
        self.logger.info(f"Starting optimization index generation for: {target}")
        
        # We need to update verify_optimization.py to accept the target argument
        # Or we can just call create_index directly here if we want to skip the "verify" wrapper
        # But the user asked for verification.
        # Let's import create_index directly for the work, and maybe run a quick verify after?
        # Actually, the previous verify_optimization.py was hardcoded for chartevents.
        # We should update verify_optimization.py to be more flexible too.
        
        from utils.analysis.create_lookup_index import create_index
        create_index(target)
        
        # Verify
        from utils.tests.verify_optimization import verify
        from utils.analysis.filtering import IDs
        
        if target and target.lower() != 'all':
            verify(self.logger, target)
        else:
            # Verify all or just a sample? Verifying all might be verbose.
            # Let's verify just chartevents as a smoke test, or iterate if the user wants.
            # Since 'all' takes a long time, the user probably wants to know it worked.
            self.logger.info("Verifying optimization for all processed files...")
            for file_id in IDs:
                 # Only verify if the file exists
                 if "location" in IDs[file_id]:
                     # We can just try verifying, it will skip if columns missing
                     verify(self.logger, file_id)
        
        self.logger.info("Optimization process completed.")

if __name__ == "__main__":
    flags = Flags()
    args = flags.parse()
    runner = Runner(args)
    runner.run()