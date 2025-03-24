import json
import argparse
from pathlib import Path
from aind_metadata_mapper.fiber_photometry.session import ETL
from aind_metadata_mapper.fiber_photometry.models import JobSettings


def create_session_metadata(settings_file: Path, output_path: Path) -> None:
    """Create fiber photometry session metadata and save to specified path.

    Args:
        settings_file: Path to the job settings JSON file
        output_path: Path where session_fib.json should be saved
    """
    # Load settings from JSON file
    with open(settings_file, "r") as f:
        settings_data = json.load(f)

    # Create JobSettings instance and run ETL
    job_settings = JobSettings(**settings_data)
    etl = ETL(job_settings)
    response = etl.run_job()

    # Save output to session_fib.json in specified directory
    output_file = output_path / "session_fib.json"

    # If response.data is a string, try to parse it as JSON first
    if isinstance(response.data, str):
        try:
            data_to_save = json.loads(response.data)
        except json.JSONDecodeError:
            data_to_save = response.data
    else:
        data_to_save = response.data

    # Save with proper formatting
    with open(output_file, "w") as f:
        json.dump(data_to_save, f, indent=2, sort_keys=True)

    print(f"Session metadata saved to: {output_file}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Create fiber photometry session metadata"
    )
    parser.add_argument(
        "settings_file",
        type=Path,
        help="Path to job settings JSON file (if not absolute, assumed to be in script directory)",
    )
    parser.add_argument(
        "--output-path",
        type=Path,
        default=Path.cwd(),
        help="Directory where session_fib.json will be saved (default: current directory)",
    )

    args = parser.parse_args()

    # Handle job settings file path
    if not args.settings_file.is_absolute():
        # If not absolute path, assume file is in script directory
        settings_path = Path(__file__).resolve().parent / args.settings_file
    else:
        settings_path = args.settings_file

    # Verify settings file exists
    if not settings_path.exists():
        parser.error(f"Job settings file not found: {settings_path}")

    # Ensure output directory exists
    args.output_path.mkdir(parents=True, exist_ok=True)

    # Create session metadata
    create_session_metadata(settings_path, args.output_path)
