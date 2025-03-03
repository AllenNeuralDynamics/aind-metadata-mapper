from pathlib import Path
import json
from aind_metadata_mapper.fip.session import FIBEtl
from aind_metadata_mapper.fip.job_settings import JobSettings


def test_fip_workflow():
    # Get the directory where this script lives
    example_dir = Path(__file__).parent

    # Load settings from JSON in same directory
    settings_path = example_dir / "job_settings.json"
    with open(settings_path, "r") as f:
        settings_data = json.load(f)

    # Create JobSettings instance
    job_settings = JobSettings(**settings_data)

    # Create and run ETL
    etl = FIBEtl(job_settings)
    response = etl.run_job()

    # Write the resulting session to JSON in same directory
    output_path = example_dir / "generated_session.json"
    with open(output_path, "w") as f:
        json.dump(json.loads(response.data), f, indent=2)

    print(f"Generated session saved to: {output_path}")


if __name__ == "__main__":
    test_fip_workflow()
