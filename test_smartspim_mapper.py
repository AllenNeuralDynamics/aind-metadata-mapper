import json
from pathlib import Path
from aind_metadata_mapper.smartspim.mapper import SmartspimMapper

def main():
    json_path = Path("tests/resources/smartspim/smartspim.json")
    
    with open(json_path, 'r') as f:
        metadata = json.load(f)
    
    mapper = SmartspimMapper()
    acquisition = mapper.transform(metadata)
    
    print(acquisition.model_dump_json(indent=2))

if __name__ == "__main__":
    main()
