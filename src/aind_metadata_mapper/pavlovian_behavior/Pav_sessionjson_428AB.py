# -*- coding: utf-8 -*-
"""
Created on Thu Jan  2 00:49:49 2025

@author: kenta.hagihara
"""

import os
import json
import glob
import pandas as pd
from datetime import datetime

# Load the single template JSON file
with open(r"S:\\KentaHagihara_InternalTransfer\\Metadata\\Pavlovian-session-template_428.json", 'r') as file:
    template = json.load(file)

# Set the base path
base_path = r"S:\\svc_aind_behavior_transfer\\428-9-A"
all_new_folders = []
for root, dirs, files in os.walk(base_path):
    for subdir in dirs:
        subfolder_path = os.path.join(root, subdir)
        if len(subfolder_path.split(os.sep)) - len(base_path.split(os.sep)) == 2:
            metadata_dir = os.path.join(subfolder_path, "metadata_dir")
            rig_file_path = os.path.join(metadata_dir, "rig.json")

            if not os.path.exists(rig_file_path):
                print(f"Skipping {subfolder_path}: rig.json not found")
                continue

            with open(rig_file_path, 'r') as rig_file:
                rig_data = json.load(rig_file)

            rig_id = rig_data.get("rig_id")
            if rig_id is not None:
                template["rig_id"] = rig_id

            animalname = os.path.basename(os.path.dirname(subfolder_path))
            template["subject_id"] = animalname

            behavior_folder = os.path.join(subfolder_path, "behavior")
            behavior_files = glob.glob(os.path.join(behavior_folder, "TS_CS1_*.csv"))

            if not behavior_files:
                print(f"Skipping {subfolder_path}: No behavior files found")
                continue

            behavior_file = behavior_files[0]
            raw_time_part = "_".join(os.path.splitext(os.path.basename(behavior_file))[0].split("_")[2:])

            parsed_time = datetime.strptime(raw_time_part, "%Y-%m-%dT%H_%M_%S")
            formatted_time = parsed_time.strftime("%Y-%m-%dT%H:%M:%S.000000-08:00")

            # Update data_streams fields
            if "data_streams" in template and len(template["data_streams"]) > 0:
                template["data_streams"][0]["stream_start_time"] = formatted_time
                template["data_streams"][0]["stream_end_time"] = None

            # Update session start and end times
            template["session_start_time"] = formatted_time
            template["session_end_time"] = None

            # Update stimulus_epochs fields
            trial_files = glob.glob(os.path.join(behavior_folder, "TrialN_TrialType_ITI_*.csv"))

            if not trial_files:
                print(f"Skipping {subfolder_path}: No trial files found")
                continue

            trial_file = trial_files[0]
            trial_data = pd.read_csv(trial_file)

            if "stimulus_epochs" in template and len(template["stimulus_epochs"]) > 0:
                template["stimulus_epochs"][0]["stimulus_start_time"] = formatted_time
                template["stimulus_epochs"][0]["stimulus_end_time"] = None

                trials_finished = int(trial_data["TrialNumber"].iloc[-1])
                trials_total = int(trial_data["TrialNumber"].iloc[-1])
                trials_rewarded = int(trial_data["TotalRewards"].iloc[-1])
                reward_consumed_during_epoch = int(trial_data["TotalRewards"].iloc[-1]) * 2

                template["stimulus_epochs"][0]["trials_finished"] = trials_finished
                template["stimulus_epochs"][0]["trials_total"] = trials_total
                template["stimulus_epochs"][0]["trials_rewarded"] = trials_rewarded
                template["stimulus_epochs"][0]["reward_consumed_during_epoch"] = reward_consumed_during_epoch

            # Append folder name to notes
            folder_name = os.path.basename(subfolder_path)
            if "notes" in template and template["notes"]:
                template["notes"] += f"\nOriginal folder name: {folder_name}"
            else:
                template["notes"] = f"Original folder name: {folder_name}"

            # Rename folder
            new_folder_name = f"behavior_{animalname}_{parsed_time.strftime('%Y-%m-%d_%H-%M-%S')}"
            new_subfolder_path = os.path.join(base_path, new_folder_name)
            os.rename(subfolder_path, new_subfolder_path)
            all_new_folders.append(new_subfolder_path)

            # Update session.json path
            metadata_dir = os.path.join(new_subfolder_path, "metadata_dir")
            session_path = os.path.join(metadata_dir, "session.json")
            with open(session_path, 'w') as session_file:
                json.dump(template, session_file, indent=4)

            print(f"Processed {new_subfolder_path}: session.json created and folder renamed")

# Remove empty directories
for root, dirs, _ in os.walk(base_path, topdown=False):
    for dir_name in dirs:
        dir_path = os.path.join(root, dir_name)
        if dir_path not in all_new_folders and os.path.isdir(dir_path):
            try:
                os.rmdir(dir_path)
                print(f"Removed empty directory: {dir_path}")
            except OSError as e:
                print(f"Failed to remove {dir_path}: {e}")