#!/usr/bin/env python3
"""
automate_random_volumes.py

Automated wrapper script for sequential random volume processing.
Models after automate_pipeline.py, prompting for credentials and sequential runs,
filtering for ecclesiastical/sacramental volumes, submitting transcription jobs
to Archivault, downloading artifacts named by Volume ID, and logging execution metrics.
"""

import os
import sys
import getpass
import time
import json
import random
import datetime
import requests
import argparse

# Import process helpers
try:
    from process_random_volume import (
        map_language,
        map_time_period,
        list_s3_objects_with_prefix,
        is_ecclesiastical_or_sacramental,
        is_landscape_image
    )
except ImportError:
    print("[!] Error: Could not import 'process_random_volume.py'. Ensure it is in the current directory.")
    sys.exit(1)

# Import submit job helpers
try:
    from submit_job import (
        login,
        submit_job,
        DEFAULT_API_URL,
        DEFAULT_METADATA_SCHEMA
    )
except ImportError:
    print("[!] Error: Could not import 'submit_job.py'. Ensure it is in the current directory.")
    sys.exit(1)


def print_status(msg):
    print(f"[*] {msg}")


def download_artifacts_by_volume_id(artifacts, output_dir, volume_id):
    """
    Downloads job artifacts named by the volume ID rather than original S3 keys.
    """
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        
    for key, info in artifacts.items():
        if isinstance(info, dict) and 'presigned_url' in info:
            url = info['presigned_url']
            if not url:
                continue
                
            # Determine extension based on artifact type/key
            if key == 'json':
                filename = f"{volume_id}.json"
            elif key == 'markdown':
                filename = f"{volume_id}.md"
            elif key == 'tables_zip':
                filename = f"{volume_id}_tables.zip"
            else:
                filename = f"{volume_id}_{key}"
                
            filepath = os.path.join(output_dir, filename)
            print_status(f"Downloading {key} artifact to {filepath}...")
            
            resp = requests.get(url, stream=True)
            if resp.ok:
                with open(filepath, 'wb') as f:
                    for chunk in resp.iter_content(chunk_size=8192):
                        f.write(chunk)
            else:
                print(f"[!] Failed to download {key} artifact: {resp.status_code}")


def split_keys(keys_list, max_size=1500):
    """
    Recursively divides a list of S3 keys in half until all resulting
    sublists contain at most max_size keys.
    """
    if len(keys_list) <= max_size:
        return [keys_list]
    mid = len(keys_list) // 2
    left = keys_list[:mid]
    right = keys_list[mid:]
    return split_keys(left, max_size) + split_keys(right, max_size)


def main():
    parser = argparse.ArgumentParser(
        description="Automation wrapper for sequential Archivault processing of random volumes."
    )
    
    # Defaults based on process_random_volume.py + request
    parser.add_argument("--source-bucket", default="ssda-production-jpgs", help="S3 bucket containing the source files (default: ssda-production-jpgs)")
    parser.add_argument("--processed-file", default="processed_volumes.txt", help="Path to text file containing list of processed volume IDs (default: processed_volumes.txt)")
    parser.add_argument("--landscape-file", default="landscape_volumes.txt", help="Path to text file containing list of landscape volume IDs (default: landscape_volumes.txt)")
    parser.add_argument("--volumes-file", default="volumes.json", help="Path to volumes.json database (default: volumes.json)")
    parser.add_argument("--api-url", default=DEFAULT_API_URL, help="Base API URL for the Archivault backend")
    parser.add_argument("--out-dir", default="./output", help="Directory to save downloaded artifacts (default: ./output)")
    parser.add_argument("--log-file", default="pipeline_run_log.txt", help="Log file for multi-job execution statistics (default: pipeline_run_log.txt)")

    args = parser.parse_args()

    print("\n" + "="*60)
    print("    SSDA RANDOM VOLUME PROCESSING WRAPPER    ")
    print("="*60 + "\n")

    # 1. Prompt securely for Archivault credentials
    print_status("Authentication required")
    email = input("Email: ").strip()
    if not email:
        print("[!] Error: Email cannot be empty.")
        sys.exit(1)
        
    password = getpass.getpass("Password: ")
    if not password:
        print("[!] Error: Password cannot be empty.")
        sys.exit(1)

    # 2. Login immediately to verify credentials and obtain JWT token
    print()
    token = login(args.api_url, email, password)
    print()

    # 3. Prompt for sequential loop iterations
    print_status("Automation loop configuration")
    while True:
        runs_str = input("Number of volumes to process sequentially: ").strip()
        try:
            iterations = int(runs_str)
            if iterations <= 0:
                print("[!] Please enter a positive integer greater than 0.")
                continue
            break
        except ValueError:
            print("[!] Invalid input. Please enter a valid integer.")
    print()

    # Metrics trackers
    run_start_time = time.time()
    total_images_processed = 0
    volumes_processed_count = 0
    job_details = []

    # 4. Sequentially process volumes
    for run_idx in range(1, iterations + 1):
        print("\n" + "="*60)
        print(f"    SEQUENTIAL RUN {run_idx} OF {iterations}")
        print("="*60 + "\n")

        volume_start_time = time.time()

        # Reload processed set each run to maintain dynamic history
        processed = set()
        if args.processed_file and os.path.exists(args.processed_file):
            try:
                with open(args.processed_file, 'r', encoding='utf-8') as f:
                    for line in f:
                        val = line.strip()
                        if val:
                            processed.add(val)
            except Exception as e:
                print(f"[!] Warning: Error reading processed file: {e}")

        # Reload landscape set each run to maintain dynamic history
        landscape_set = set()
        if args.landscape_file and os.path.exists(args.landscape_file):
            try:
                with open(args.landscape_file, 'r', encoding='utf-8') as f:
                    for line in f:
                        val = line.strip()
                        if val:
                            landscape_set.add(val)
            except Exception as e:
                print(f"[!] Warning: Error reading landscape file: {e}")

        # Reload/Parse volumes.json
        if not os.path.exists(args.volumes_file):
            print(f"[!] Error: Volumes file '{args.volumes_file}' not found.")
            sys.exit(1)
            
        try:
            with open(args.volumes_file, 'r', encoding='utf-8') as f:
                volumes = json.load(f)
        except Exception as e:
            print(f"[!] Error loading JSON volumes file: {e}")
            sys.exit(1)

        # Filter available volumes (unprocessed, not landscape & ecclesiastical/sacramental)
        available_volumes = []
        for vol in volumes:
            vol_id = str(vol.get("id"))
            if vol_id not in processed and vol_id not in landscape_set:
                fields = vol.get("fields", {})
                subject_val = fields.get("subject", [])
                type_val = fields.get("type", [])
                if is_ecclesiastical_or_sacramental(subject_val, type_val):
                    available_volumes.append(vol)

        print_status(f"Found {len(available_volumes)} available (unprocessed ecclesiastical/sacramental) volume(s) out of {len(volumes)} total.")
        if not available_volumes:
            print_status("Warning: No available volumes left to process. Stopping loop.")
            break

        # Select volume and list S3 keys
        selected_volume = None
        volume_id = None
        keys = []

        while available_volumes:
            candidate = random.choice(available_volumes)
            candidate_id = str(candidate.get("id"))
            candidate_keys = list_s3_objects_with_prefix(args.source_bucket, candidate_id)
            if candidate_keys:
                # Check aspect ratio of the middle image
                sorted_keys = sorted(candidate_keys)
                middle_key = sorted_keys[len(sorted_keys) // 2]
                
                if is_landscape_image(args.source_bucket, middle_key):
                    print_status(f"Volume ID '{candidate_id}' has landscape aspect ratio. Marking and skipping...")
                    if args.landscape_file:
                        try:
                            with open(args.landscape_file, 'a', encoding='utf-8') as f:
                                f.write(candidate_id + "\n")
                        except Exception as e:
                            print(f"[!] Warning: Could not write to landscape file: {e}")
                    available_volumes.remove(candidate)
                    continue
                    
                selected_volume = candidate
                volume_id = candidate_id
                keys = candidate_keys
                break
            else:
                print_status(f"No S3 objects found under prefix '{candidate_id}'. Skipping and selecting another...")
                available_volumes.remove(candidate)

        if not selected_volume:
            print_status("Warning: Checked all available volumes, but none have portrait objects in the S3 bucket. Stopping loop.")
            break

        print_status(f"Selected volume ID: '{volume_id}'")
        fields = selected_volume.get("fields", {})
        title = fields.get("title", f"Volume {volume_id}")
        print_status(f"Volume Title: '{title}'")

        # Map metadata fields
        raw_lang = fields.get("language")
        mapped_language = map_language(raw_lang)
        print_status(f"Language Mapping: original={raw_lang} -> mapped='{mapped_language}'")

        start_date = fields.get("start_date")
        end_date = fields.get("end_date")
        mapped_time_period = map_time_period(start_date, end_date)
        print_status(f"Time Period Mapping: start={start_date}, end={end_date} -> mapped='{mapped_time_period}'")

        country = "US"
        state = "TN"
        description = fields.get("description", "")

        # Construct Job metadata
        metadata = {
            "writing_style": "",
            "language": mapped_language,
            "time_period": mapped_time_period,
            "layout_structure": "",
            "transcription_model": "gemini-3.1-pro-preview",
            "captioning_model": "gemini-3.1-flash-lite",
            "foliation_model": "gemini-3.1-flash-lite",
            "aggregation_model": "gemini-3.1-flash-lite",
            "metadata_model": "gemini-3.1-flash-lite",
            "non_textual_elements": [],
            "transcription_preferences": {
                "expand_abbreviations": False,
                "preserve_line_breaks": True,
                "retain_punctuation_and_spelling": True,
                "normalize_to_modern_language": False,
                "ignore_marginalia": False
            },
            "metadata_schema": DEFAULT_METADATA_SCHEMA,
            "additional_context_file": "",
            "additional_context_modules": ["foliation", "metadata", "transcription", "ner", "aggregation", "captioning", "layout"],
            "foliation_file": "",
            "foliation_override_discrete": False,
            "delete_data": True,
            "transcription_instructions": ""
        }

        # Split keys if they exceed max_size (1500)
        parts = split_keys(keys, max_size=1500)
        
        all_parts_successful = True
        volume_total_images = 0
        
        if len(parts) > 1:
            print_status(f"Volume '{volume_id}' consists of {len(keys)} images. Divided into {len(parts)} parts for sequential processing.")
            
        for part_idx, part_keys in enumerate(parts):
            if len(parts) > 1:
                part_title = f"{volume_id}_{part_idx + 1}"
                print_status(f"\n--- Processing Part {part_idx + 1} of {len(parts)}: '{part_title}' ({len(part_keys)} images) ---")
            else:
                part_title = volume_id

            part_start_time = time.time()
            
            # Submit job and poll for completion
            job_id, artifacts, upload_duration, inference_duration = submit_job(
                api_url=args.api_url,
                token=token,
                directory=None,
                files_to_upload=[],
                title=part_title,
                steps=["transcribe"],
                country=country,
                state=state,
                description=description,
                metadata=metadata,
                source_bucket=args.source_bucket,
                keys=part_keys
            )

            if artifacts:
                # Download using part_title as filename
                print_status("Downloading artifacts...")
                download_artifacts_by_volume_id(artifacts, args.out_dir, part_title)
                print_status(f"Part '{part_title}' completed successfully!")
                
                # Update metrics
                part_image_count = len(part_keys)
                total_images_processed += part_image_count
                volume_total_images += part_image_count
                part_duration = time.time() - part_start_time
                job_details.append(f"{part_title} ({part_image_count} images in {part_duration:.2f}s)")
            else:
                print_status(f"Warning: No artifacts returned for part '{part_title}'.")
                all_parts_successful = False

        if all_parts_successful:
            print_status(f"Volume {volume_id} completed successfully (all parts done)!")
            # Record processed volume
            if args.processed_file:
                try:
                    with open(args.processed_file, 'a', encoding='utf-8') as f:
                        f.write(volume_id + "\n")
                    print_status(f"Added volume ID '{volume_id}' to processed file '{args.processed_file}'.")
                except Exception as e:
                    print(f"[!] Warning: Could not write to processed file: {e}")
            volumes_processed_count += 1
        else:
            print_status(f"Warning: One or more parts failed for volume {volume_id}. Volume not marked as processed.")

    # 5. Log execution metrics for multi-job runs
    total_duration = time.time() - run_start_time
    if iterations > 1 and volumes_processed_count > 0:
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        job_details_str = ", ".join(job_details)
        log_line = (
            f"[{timestamp}] Multi-job run complete. "
            f"Total duration: {total_duration:.2f} seconds | "
            f"Total volumes processed: {volumes_processed_count} | "
            f"Total images processed: {total_images_processed} | "
            f"Job details: {job_details_str}\n"
        )
        try:
            with open(args.log_file, 'a', encoding='utf-8') as f:
                f.write(log_line)
            print_status(f"Execution statistics logged successfully to '{args.log_file}'")
        except Exception as e:
            print(f"[!] Error writing statistics to log file: {e}")

    print("\n" + "="*60)
    print("    PIPELINE SEQUENTIAL RUNS COMPLETE    ")
    print("="*60 + "\n")
    print(f"Total volumes processed: {volumes_processed_count}")
    print(f"Total images processed:  {total_images_processed}")
    print(f"Total elapsed duration:  {total_duration:.2f} seconds\n")


if __name__ == "__main__":
    main()
