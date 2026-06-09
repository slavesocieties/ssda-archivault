#!/usr/bin/env python3
"""
process_random_volume.py

Selects a random unprocessed volume from volumes.json, queries a specified S3 bucket
for all objects having that volume ID as a prefix, and submits the job to the Archivault
image processing pipeline using submit_job.py's implementation.
"""

import os
import sys
import json
import random
import argparse
import getpass
import io
import boto3
from PIL import Image

# Import utilities from submit_job.py
try:
    from submit_job import (
        login,
        submit_job,
        download_artifacts,
        DEFAULT_API_URL,
        DEFAULT_METADATA_SCHEMA
    )
except ImportError:
    print("[!] Error: Could not import 'submit_job.py'. Ensure it is in the current directory.")
    sys.exit(1)


def print_status(msg):
    print(f"[*] {msg}")


def map_language(lang_val):
    """
    Maps language metadata from volumes.json to controlled vocabulary in presign.py:
    ['english', 'spanish', 'portuguese', 'french', 'german', 'mixed']
    """
    if not lang_val:
        return "english"

    if isinstance(lang_val, str):
        raw_langs = [lang_val]
    elif isinstance(lang_val, list):
        raw_langs = [str(l) for l in lang_val if l]
    else:
        return "english"

    valid_vocab = {"english", "spanish", "portuguese", "french", "german", "mixed"}
    matched_langs = []

    for l in raw_langs:
        l_clean = l.strip().lower()
        if l_clean in valid_vocab:
            matched_langs.append(l_clean)
        else:
            # Simple substring/partial matching (e.g. Spanish -> spanish)
            matched = False
            for v in valid_vocab:
                if v in l_clean or l_clean in v:
                    matched_langs.append(v)
                    matched = True
                    break
            # If not matched, it won't be added to matched_langs

    if not matched_langs:
        return "english"
    
    # If there are distinct languages in the list, map to 'mixed'
    if len(set(matched_langs)) > 1:
        return "mixed"
        
    return matched_langs[0]


def map_time_period(start_date, end_date):
    """
    Extracts year information from ISO date strings and maps to controlled vocabulary:
    ['contemporary', 'mid_20th_century', 'early_20th_century', '19th_century_or_earlier']
    """
    start_year = None
    end_year = None

    if start_date and isinstance(start_date, str) and len(start_date) >= 4:
        prefix = start_date[:4]
        if prefix.isdigit():
            start_year = int(prefix)

    if end_date and isinstance(end_date, str) and len(end_date) >= 4:
        prefix = end_date[:4]
        if prefix.isdigit():
            end_year = int(prefix)

    # Resolve reference year
    if start_year and end_year:
        ref_year = (start_year + end_year) // 2
    elif start_year:
        ref_year = start_year
    elif end_year:
        ref_year = end_year
    else:
        # Fallback if no valid years found
        return "19th_century_or_earlier"

    # Map reference year to categories
    if ref_year <= 1900:
        return "19th_century_or_earlier"
    elif 1901 <= ref_year <= 1950:
        return "early_20th_century"
    elif 1951 <= ref_year <= 1975:
        return "mid_20th_century"
    else:
        return "contemporary"


def list_s3_objects_with_prefix(bucket, prefix):
    """
    Queries S3 bucket using a paginator to list all objects with specified prefix.
    Filters out common folder objects (keys ending with /).
    """
    s3_client = boto3.client('s3')
    paginator = s3_client.get_paginator('list_objects_v2')
    keys = []
    
    print_status(f"Querying S3 bucket '{bucket}' for prefix '{prefix}'...")
    try:
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            if 'Contents' in page:
                for obj in page['Contents']:
                    key = obj['Key']
                    if not key.endswith('/'):
                        keys.append(key)
    except Exception as e:
        print(f"[!] S3 client error: {e}")
        sys.exit(1)
        
    return keys


def is_landscape_image(bucket, key):
    """
    Downloads the S3 object in-memory and returns True if its aspect ratio is landscape (width > height).
    """
    s3_client = boto3.client('s3')
    print_status(f"Checking aspect ratio of middle S3 image '{key}'...")
    try:
        file_stream = io.BytesIO()
        s3_client.download_fileobj(bucket, key, file_stream)
        file_stream.seek(0)
        with Image.open(file_stream) as img:
            width, height = img.size
            is_landscape = width > height
            print_status(f"Image dimensions: {width}x{height} (landscape={is_landscape})")
            return is_landscape
    except Exception as e:
        print(f"[!] Warning: Failed to check S3 object orientation for '{key}': {e}")
        return False


def is_ecclesiastical_or_sacramental(subject, type_info):
    """
    Checks if a volume record is an ecclesiastical record or contains sacramental subjects.
    """
    # Check type for "ecclesiastical records"
    is_ecc = False
    if type_info:
        if isinstance(type_info, str):
            is_ecc = "ecclesiastical records" in type_info.lower()
        elif isinstance(type_info, list):
            is_ecc = any("ecclesiastical records" in str(item).lower() for item in type_info if item)
    
    # Check subject for sacramental subjects
    is_sac = False
    sacraments = {"baptism", "marriage", "burial", "baptisms", "marriages", "burials"}
    if subject:
        if isinstance(subject, str):
            val_lower = subject.strip().lower()
            is_sac = any(sac in val_lower for sac in sacraments)
        elif isinstance(subject, list):
            is_sac = any(any(sac in str(item).lower() for sac in sacraments) for item in subject if item)
    
    return is_ecc or is_sac


def main():
    parser = argparse.ArgumentParser(
        description="Select a random unprocessed volume, retrieve S3 objects, and submit a transcription job."
    )
    
    # Bucket & Files
    parser.add_argument("--source-bucket", required=True, help="S3 bucket containing the source files")
    parser.add_argument("--processed-file", help="Path to text file containing list of processed volume IDs (one per line)", default="processed_volumes.txt")
    parser.add_argument("--landscape-file", default="landscape_volumes.txt", help="Path to text file containing list of landscape volume IDs (one per line)")
    parser.add_argument("--volumes-file", default="volumes.json", help="Path to volumes.json database (default: volumes.json)")
    
    # Credentials & API URL
    parser.add_argument("--email", help="Authentication email")
    parser.add_argument("--password", help="Authentication password")
    parser.add_argument("--api-url", default=DEFAULT_API_URL, help="Base API URL for the Archivault backend")
    parser.add_argument("--out-dir", default="./output", help="Directory to save downloaded artifacts")
    
    args = parser.parse_args()

    # 1. Read processed volume IDs
    processed = set()
    if args.processed_file and os.path.exists(args.processed_file):
        print_status(f"Reading processed volumes list from '{args.processed_file}'...")
        try:
            with open(args.processed_file, 'r', encoding='utf-8') as f:
                for line in f:
                    val = line.strip()
                    if val:
                        processed.add(val)
            print_status(f"Found {len(processed)} already processed volume ID(s).")
        except Exception as e:
            print(f"[!] Error reading processed file: {e}")
            sys.exit(1)

    # Read landscape volume IDs
    landscape_set = set()
    if args.landscape_file and os.path.exists(args.landscape_file):
        print_status(f"Reading landscape volumes list from '{args.landscape_file}'...")
        try:
            with open(args.landscape_file, 'r', encoding='utf-8') as f:
                for line in f:
                    val = line.strip()
                    if val:
                        landscape_set.add(val)
            print_status(f"Found {len(landscape_set)} already identified landscape volume ID(s).")
        except Exception as e:
            print(f"[!] Warning: Error reading landscape file: {e}")

    # 2. Parse volumes.json
    if not os.path.exists(args.volumes_file):
        print(f"[!] Error: Volumes file '{args.volumes_file}' not found.")
        sys.exit(1)
        
    print_status(f"Loading volumes from '{args.volumes_file}'...")
    try:
        with open(args.volumes_file, 'r', encoding='utf-8') as f:
            volumes = json.load(f)
    except Exception as e:
        print(f"[!] Error loading JSON volumes file: {e}")
        sys.exit(1)

    # Filter volumes that have not been processed, are not landscape, and are ecclesiastical/sacramental records
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
        print("[!] Error: No available ecclesiastical or sacramental volumes to process.")
        sys.exit(1)

    # 3. Find a volume with S3 objects and verify orientation
    selected_volume = None
    volume_id = None
    keys = []

    # Keep selecting random volumes until we find one with objects in S3 and portrait orientation
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
        print("[!] Error: Checked all available volumes, but none have portrait objects in the S3 bucket.")
        sys.exit(1)

    print_status(f"Successfully selected volume ID: '{volume_id}'")
    fields = selected_volume.get("fields", {})
    title = fields.get("title", f"Volume {volume_id}")
    print_status(f"Volume Title: '{title}'")

    # 4. Map metadata fields
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
    print_status(f"Additional Metadata: country='{country}', state='{state}'")

    # Prompt for credentials
    email = args.email
    if not email:
        email = input("Email: ").strip()
    if not email:
        print("[!] Error: Email is required.")
        sys.exit(1)

    password = args.password
    if not password:
        password = getpass.getpass("Password: ")
    if not password:
        print("[!] Error: Password is required.")
        sys.exit(1)

    # Retrieve login token
    token = login(args.api_url, email, password)

    # 6. Construct Job metadata
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

    # 7. Submit job and poll for completion
    job_id, artifacts, upload_duration, inference_duration = submit_job(
        api_url=args.api_url,
        token=token,
        directory=None,
        files_to_upload=[],
        title=volume_id,
        steps=["transcribe"],
        country=country,
        state=state,
        description=description,
        metadata=metadata,
        source_bucket=args.source_bucket,
        keys=keys
    )

    if artifacts:
        print_status("Downloading artifacts...")
        download_artifacts(artifacts, args.out_dir)
        print_status("Pipeline execution complete.")
        
        # 8. Add to processed file
        if args.processed_file:
            try:
                # Open in append mode
                with open(args.processed_file, 'a', encoding='utf-8') as f:
                    f.write(volume_id + "\n")
                print_status(f"Added volume ID '{volume_id}' to processed file '{args.processed_file}'.")
            except Exception as e:
                print(f"[!] Warning: Could not write to processed file '{args.processed_file}': {e}")
    else:
        print_status("No artifacts were returned for this job.")


if __name__ == "__main__":
    main()
