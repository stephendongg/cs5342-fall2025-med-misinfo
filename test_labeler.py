"""Script for testing the automated labeler"""

import warnings
# Suppress pydantic warnings from dependencies
warnings.filterwarnings("ignore", category=UserWarning, module="pydantic")

import argparse
import json

import pandas as pd
from atproto import Client
from dotenv import load_dotenv
from pylabel.policy_proposal_labeler import PolicyProposalLabeler
from pylabel import label_post, did_from_handle

load_dotenv(override=True)
USERNAME = 'tns-labeler.bsky.social'
PW = '20251107'

def main():
    """
    Main function for the test script
    """
    client = Client()
    labeler_client = None
    client.login(USERNAME, PW)
    did = did_from_handle(USERNAME)

    parser = argparse.ArgumentParser()
    parser.add_argument("input_urls", type=str)
    parser.add_argument("--emit_labels", action="store_true")
    args = parser.parse_args()

    if args.emit_labels:
        labeler_client = client.with_proxy("atproto_labeler", did)

    labeler = PolicyProposalLabeler(client)

    test_data = pd.read_csv(args.input_urls)
    num_correct, total = 0, test_data.shape[0]
    
    # Check that CSV has required columns
    if "Labels" not in test_data.columns:
        raise ValueError("CSV must contain 'Labels' column.")
    if "URL" not in test_data.columns and "Text" not in test_data.columns:
        raise ValueError("CSV must contain either 'Text' or 'URL' column.")
    
    for _index, row in test_data.iterrows():
        expected_labels = json.loads(row["Labels"])
        
        # Determine input column and source per row (handles CSV with both columns)
        text_val = row.get("Text") if "Text" in row.index else None
        url_val = row.get("URL") if "URL" in row.index else None
        
        if text_val is not None and pd.notna(text_val) and str(text_val).strip():
            # Use Text column if it has a value
            labels = labeler.moderate_post(text=str(text_val), 
                                          expected_labels=expected_labels, 
                                          source="generated")
            input_value = str(text_val)
        elif url_val is not None and pd.notna(url_val) and str(url_val).strip():
            # Use URL column if it has a value
            labels = labeler.moderate_post(url=str(url_val), 
                                          expected_labels=expected_labels, 
                                          source="real")
            input_value = str(url_val)
        else:
            print(f"Row {_index}: Both URL and Text are empty, skipping")
            continue
        
        if sorted(labels) == sorted(expected_labels):
            num_correct += 1
        else:
            print(f"For {input_value}, labeler produced {labels}, expected {expected_labels}")
        if args.emit_labels and (len(labels) > 0) and labeler_client:
            label_post(client, labeler_client, input_value, labels)
    
    print(f"The labeler produced {num_correct} correct labels assignments out of {total}")
    print(f"Overall ratio of correct label assignments {num_correct/total}")


if __name__ == "__main__":
    main()
