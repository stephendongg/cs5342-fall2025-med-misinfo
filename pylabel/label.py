"""Command-line tool for labeling posts and accounts on Bluesky"""

import argparse
import os
from typing import List

import requests
from atproto import Client, models
from atproto_client.models.com.atproto.admin.defs import RepoRef
from atproto_client.models.com.atproto.repo.strong_ref import Main
from dotenv import load_dotenv

load_dotenv(override=True)
USERNAME = os.getenv("USERNAME")
PW = os.getenv("PW")

def did_from_handle(handle: str):
    """
    Resolve the DID associated with a handle.

    Args:
        handle (str): The handle to resolve.

    Returns:
        str: The DID associated with the input handle.
    """
    # via: https://github.com/skygaze-ai/atproto-101
    return requests.get(
        "https://bsky.social/xrpc/com.atproto.identity.resolveHandle",
        params={"handle": handle},
        timeout=10,
    ).json()["did"]


def post_from_url(client: Client, url: str):
    """
    Retrieve a Bluesky post from its URL
    """
    parts = url.split("/")
    rkey = parts[-1]
    handle = parts[-3]
    return client.get_post(rkey, handle)


def label_account(client: Client, handle: str, label_value: List[str]):
    """
    Apply a label to an account with the specified handle
    """
    did = did_from_handle(handle)
    data = models.ToolsOzoneModerationEmitEvent.Data(
        created_by=client.me.did,
        event=models.ToolsOzoneModerationDefs.ModEventLabel(
            create_label_vals=label_value,
            negate_label_vals=[],
        ),
        subject=RepoRef(did=did),
        subject_blob_cids=[],
    )
    return client.tools.ozone.moderation.emit_event(data)


def label_post(
    client: Client, labeler_client: Client, post_url: str, label_value: List[str]
):
    """
    Apply a label to a post with the specified URL
    """
    post = post_from_url(client, post_url)
    post_ref = Main(cid=post.cid, uri=post.uri)
    data = models.ToolsOzoneModerationEmitEvent.Data(
        created_by=client.me.did,
        event=models.ToolsOzoneModerationDefs.ModEventLabel(
            create_label_vals=label_value,
            negate_label_vals=[],
        ),
        subject=post_ref,
        subject_blob_cids=[],
    )
    return labeler_client.tools.ozone.moderation.emit_event(data)


def main():
    """
    Main function for command-line tool.
    """
    client = Client()
    client.login(USERNAME, PW)
    did = did_from_handle(USERNAME)
    labeler_client = client.with_proxy("atproto_labeler", did)
    parser = argparse.ArgumentParser()
    parser.add_argument("label_target", type=str)
    parser.add_argument("target_id", type=str)
    parser.add_argument("label_value", type=str)
    args = parser.parse_args()
    label_target, target_id, label_value = (
        args.label_target,
        args.target_id,
        args.label_value,
    )

    if label_target == "post":
        result = label_post(client, labeler_client, target_id, [label_value])
    elif label_target == "account":
        result = label_account(labeler_client, target_id, [label_value])
    else:
        raise ValueError("Error: Invalid target")

    print("result:", result)


if __name__ == "__main__":
    main()
