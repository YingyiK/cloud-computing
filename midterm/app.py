#!/usr/bin/env python3

import aws_cdk as cdk

from stacks.cleaner_stack import CleanerStack
from stacks.replicator_stack import ReplicatorStack
from stacks.storage_stack import StorageStack

app = cdk.App()

storage = StorageStack(app, "Midterm-Storage")

ReplicatorStack(
    app,
    "Midterm-Replicator",
    src_bucket=storage.src_bucket,
    dst_bucket=storage.dst_bucket,
    table=storage.table,
)

CleanerStack(
    app,
    "Midterm-Cleaner",
    dst_bucket=storage.dst_bucket,
    table=storage.table,
)

app.synth()

