#!/usr/bin/env python3

import aws_cdk as cdk

from stacks.data_stack import DataStack
from stacks.bucket_tracking_stack import BucketTrackingStack
from stacks.plotting_api_stack import PlottingApiStack
from stacks.driver_stack import DriverStack


app = cdk.App()

data = DataStack(app, "Assignment3-Data")

bucket_tracking = BucketTrackingStack(
    app,
    "Assignment3-BucketTracking",
    table=data.table,
)

plotting_api = PlottingApiStack(
    app,
    "Assignment3-PlottingApi",
    bucket=bucket_tracking.bucket,
    table=data.table,
)

DriverStack(
    app,
    "Assignment3-Driver",
    bucket=bucket_tracking.bucket,
    plot_api_url=plotting_api.plot_api_url,
)

app.synth()

