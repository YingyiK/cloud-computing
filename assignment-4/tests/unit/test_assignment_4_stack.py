import aws_cdk as core
import aws_cdk.assertions as assertions

from assignment_4.assignment_4_stack import Assignment4Stack

# example tests. To run these tests, uncomment this file along with the example
# resource in assignment_4/assignment_4_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = Assignment4Stack(app, "assignment-4")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
