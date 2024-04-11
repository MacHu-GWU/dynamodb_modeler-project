# -*- coding: utf-8 -*-

"""
What is This
------------------------------------------------------------------------------
This is an example of how to implement a simple versioned document system in DynamoDB.


Analyze Business Requirement
------------------------------------------------------------------------------
**Entities**

**Relationships**

**User Interaction**

Reference:

- Sample data: https://docs.google.com/spreadsheets/d/1PG2YyBoH2NoPyhcGhweARX2XdbRlh8-EEH9YD4IidHU/edit#gid=1040270410
"""

import typing as T
import dataclasses

import pynamodb_mate as pm
import pynamodb.exceptions as exc

from moto import mock_dynamodb

# create a DynamoDB connection, ensure that your default AWS credential is right
# if you are using mock, then this line always works
connect = pm.Connection()

# use moto to mock DynamoDB, it is an in-memory implementation of DynamoDB
# you can also use the real DynamoDB table by just comment out the below two line
mock = mock_dynamodb()
mock.start()

# Type hint notation helper
REQUIRED_STR = T.Union[str, pm.UnicodeAttribute]
OPTIONAL_STR = T.Optional[REQUIRED_STR]


# ------------------------------------------------------------------------------
# Define DynamoDB Data Model
# ------------------------------------------------------------------------------
class Entity(pm.Model):
    """ """

    class Meta:
        table_name = "entity"
        region = "us-east-1"
        billing_mode = pm.PAY_PER_REQUEST_BILLING_MODE

    # partition key and sort key
    pk: REQUIRED_STR = pm.UnicodeAttribute(hash_key=True)
    sk: REQUIRED_STR = pm.UnicodeAttribute(range_key=True)


Entity.create_table(wait=True)

# ------------------------------------------------------------------------------
# Implement Business Operations
# ------------------------------------------------------------------------------
@dataclasses.dataclass
class BusinessOperation:
    def print_all(self):
        for item in Entity.scan():
            print(join_path(item.pk, item.sk))

op = BusinessOperation()

# ------------------------------------------------------------------------------
# Setup Dummy Data For Testing
# ------------------------------------------------------------------------------
Entity(pk="/", sk="__root__").save()


# ------------------------------------------------------------------------------
# Test Business Operations
# ------------------------------------------------------------------------------
def print_all(lst):
    for item in lst:
        print(item)
