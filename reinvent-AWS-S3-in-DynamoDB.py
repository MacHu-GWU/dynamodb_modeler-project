# -*- coding: utf-8 -*-

"""
What is This
------------------------------------------------------------------------------
This is an example of how to implement a simple hierarchy file system in DynamoDB.


Analyze Business Requirement
------------------------------------------------------------------------------
**Entities**

- 📁 **Directory**
- 📄 **File**

There's a root directory ``/``. The parent directory of ``/`` is ``None``.

**Relationships**

- **Directory or File** can be a child of another **Directory**

**User Interaction**

- Make directory
- Make file
- List all directory and file

Reference:

- Sample data: https://docs.google.com/spreadsheets/d/1PG2YyBoH2NoPyhcGhweARX2XdbRlh8-EEH9YD4IidHU/edit#gid=1168957421
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
        table_name = "path"
        region = "us-east-1"
        billing_mode = pm.PAY_PER_REQUEST_BILLING_MODE

    # partition key and sort key
    pk: REQUIRED_STR = pm.UnicodeAttribute(hash_key=True)
    sk: REQUIRED_STR = pm.UnicodeAttribute(range_key=True)


Entity.create_table(wait=True)


def split_path(path: str) -> tuple[T.Optional[str], str]:
    if path == "/":
        return None, path
    # /a/ -> (/, a/)
    # /a/b/ -> (/a/, b/)
    elif path.endswith("/"):
        a, b, c = path.rsplit("/", 2)
        return f"{a}/", f"{b}/"
    else:
        a, b = path.rsplit("/", 1)
        # /a -> (/, a)
        if a == "":
            return "/", b
        # /a/b -> (/a/, b)
        else:
            return f"{a}/", b


def test_split_path():
    assert split_path("/") == (None, "/")
    assert split_path("/a") == ("/", "a")
    assert split_path("/a/") == ("/", "a/")
    assert split_path("/a/b/") == ("/a/", "b/")


test_split_path()


def join_path(pk: str, sk: str):
    if sk == "__root__":
        return "/"
    else:
        return f"{pk}{sk}"


def test_join_path():
    assert join_path("/", "__root__") == "/"
    assert join_path("/", "a") == "/a"
    assert join_path("/", "a/") == "/a/"
    assert join_path("/a/", "b") == "/a/b"
    assert join_path("/a/", "b/") == "/a/b/"


test_join_path()


class TypeEnum:
    DIR = "D"
    FILE = "F"


# ------------------------------------------------------------------------------
# Implement Business Operations
# ------------------------------------------------------------------------------
@dataclasses.dataclass
class BusinessOperation:
    def print_all(self):
        for item in Entity.scan():
            print(join_path(item.pk, item.sk))

    def exists(self, dir_path: str):
        """
        Check if a directory or file exists
        """
        if dir_path == "/":
            return True
        a, b = split_path(dir_path)
        try:
            Entity.get(a, b)
            return True
        except exc.DoesNotExist as e:
            return False

    def make_dir(
        self,
        name: str,
        dir_path: str,
    ):
        """
        Make a new directory.
        """
        if "/" in name:
            raise ValueError
        if self.exists(dir_path):
            try:
                Entity(pk=dir_path, sk=f"{name}/").save(
                    condition=Entity.pk.does_not_exist() & Entity.sk.does_not_exist()
                )
            except exc.PutError as e:
                pass
        else:
            raise FileNotFoundError(f"dir {dir_path!r} not found!")

    def make_file(
        self,
        name: str,
        dir_path: str,
    ):
        """
        Make a new file.
        """
        if "/" in name:
            raise ValueError
        if self.exists(dir_path):
            try:
                Entity(pk=dir_path, sk=name).save(
                    condition=Entity.pk.does_not_exist() & Entity.sk.does_not_exist()
                )
            except exc.PutError as e:
                pass
        else:
            raise FileNotFoundError(f"dir {dir_path!r} not found!")

    def listdir(self, dir_path: str) -> list[str]:
        return [join_path(item.pk, item.sk) for item in Entity.query(dir_path)]


op = BusinessOperation()

# ------------------------------------------------------------------------------
# Setup Dummy Data For Testing
# ------------------------------------------------------------------------------
Entity(pk="/", sk="__root__").save()
op.make_dir(name="documents", dir_path="/")
op.make_file(name="file1.txt", dir_path="/")
op.make_file(name="file2.txt", dir_path="/")
op.make_dir(name="images", dir_path="/documents/")
op.make_file(name="deck1.ppt", dir_path="/documents/")
op.make_file(name="deck2.ppt", dir_path="/documents/")
op.make_file(name="logo.png", dir_path="/documents/images/")
op.make_file(name="icon.png", dir_path="/documents/images/")

op.print_all()


# ------------------------------------------------------------------------------
# Test Business Operations
# ------------------------------------------------------------------------------
def print_all(lst):
    for item in lst:
        print(item)


print("--- listdir: / ---")
print_all(op.listdir("/"))

print("--- listdir: /documents/ ---")
print_all(op.listdir("/documents/"))

print("--- listdir: /documents/images/ ---")
print_all(op.listdir("/documents/images/"))

print("--- exists: /documents/ ---")
print(op.exists("/documents/"))

print("--- exists: /documents/deck1.ppt ---")
print(op.exists("/documents/deck1.ppt"))

print("--- exists: /not-exists.txt ---")
print(op.exists("/not-exists.txt"))
