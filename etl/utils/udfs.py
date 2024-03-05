import json

from pyspark.sql import functions as F, types as T

import hashlib


@F.udf(returnType=T.StringType())
def hash_values(*values):
    """
    Generate SHA-256 hash of the input values.

    :param values: Variable length argument list of values to be hashed.
    :return: SHA-256 hash of the concatenated string of all values.
    """
    # Concatenate all values into a single string
    concatenated_values = ''.join(str(value) for value in values)

    # Encode the concatenated string to a bytes object
    encoded_values = concatenated_values.encode()

    # Generate SHA-256 hash
    hash_object = hashlib.sha256(encoded_values)
    hash_hex = hash_object.hexdigest()

    return hash_hex
@F.udf(returnType=T.StringType())
def struct_get(root, path, default_value):
    fields = path.split(".")
    last_item = fields[-1]
    fields = fields[:-1]

    buffer = root
    for field in fields:
        if buffer is not None:
            try:
                if field in buffer.__fields__:
                    buffer = buffer[field]
                else:
                    buffer = None
            except AttributeError:  # In case buffer is not a Row but has no __fields__ attribute
                buffer = None

    if buffer is None:
        return default_value
    else:
        # Assuming the final field refers to a simple string value within the Row
        try:
            return buffer[last_item]
        except (KeyError, TypeError):
            return default_value

