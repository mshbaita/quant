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
