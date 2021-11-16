"""
When data is uploaded or downloaded an arbitrary set of transformations
may be applied to the data in transit including encryption. This file
assembles pipelines to apply these transformations depending on configuration.
"""
from bversion.backup import crypto

#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++==
def preprocess_config(interface, conn, config: dict):
    """ apply transformations to configuration data which should only be done once,
    for example key derivation """

    return crypto.preprocess_config(interface, conn, config)

#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++==
def get_default_pipeline_format() -> dict:
    return {'version' : 1,
            'format'  : {}}

#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++==
def build_pipeline_streaming(interface, direction):
    """ Build a chunked (streaming) pipeline of transformers """

    pipeline = interface

    if direction == 'out':
        pipeline = crypto.streaming_encrypt(pipeline)

    elif direction == 'in':
        pipeline = crypto.streaming_decrypt(pipeline)

    else:
        raise ValueError('Unknown pipeline direction')

    return pipeline
