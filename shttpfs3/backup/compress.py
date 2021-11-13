import bz2
from rrbackup import pipeline

#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++==
# One-shot compression and decompression
#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++==
def compress(child, data, meta, config):
    pl_format = pipeline.parse_pipeline_format(meta['header'])
    if 'compress' in pl_format['format']:
        pl_format['format']['compress'] = {'A' : 'bz2'}
        data = bz2.compress(data)

    meta['header'] = pipeline.serialise_pipeline_format(pl_format)
    return child(data, meta, config)

def decompress(child, meta, config):
    data, meta2 = child(meta, config)

    pl_format = pipeline.parse_pipeline_format(meta2['header'])
    if 'compress' in pl_format['format']:
        data = bz2.decompress(data)

    return data, meta2

#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++==
# Streaming (chunked) compression and decompression
#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++==

# compression object, store chunk length as fixed size entity at
# start of stream, if compressed result less than chunk size
# grab another chunk from the provider. Merge n chunks, truncating
# the last one and adding remainder to next chunk, Store in header.
