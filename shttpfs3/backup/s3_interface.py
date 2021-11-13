import struct
import boto3
import rrbackup.pipeline as pipeline

def add_default_config(config):
    config["s3"] = { "access_key": "",
                     "secret_key": "",
                     "bucket":     "" }
    return config


#++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++==
def connect(config):
    """ Connect to S3 and ensure that versioning is enabled """

    access_key = config['s3']['access_key']; secret_key = config['s3']['secret_key']

    if 'endpoint' in config['s3']:
        client = boto3.client( 's3',
                            endpoint_url = config['s3']['endpoint'],
                            aws_access_key_id=access_key,
                            aws_secret_access_key=secret_key)
    else:
        client = boto3.client( 's3',
                            aws_access_key_id=access_key,
                            aws_secret_access_key=secret_key)

    bucket_versioning = client.get_bucket_versioning(Bucket=config['s3']['bucket'])
    if bucket_versioning['Status'] != 'Enabled':
        print('Bucket versioning must be enabled, attempting to enable, please restart application')
        client.put_bucket_versioning(Bucket=config['s3']['bucket'], VersioningConfiguration={'Status': 'Enabled' })
        raise SystemExit(0)
    return {'client' : client, 'bucket' : config['s3']['bucket']}

#++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++==
def wipe_all(conn):
    """ wipe everything on the remote for testing purposes """

    truncated = True
    key_marker = None
    while truncated:
        if key_marker is None:
            version_list = conn['client'].list_object_versions(Bucket=conn['bucket'])
        else:
            version_list = conn['client'].list_object_versions(Bucket=conn['bucket'],KeyMarker=key_marker)

        try:
            versions = version_list['Versions']
            objects = [{'VersionId':v['VersionId'],'Key': v['Key']} for v in versions]
            conn['client'].delete_objects(Bucket=conn['bucket'],Delete={'Objects':objects})
        except: pass

        try:
            delete_markers = version_list['DeleteMarkers']
            objects = [{'VersionId':d['VersionId'],'Key': d['Key']} for d in delete_markers]
            conn['client'].delete_objects(Bucket=conn['bucket'],Delete={'Objects':objects})
        except: pass

        truncated  = version_list['IsTruncated']
        #if 'NextKeyMarker' in version_list:
        #    Key_marker = version_list['NextKeyMarker']

#++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++==
def get_object(conn, key, error='object not found', version_id=None):
    """ Gets an object from s3 """

    def helper():
        try:
            if version_id is None: return conn['client'].get_object(Bucket=conn['bucket'], Key=key)
            else: return conn['client'].get_object(Bucket=conn['bucket'], Key=key, VersionId=version_id)
        except conn['client'].exceptions.NoSuchKey:
            raise ValueError(error)
    k = helper()

    return {'key'             : key,
            'version_id'      : k['VersionId'],
            'body'            : k['Body'],
            'content_length'  : k['ContentLength'],
            'content_type'    : k['ContentType'],
            'metadata'        : k['Metadata'],
            'last_modified'   : k['LastModified']}

#++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++==
def put_object(conn, key, contents, meta=None):
    """ Creates an object or object revision on s3 """

    if meta is None:
        k = conn['client'].put_object(Bucket=conn['bucket'], Key=key, Body=contents)
    else:
        k = conn['client'].put_object(Bucket=conn['bucket'], Key=key, Body=contents, Metadata=meta)
    return {'key': key, 'version_id' : k['VersionId']}


#++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++==
def delete_object(conn, key, version_id=None):
    """ Creates an object or object revision on s3 """
    if version_id is None: return conn['client'].delete_object(Bucket=conn['bucket'], Key=key)
    else:                  return conn['client'].delete_object(Bucket=conn['bucket'], Key=key, VersionId=version_id)

#++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++==
def list_versions(conn, fle = None):
    if fle is None: version_list = conn['client'].list_object_versions(Bucket=conn['bucket'])
    else: version_list = conn['client'].list_object_versions(Bucket=conn['bucket'], Prefix=fle)

    if version_list['IsTruncated']: raise Exception('truncated result')
    if 'Versions' not in version_list: return []

    version_list = version_list['Versions']

    version_list = sorted(version_list, key=lambda v: v['LastModified'])

    return version_list

#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++==
def write_file(conn, data, meta, config): # pylint: disable=unused-argument
    """ Pipeline format and other metadata is stored in the header to allow decryption should
    the manifest be lost. This is not used during normal operation. This is included as
    additional data during encryption to stop tampering. """

    header = struct.pack('!I', len(meta['header'])) + meta['header']
    res = put_object(conn, meta['path'], header + data, {})
    meta['version_id'] = res['version_id']
    return meta

def read_file(conn, meta, config): # pylint: disable=unused-argument
    # boto3 read obtains a chunk from the remote and caches it,
    # don't have to worry about repeated calls.
    version_id = meta['version_id'] if 'version_id' in meta else None

    res = get_object(conn, meta['path'], version_id = version_id)

    header_length = struct.unpack('!I', res['body'].read(4))[0]
    header = res['body'].read(header_length)
    meta['header'] = header
    meta['last_modified'] = res['last_modified']
    data = res['body'].read()
    return data, meta

#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++==
class streaming_upload:
    """ Streaming (chunked) object upload """
    def __init__(self):
        self.header     = None
        self.client     = None
        self.mpu        = None
        self.part_id    = None
        self.chunk_size = None
        self.uid        = None
        self.part_info  = None
        self.client     = None
        self.bucket     = None
        self.key        = None

    def pass_config(self, config, header): # pylint: disable=unused-argument
        self.header = header

    def begin(self, conn, key):
        self.client = conn['client']
        self.bucket = conn['bucket']
        self.key = key
        self.mpu = self.client.create_multipart_upload(Bucket=self.bucket, Key=self.key, StorageClass='STANDARD_IA')
        self.part_id = 1
        self.part_info = {'Parts': []}
        self.uid = self.mpu['UploadId']

    def next_chunk(self, chunk):
        if self.part_id == 1: chunk = struct.pack('!I', len(self.header)) + self.header + chunk

        part = self.client.upload_part(Bucket=self.bucket, Key=self.key,
            PartNumber=self.part_id, UploadId=self.uid, Body=chunk)
        self.part_info['Parts'].append({'PartNumber': self.part_id, 'ETag': part['ETag']})
        self.part_id += 1

    def abort(self):
        return self.client.abort_multipart_upload(
            Bucket=self.bucket,
            Key=self.key,
            UploadId=self.uid
        )

    def finish(self):
        return self.client.complete_multipart_upload(Bucket=self.bucket, Key=self.key,
            UploadId=self.uid, MultipartUpload=self.part_info)

#--------
def delete_failed_uploads(conn):
    uploads = conn['client'].list_multipart_uploads(Bucket=conn['bucket'])
    if uploads['IsTruncated']: raise Exception('Unhandled truncated result set')
    if 'Uploads' in uploads:
        print('Deleting failed multipart uploads')
        for u in uploads['Uploads']:
            print('Deleting failed upload: '+u['Key'])
            conn['client'].abort_multipart_upload(
                Bucket=conn['bucket'],
                Key=u['Key'],
                UploadId=u['UploadId'])
        print('------------------------')

#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++==
class streaming_download:
    """ Streaming (chunked) object download """

    def __init__(self):
        self.res        = None
        self.chunk_size = None

    def begin(self, conn, key, version_id):
        self.res = get_object(conn, key, version_id = version_id)
        header_length = struct.unpack('!I', self.res['body'].read(4))[0]
        header = self.res['body'].read(header_length)

        pl_format = pipeline.parse_pipeline_format(header)
        self.chunk_size = pl_format['chunk_size']
        return header, pl_format

    def next_chunk(self, add_bytes = 0):
        res = self.res['body'].read(self.chunk_size + add_bytes)
        return res if res != b'' else None
