import json, re

serialise_mapper = {'encrypt'    : 'E'}

#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++==
def serialise_pipeline_format(pl_format: dict) -> bytes:
    """ For a given version the output of this MUST NOT CHANGE as it
    is used as additional data for validation"""
    if not isinstance(pl_format, dict): raise TypeError('pipeline format must be a dict')
    if not isinstance(pl_format['version'], int):
        raise TypeError('Version must be an integer')

    to_json = {'V' : str(pl_format['version'])}

    if'chunk_size' in  pl_format:
        to_json['S'] = str(pl_format['chunk_size'])

    for i in pl_format['format']:
        if not isinstance(i, str):
            raise TypeError('Format specifiers must be strings')

        if isinstance(pl_format['format'][i], dict):  to_json[serialise_mapper[i]] = pl_format['format'][i]
        elif pl_format['format'][i] is None: to_json[serialise_mapper[i]] = ''
        else: raise TypeError('Unexpected type')

    return json.dumps(to_json, separators=(',',':')).encode('utf-8')

#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++==
def parse_pipeline_format(serialised_pl_format: bytes) -> dict:
    raw = json.loads(serialised_pl_format)

    if 'V' not in raw: raise ValueError('Version not found')
    version = raw.pop('V')
    if not re.compile(r'^[0-9]+$').match(version): raise ValueError('Invalid version number')
    pl_format = {'version' : int(version), 'format'  : {}}

    try: pl_format['chunk_size'] = int(raw.pop('S'))
    except: pass

    inv_map = {v: k for k, v in serialise_mapper.items()}
    for k, v in raw.items():
        if v == '': pl_format['format'][inv_map[k]] = None
        elif isinstance(v, dict): pl_format['format'][inv_map[k]] = v
        else: raise ValueError('Unknown type in serialised pipeline format')

    return pl_format
