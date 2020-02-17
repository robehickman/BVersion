import hashlib, os
from httpfs3 import read_body

def tester(name):
    fle = open(name, 'rb')
    body_length = os.stat(name).st_size
    body_partial = fle.read(50)

    reader = read_body(fle.read, body_length, body_partial)
    return reader
    
print("Test read whole when the whole content is in the partial buffer")
reader = tester('test_file_1')
res = reader()
print(hashlib.sha256(res).hexdigest() == '4c49effa2da0ae9154082d695535f73f553d3e38e9fd241671459d955a58adb3')
print(reader() is None)

print("Test read whole when not in the parial buffer")
reader = tester('test_file_2')
res = reader()
print(hashlib.sha256(res).hexdigest() == '762e1e4113a10e29af07f232c888717c5607ab8449bcfe2d277fcd3974bd3198')
print(reader() is None)

print("Test read partial when fully in partial buffer")
reader = tester('test_file_1')
res = reader(40)
print(hashlib.sha256(res).hexdigest() == '344b857b65785247f2f6ec5e03e5454623af7a1f9060b5ae5ee76b325c65d251')

print("Test read partial when partly in partial buffer")
reader = tester('test_file_2')
res = reader(100)
print(hashlib.sha256(res).hexdigest() == '559321ebe66898776b5b3b46921c08108c29a168f20c10d836ea9b18a360eeb3')

print("Test read whole file in chunks")
reader = tester('test_file_2')

buffer = b''
while True:
    res = reader(100)
    if res is None: break
    buffer += res
print(hashlib.sha256(buffer).hexdigest() == '762e1e4113a10e29af07f232c888717c5607ab8449bcfe2d277fcd3974bd3198')

