#!/usr/bin/env python
import boto
import boto.s3.connection
import subprocess
import json

r = subprocess.check_output("radosgw-admin user info -i testuser", shell=True)
j = json.loads(r)
host = "iclcompute2"

for key in j['keys']:
    if key['user']=="testuser":
       access_key = key["access_key"]
       secret_key = key["secret_key"]
swift_user = j["swift_keys"][0]["user"] 
swift_key  = j["swift_keys"][0]["secret_key"] 
print "Amazon S3 access key:\t%s" % access_key
print "Amazon S3 secret key:\t%s" % secret_key
conn = boto.connect_s3(
    aws_access_key_id = access_key,
    aws_secret_access_key = secret_key,
    host = host,
    is_secure=False,
    calling_format = boto.s3.connection.OrdinaryCallingFormat(),
)
bucket = conn.create_bucket('my-new-bucket')
for bucket in conn.get_all_buckets():
    print "Amazon S3 API:\t{name}\t{created}".format(
                name = bucket.name,
                created = bucket.creation_date,
)
r = subprocess.check_output("swift -A http://%s/auth/1.0 -U %s -K %s list" % (host,swift_user,swift_key), shell=True)
print "Swift API:\t%s" % r
