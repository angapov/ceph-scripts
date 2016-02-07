#!/usr/bin/env python
import ConfigParser
from vsm import db
config = ConfigParser.ConfigParser()
config.read(['/etc/ceph/ceph.conf'])
sections = config.sections()
