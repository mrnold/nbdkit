#!/usr/sbin/nbdkit python3

import atexit
import errno
import nbdkit
import pprint
import ssl
import time
from urllib.parse import urlparse
import uuid
from http.client import HTTPSConnection, HTTPConnection

import openstack

conf = {}
handles = {}

def dump_plugin():
    nbdkit.debug("Dumping plugin")

def config(key, value):
    if key == "password":
        password = nbdkit.read_password(value)
        conf["password"] = password.decode("utf-8")
    else:
        conf[key] = value

def config_complete():
    global connection
    global disk_size
    global image
    config = pprint.pformat(conf)
    nbdkit.debug("Config complete:")
    nbdkit.debug(config)
    if not "auth_url" in config:
        nbdkit.set_error(errno.EHOSTUNREACH)
        raise ValueError("No URL was specified through the 'auth_url=' parameter!")
    if not "username" in config:
        nbdkit.set_error(errno.EPERM)
        raise ValueError("No username was specified through the 'username=' parameter!")
    if not "password" in config:
        nbdkit.set_error(errno.EPERM)
        raise ValueError("No password was specified through the 'password=' parameter!")
    if not "image" in config:
        nbdkit.set_error(errno.ENXIO)
        raise ValueError("No image ID was specified through the 'image=' parameter!")
    connection = openstack.connect(auth_url=conf['auth_url'], username=conf['username'], password=conf['password'], project_name=conf['project_name'], project_domain_name=conf['project_domain_name'], user_domain_name=conf['user_domain_name'], verify=False)
    image = connection.image.find_image(conf['image'])
    disk_size = image.size
    nbdkit.debug("Size is: {}".format(disk_size))


def start_new_download(handle):
    print("start_new_download starting")
    download = connection.image.download_image(image)
    print("start_new_download download_image done")

    context = ssl.create_default_context()
    #if not conf['verify']:
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE

    if 'download_conn' in handles[handle]:
        handles[handle]['download_conn'].close()

    url = urlparse(download.url)
    download_conn = HTTPSConnection(url.hostname, url.port, context = context)
    headers = {"X-Auth-Token": connection.identity.get_token()}
    handles[handle]['download_conn'] = download_conn
    handles[handle]['index'] = 0
    download_conn.request("GET", url.path, headers=headers)
    print("start_new_download download_image sent get")
    response = download_conn.getresponse()
    print("start_new_download response code: {}".format(response.status))
    handles[handle]['response'] = response


# First attempt: start a new download for every open
def open(readonly):
    handle = uuid.uuid4()
    nbdkit.debug("Open: ro={}, uuid={}".format(readonly, handle))
    if not handle in handles:
        handles[handle] = {}
    handles[handle]["readonly"] = readonly
    start_new_download(handle)
    return handle


def close(handle):
    nbdkit.debug("Close: {0}".format(handle))
    if 'download_conn' in handles[handle]:
        handles[handle]['download_conn'].close()

def get_size(handle):
    size = disk_size
    nbdkit.debug("Size is: {}".format(size))
    return size

def can_write(handle):
    return False

def pread(handle, count, offset):
    nbdkit.debug("Read request for {0} bytes!".format(count))
    if offset < handles[handle]['index']:
        nbdkit.debug("Offset < index! ({0} < {1}), start new download".format(offset, handles[handle]['index']))
        start_new_download(handle) # Should reset index to 0 and cancel existing download
    response = handles[handle]['response']
    if offset >= handles[handle]['index']:
        ignorecount = offset-handles[handle]['index']
        nbdkit.debug("Ignoring {0} bytes on this read".format(ignorecount))
        response.read(ignorecount)
        nbdkit.debug("Done ignoring bytes")
    nbdkit.debug("Reading in {0} bytes...".format(count))
    contents = bytearray(b'\x00'*count)
    bytesread = response.readinto(contents)
    nbdkit.debug("Response contents length: {}".format(bytesread))
    handles[handle]['index'] += bytesread
    return bytearray(contents)

@atexit.register
def cleanup():
    print("Exit cleanup")
    for k, v in handles.items():
        if 'download_conn' in v:
            v['download_conn'].close()
    connection.close()
