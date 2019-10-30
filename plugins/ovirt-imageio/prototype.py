#!/usr/sbin/nbdkit python3

import atexit
import errno
import nbdkit
import ovirtsdk4
import ovirtsdk4.types as types
import pprint
import ssl
import time
from urllib.parse import urlparse
import uuid
from http.client import HTTPSConnection, HTTPConnection

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
    global transfer
    global disk_size
    config = pprint.pformat(conf)
    nbdkit.debug("Config complete:")
    nbdkit.debug(config)
    if not "url" in config:
        nbdkit.set_error(errno.EHOSTUNREACH)
        raise ValueError("No URL was specified through the 'url=' parameter!")
    if not "username" in config:
        nbdkit.set_error(errno.EPERM)
        raise ValueError("No username was specified through the 'username=' parameter!")
    if not "password" in config:
        nbdkit.set_error(errno.EPERM)
        raise ValueError("No password was specified through the 'password=' parameter!")
    if not "disk" in config:
        nbdkit.set_error(errno.ENXIO)
        raise ValueError("No disk ID was specified through the 'disk=' parameter!")
    try: # One transfer just to get the right size
        connection = ovirtsdk4.Connection(url=conf["url"], username=conf["username"], password=conf["password"], insecure=True)
        system_service = connection.system_service()
        transfers_service = system_service.image_transfers_service()
        new_image_transfer = types.ImageTransfer(disk=types.Disk(id=conf["disk"]),
                inactivity_timeout=30, direction=(types.ImageTransferDirection.DOWNLOAD))
        transfer = transfers_service.add(new_image_transfer)
        transfer_service = transfers_service.image_transfer_service(transfer.id)
        while transfer.phase == types.ImageTransferPhase.INITIALIZING:
            time.sleep(1)
            transfer = transfer_service.get()
        nbdkit.debug("Started a transfer, ticket: {}".format(transfer.signed_ticket))
    except ovirtsdk4.AuthError as error:
        nbdkit.debug("Unable to log in to {}! The error was: {}".format(conf["url"], error))
        nbdkit.set_error(errno.EACCES)
        raise error
    context = ssl.create_default_context()
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE
    url = urlparse(transfer.proxy_url)
    http = HTTPSConnection(url.hostname, url.port, context = context)
    headers = {}
    headers["Authorization"] = transfer.signed_ticket
    http.request("GET", url.path, headers=headers)
    response = http.getresponse()
    nbdkit.debug("Response code: {}".format(response.status))
    contents = response.read(512)
    nbdkit.debug("Response contents: {}".format(contents))
    disk_size = int(response.getheader("Content-Length", 0))
    nbdkit.debug("Config done: size is {}".format(disk_size))
    http.close()
    tid = transfer.id
    transfer_service = transfers_service.image_transfer_service(tid)
    transfer_service.cancel()
    transfer_service.finalize()
    nbdkit.debug("Finalized for size")

    disks_service = system_service.disks_service()
    disk = disks_service.list(search='id="{}"'.format(conf["disk"]))[0]
    while disk.status != types.DiskStatus.OK:
        nbdkit.debug("Waiting for disk OK...")
        time.sleep(1)
        disk = disks_service.list(search='id="{}"'.format(conf["disk"]))[0]
    connection.close()

    try: # Save this transfer for actual disk data
        connection = ovirtsdk4.Connection(url=conf["url"], username=conf["username"], password=conf["password"], insecure=True)
        system_service = connection.system_service()
        transfers_service = system_service.image_transfers_service()
        new_image_transfer = types.ImageTransfer(disk=types.Disk(id=conf["disk"]),
                inactivity_timeout=0, direction=(types.ImageTransferDirection.DOWNLOAD))
        old_transfer = transfers_service.image_transfer_service(tid)
        try:
            while old_transfer.get():
                nbdkit.debug("Waiting for old transfer service to go away...")
                time.sleep(1)
        except ovirtsdk4.NotFoundError as success:
            nbdkit.debug("Old transfer cleaned up.")
        transfer = transfers_service.add(new_image_transfer)
        transfer_service = transfers_service.image_transfer_service(transfer.id)
        while transfer.phase == types.ImageTransferPhase.INITIALIZING:
            time.sleep(1)
            transfer = transfer_service.get()
        nbdkit.debug("Started a transfer, ticket: {}".format(transfer.signed_ticket))
    except ovirtsdk4.AuthError as error:
        nbdkit.debug("Unable to log in to {}! The error was: {}".format(conf["url"], error))
        nbdkit.set_error(errno.EACCES)
        raise error

def open(readonly):
    handle = uuid.uuid4()
    nbdkit.debug("Open: ro={}, uuid={}".format(readonly, handle))
    if not handle in handles:
        handles[handle] = {}
    handles[handle]["readonly"] = readonly
    context = ssl.create_default_context()
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE
    url = urlparse(transfer.transfer_url)
    http = HTTPSConnection(url.hostname, url.port, context = context)
    handles[handle]["http"] = http
    handles[handle]["path"] = url.path
    return handle

def close(handle):
    nbdkit.debug("Close: {0}".format(handle))

def get_size(handle):
    nbdkit.debug("Get size!")
    size = disk_size

###    context = ssl.create_default_context()
###    context.check_hostname = False
###    context.verify_mode = ssl.CERT_NONE
###    url = urlparse(transfer.transfer_url)
###    http = HTTPSConnection(url.hostname, url.port, context = context)
###    headers = {}
###    headers["Authorization"] = transfer.signed_ticket
###    http.request("GET", url.path, headers=headers)
###    response = http.getresponse()
###    nbdkit.debug("Response code: {}".format(response.status))
###    contents = response.read(512)
###    #nbdkit.debug("Response contents: {}".format(contents))
###    size = int(response.getheader("Content-Length", 0))
###    http.close()

    ##handle = handles[handle]
    ##http = handle["http"]
    ##headers = {}
    ##headers["Authorization"] = transfer.signed_ticket
    ##http.request("GET", handle["path"], headers=headers)
    ##response = http.getresponse()
    ##nbdkit.debug("Response code: {}".format(response.status))
    ##contents = response.read(512)
    ##nbdkit.debug("Response contents: {}".format(contents))
    ##size = int(response.getheader("Content-Length", 0))

    #system_service = connection.system_service()
    #disks_service = system_service.disks_service()
    #disk = disks_service.list(search='id="{}"'.format(conf["disk"]))[0]
    #size = disk.provisioned_size

    nbdkit.debug("Size is: {}".format(size))
    return size

def can_write(handle):
    return handles[handle]["readonly"]

def pread(handle, count, offset):
    nbdkit.debug("Read!")
    handle = handles[handle]
    http = handle["http"]
    headers = {}
    headers["Range"] = "bytes={}-{}".format(offset, offset+count-1)
    headers["Authorization"] = transfer.signed_ticket
    http.request("GET", handle["path"], headers=headers)
    response = http.getresponse()
    nbdkit.debug("Response code: {}".format(response.status))
    contents = response.read()
    nbdkit.debug("Response contents: {}".format(contents))
    return bytearray(contents)

@atexit.register
def cleanup():
    print("Exit cleanup")
    if transfer:
        system_service = connection.system_service()
        transfers_service = system_service.image_transfers_service()
        transfer_service = transfers_service.image_transfer_service(transfer.id)
        transfer_service.finalize()
        print("Finalized")
    connection.close()
