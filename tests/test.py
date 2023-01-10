from tqdm import tqdm
from copy import deepcopy
import pandas as pd
import pdb
import os
import time
import sys
from oslo_config import cfg
from oslo_concurrency import processutils
import re
import eventlet
import openstack
import json

def set_sys_env(env_file):
    with open(env_file, 'r') as f:
        for line in f.readlines():
            ret = re.search(r'.*(OS_.*)=(.*)', line)
            if ret:
                os.environ[ret.group(1)] = ret.group(2)

def construct_server_nic(neutron_net, row):
    return {"uuid": neutron_net.id, "fixed_ip": row['ip']}

def get_image(image_name):
    return conn.image.find_image(image_name)

def get_network(conn, name_or_id, subnet, subnets=list()):
    for sn in subnets:
        if subnet == getattr(sn, "cidr"):
            neutron_net = conn.network.find_network(getattr(sn, "network_id"))
            if getattr(neutron_net, "provider_network_type") == None:
                raise ValueError("The user does not have administrator privileges, please give the user administrator privileges")
            if getattr(neutron_net, "provider_segmentation_id") == None:
                raise ValueError("The user does not have administrator privileges, please give the user administrator privileges")
            if name_or_id == getattr(neutron_net, "name") or name_or_id == getattr(neutron_net, "id"):
                return neutron_net
            else:
                raise ValueError('Network %s with subnet %s not found' % (name_or_id, subnet))

def get_flavor(conn, flavors=list(), vcpus=int(), ram=int()):
    flavor_name = str(vcpus) + "C." + str(ram) + "G"
    f = None
    for flavor in flavors:
        if vcpus == getattr(flavor, "vcpus") and ram*1024 == getattr(flavor, "ram") and getattr(flavor, "extra_specs") == dict():
            f = flavor
            return f
    if f is None:
        return conn.create_flavor(flavor_name, row['ram']*1024, row['vcpus'], 0)

def construct_server_bdm(row):
    if 'image' in row.keys() and 'root_disk_size' in row.keys():
        bdm = {
            "boot_index": "0",
            "destination_type": "volume",
            "source_type": "image",
            "uuid": (get_image(row["image"])).id,
            "destination_type": "volume",
            "volume_type": row["vol_type"],
            "volume_size": row["root_disk_size"]
        }
    elif 'data_disk_size' in row.keys():
        bdm = {
            "destination_type": "volume",
            "volume_type": row["vol_type"].strip(),
            "volume_size": row["data_disk_size"],
            "source_type": "blank",
        }
    else:
        pass
    return bdm

def create_server(server_dict, conn):
    msg = ("creating %s" % server_dict['name'])
    pbar = tqdm(total=100, desc=msg)
    server_obj = conn.compute.create_server(availability_zone=server_dict["availability_zone"],
                                            name=server_dict["name"], image_id=(server_dict["image"]).id,
                                            flavor_id=(server_dict["flavor"]).id,
                                            block_device_mapping_v2=server_dict["bdms"], networks=server_dict["nics"],
                                            config_drive=True)
    server_dict['id'] = server_obj.id
    print("server status is \n")
    print(server_obj.status)
    pbar.update(50)
    conn.compute.wait_for_server(server_obj, wait=3600)
    pbar.update(50)
def server_is_created(row, vm_info_file):

    pass

if __name__ == "__main__":
    # 将当前脚本执行目录设置为工作目录，并设置默认的配置文件
    cur_path = os.path.dirname(os.path.abspath(__file__))
    os.chdir(cur_path)
    set_sys_env("/etc/kolla/admin-openrc.sh")
    conn = openstack.connect(region_name='RegionOne')
    flavors = [flavor for flavor in conn.compute.flavors()]
    subnets = [subnet for subnet in conn.network.subnets()]
    servers = ["cirros_test1", "cirros_test4"]
    for server in servers:
        svr = conn.compute.find_server(server)
        print(svr.status)

