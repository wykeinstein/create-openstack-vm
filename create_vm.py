from copy import deepcopy
import pandas as pd
import pdb
import os
import time
import sys
from oslo_config import cfg
from oslo_concurrency import processutils
import openstack
import  re

def set_sys_env(env_file):
    with open(env_file, 'r') as f:
        for line in f.readlines():
            ret = re.search(r'.*(OS_.*)=(.*)', line)
            if ret:
                os.environ[ret.group(1)] = ret.group(2)

def get_flavor(flavors=list(), vcpus=int(), ram=int()):
    for flavor in flavors:
        if vcpus == getattr(flavor, "vcpus") and ram*1024 == getattr(flavor, "ram") and getattr(flavor, "extra_specs") == dict():
            return flavor

def get_network(subnets=list(), network=str(),net_type=str(), segmentation_id=int()):
    for subnet in subnets:
        if network == getattr(subnet, "cidr"):
           neutron_net = conn.network.find_network(getattr(subnet, "network_id"))
           if net_type == getattr(neutron_net, "provider_network_type") and segmentation_id == getattr(neutron_net, "provider_segmentation_id"):
                return neutron_net

def get_image(image_name):
    #return conn.compute.find_image(image_name)
    return conn.image.find_image(image_name)



#def create_server(name=str(), image=str(), flavor=str(), nics=list(), bdms=list(), networks=list()):
#    image = conn.compute.find_image(image)
#    flavor = conn.compute.find_flavor(flavor)
#    server = conn.compute.create_server(name=name, image_id=image.id, flavor_id=flavor.id, nics=nics, block_device_mapping=bdms, networks=networks)
#    server = conn.compute.wait_for_server(server) 
#




#network = get_network(subnets=subnets, network="192.168.1.0/24", net_type="vxlan", segmentation_id=1)
#flavor = get_flavor(flavors=flavors, vcpus=1, ram=1024)
#bdms = [{"boot_index": "0", "destination_type": "volume", "uuid": image.id, "source_type": "image", "volume_size": "10"},{"destination_type": "volume", "source_type": "blank", "volume_size": "5"},{"destination_type": "volume", "source_type": "blank", "volume_size": "5"}]
#networks = [{"uuid": network.id, "fixed_ip": "192.168.1.200"}, {"uuid": network.id, "fixed_ip": "192.168.1.201"}]

#server = conn.compute.create_server(name="openstack_sdk_test", image_id=image.id, flavor_id=flavor.id, block_device_mapping=bdms, networks=networks, config_driver=True)
#server = conn.compute.wait_for_server(server)
server = dict()
server["bdms"] = list()

def construct_nova_server_dict(row, server=dict()):
    server["name"] = row["name"].strip()
    server["flavor"] = get_flavor(flavors, row["vcpus"], row["ram"])
    server["availability_zone"] = row["zone"].strip()
    server["image"] = get_image(row["image"])
    server["nics"] = list()
    server["vol_type"] = row["vol_type"].strip()
    netids = list()
    # row["segmentation_id"]中的数值类型为整型，需要先转换为str类型
    for net_addr, segmentation_id in zip(row["networks"].split(), str(row["segmentation_id"]).split()):
        net = get_network(subnets=subnets, network=net_addr, net_type=row["net_type"], segmentation_id=int(segmentation_id))
        netids.append(net.id)
    netids_ips = list(zip(netids, row["ips"].split()))
    netids_ips_dict = dict()
    for i in range(len(netids_ips)):
        netids_ips_dict["uuid"] = netids_ips[i][0]
        netids_ips_dict["fixed_ip"] = netids_ips[i][1]
        server["nics"].append(deepcopy(netids_ips_dict))
    server["bdms"] = list()
    vol_num = int()
    if isinstance(row["vol_size"], int):
        vol_num = 1
    if isinstance(row["vol_size"], str):
        vol_num = len(row["vol_size"].split())
    for i in range(vol_num):
        bdm = dict()
        if i == 0:
            bdm["boot_index"] = "0"
            bdm["destination_type"] = "volume"
            bdm["source_type"] = "image"
            bdm["uuid"] = (get_image(row["image"])).id
            if isinstance(row["vol_size"], int):
                bdm["volume_size"] = row["vol_size"]
        else:
            bdm["destination_type"] = "volume"
            if isinstance(row["vol_size"], int):
                bdm["volume_size"] = row["vol_size"]
            bdm["source_type"] = "blank"
        server["bdms"].append(deepcopy(bdm))
    return server

if __name__ == "__main__":
    CONF = cfg.CONF
    xls_path = cfg.StrOpt('xls', default=None, help='path to xls file')
    auth_file = cfg.StrOpt('auth', default=None, help='path to admin-openrc.sh auth file')
    CONF.register_opt(xls_path)
    CONF.register_opt(auth_file)
    CONF(default_config_files='config.ini')

    set_sys_env(CONF["auth"])

    conn = openstack.connect(region_name='RegionOne')
    flavors = [flavor for flavor in conn.compute.flavors()]
    subnets = [subnet for subnet in conn.network.subnets()]
    images = [image for image in conn.compute.images()]
    #image = conn.compute.find_image("cirros-0.3.4-x86_64-disk.raw")

    df = pd.read_excel(io=os.path.abspath(CONF["xls"]), header=2)

    for index, row in df.iterrows():
        server_dict = construct_nova_server_dict(row, server=dict())
        conn.image.update_image_properties(image=server_dict["image"], cinder_img_volume_type=server_dict["vol_type"])
        while True:
            if server_dict["image"].properties.get("cinder_img_volume_type") == server_dict["vol_type"]:
                break
        print("creating %s" % server_dict['name'])
        server_obj = conn.compute.create_server(availability_zone=server_dict["availability_zone"], name=server_dict["name"], image_id=(server_dict["image"]).id, flavor_id=(server_dict["flavor"]).id, block_device_mapping=server_dict["bdms"], networks=server_dict["nics"], config_drive=True)
        server_obj = conn.compute.wait_for_server(server_obj)
        print("%s created successfully \n" % server_dict["name"])
        #server = conn.compute.create_server(availability_zone=server["availability_zone"], name=server["name"], image_id=(server["image"]).id, flavor_id=(server["flavor"]).id, networks=server["nics"], config_drive=True, boot_from_volume=True, volume_size='200')

    time.sleep(10)
    (out, err) = processutils.execute("nova", "list")
    print(out)
