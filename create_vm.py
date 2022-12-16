from copy import deepcopy
import pandas as pd
import os
import time
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
    return conn.image.find_image(image_name)

#bdms = [{"boot_index": "0", "destination_type": "volume", "uuid": image.id, "source_type": "image", "volume_size": "10"},{"destination_type": "volume", "source_type": "blank", "volume_size": "5"},{"destination_type": "volume", "source_type": "blank", "volume_size": "5"}]
#networks = [{"uuid": network.id, "fixed_ip": "192.168.1.200"}, {"uuid": network.id, "fixed_ip": "192.168.1.201"}]

#server = conn.compute.create_server(name="openstack_sdk_test", image_id=image.id, flavor_id=flavor.id, block_device_mapping=bdms, networks=networks, config_driver=True)
#server = conn.compute.wait_for_server(server)
server = dict()
server["bdms"] = list()

def construct_nova_server_dict(row):
    server = dict()
    server["name"] = row["name"].strip()
    server["flavor"] = get_flavor(flavors, row["vcpus"], row["ram"])
    server["availability_zone"] = row["zone"].strip()
    server["image"] = get_image(row["image"])
    server["nics"] = list()
    server["vol_type"] = row["vol_type"].strip()
    server["bdms"] = list()
    """
    row["segmentation_id"]中的数值类型为整型，需要先转换为str类型
    row['net_type']目前只支持填写网络一个网络类型，即虚拟机的所有网卡都是同一种类型的
    """
    netids = [get_network(subnets=subnets, network=i, net_type=row["net_type"], segmentation_id=int(j)).id for i, j in zip(row["networks"].split(), str(row["segmentation_id"]).split())]
    server["nics"] = [{"uuid": i, "fixed_ip": j}
                      for i, j in zip(netids, row["ips"].split())]
    vol_num = int(len(row["vol_size"].split()) if isinstance(row["vol_size"], str) else 1)
    for i in range(vol_num):
        bdm = dict()
        if i == 0:
            bdm["boot_index"] = "0"
            bdm["destination_type"] = "volume"
            bdm["source_type"] = "image"
            bdm["uuid"] = (get_image(row["image"])).id
            bdm["destination_type"] = "volume"
            bdm["volume_size"] = row["vol_size"] if isinstance(row["vol_size"], int) else row["vol_size"].split()[i]
        else:
            bdm["destination_type"] = "volume"
            bdm['volume_type'] = row['vol_type'].strip()
            bdm["volume_size"] = row["vol_size"].split()[i]
            bdm["source_type"] = "blank"
        server["bdms"].append(deepcopy(bdm))
    return server

if __name__ == "__main__":
    # 将当前脚本执行目录设置为工作目录，并设置默认的配置文件
    cur_path = os.path.dirname(os.path.abspath(__file__))
    os.chdir(cur_path)
    default_conf_path = [os.path.join(cur_path, "config.ini")]

    CONF = cfg.CONF
    xls_path = cfg.StrOpt('xls', default=None, help='path to xls file')
    auth_file = cfg.StrOpt('auth', default=None, help='path to admin-openrc.sh auth file')
    CONF.register_opt(xls_path)
    CONF.register_opt(auth_file)
    CONF(default_config_files=default_conf_path)
    set_sys_env(CONF["auth"])
    conn = openstack.connect(region_name='RegionOne')
    flavors = [flavor for flavor in conn.compute.flavors()]
    subnets = [subnet for subnet in conn.network.subnets()]
    df = pd.read_excel(io=os.path.abspath(CONF["xls"]), header=2)
    for index, row in df.iterrows():
        server_dict = construct_nova_server_dict(row)
        conn.image.update_image_properties(image=server_dict["image"], cinder_img_volume_type=server_dict["vol_type"])
        while True:
            if server_dict["image"].properties.get("cinder_img_volume_type") == server_dict["vol_type"]:
                break
        print("creating %s" % server_dict['name'])
        server_obj = conn.compute.create_server(availability_zone=server_dict["availability_zone"], name=server_dict["name"], image_id=(server_dict["image"]).id, flavor_id=(server_dict["flavor"]).id, block_device_mapping_v2=server_dict["bdms"], networks=server_dict["nics"], config_drive=True)
        server_obj = conn.compute.wait_for_server(server_obj, status="active", wait=600)
        print("%s created successfully \n" % server_dict["name"])
    time.sleep(10)
    (out, err) = processutils.execute("nova", "list")
    print(out)
