from tqdm import tqdm
import os
import re
import time
from copy import deepcopy
import pandas as pd
from oslo_concurrency import processutils
import openstack
from oslo_config import cfg
import eventlet

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
           if getattr(neutron_net, "provider_network_type") == None:
               raise ValueError("The user does not have administrator privileges, please give the user administrator privileges")
           if getattr(neutron_net, "provider_segmentation_id") == None:
               raise ValueError("The user does not have administrator privileges, please give the user administrator privileges")
           if net_type == getattr(neutron_net, "provider_network_type") and segmentation_id == getattr(neutron_net, "provider_segmentation_id"):
                return neutron_net

def get_image(image_name):
    return conn.image.find_image(image_name)

#bdms = [{"boot_index": "0", "destination_type": "volume", "uuid": image.id, "source_type": "image", "volume_size": "10"},{"destination_type": "volume", "source_type": "blank", "volume_size": "5"},{"destination_type": "volume", "source_type": "blank", "volume_size": "5"}]

def construct_nova_server_dict(index, row):
    server = dict()
    server["name"] = row["name"].strip()
    server["flavor"] = get_flavor(flavors, row["vcpus"], row["ram"])
    if server['flavor'] == None:
        flavor_name = str(row['vcpus']) + "C." + str(row['ram']) + "G"
        server['flavor'] = conn.create_flavor(flavor_name, row['ram']*1024, row['vcpus'], 0)
    server["availability_zone"] = row["zone"].strip()
    server["image"] = get_image(row["image"])
    if server['image'] == None:
        raise ValueError("Image does not exist, please create image. Error in row %d" % index)
    server["nics"] = list()
    server["vol_type"] = row["vol_type"].strip()
    server["bdms"] = list()
    """
    row["segmentation_id"]中的数值类型为整型，需要先转换为str类型
    row['net_type']目前只支持填写网络一个网络类型，即虚拟机的所有网卡都是同一种类型的
    """
    for i, j in zip(row["networks"].split(), str(row["segmentation_id"]).split()):
        network = get_network(subnets=subnets, network=i, net_type=row["net_type"], segmentation_id=int(j))
        if network is None:
            raise ValueError("Network %s does not exist, please create network. Error in row %d" % (i, index))
    netids = [get_network(subnets=subnets, network=i, net_type=row["net_type"], segmentation_id=int(j)).id for i, j in zip(row["networks"].split(), str(row["segmentation_id"]).split())]
    server["nics"] = [{"uuid": i, "fixed_ip": j} for i, j in zip(netids, row["ips"].split())]
    vol_num = int(len(row["vol_size"].split()) if isinstance(row["vol_size"], str) else 1)
    # 系统盘bdm
    bdm = {
        "boot_index": "0",
        "destination_type": "volume",
        "source_type": "image",
        "uuid": (get_image(row["image"])).id,
        "destination_type": "volume",
        "volume_type": row["vol_type"].strip(),
        "volume_size": row["vol_size"] if isinstance(row["vol_size"], int) else row["vol_size"].split()[0],
    }
    server["bdms"].append(deepcopy(bdm))
    # 数据盘bdm
    for i in range(1, vol_num):
        bdm = {
            "destination_type": "volume",
            "volume_type": row["vol_type"].strip(),
            "volume_size": row["vol_size"].split()[i],
            "source_type": "blank",
        }
        server["bdms"].append(deepcopy(bdm))
    return server

def create_server(server_dict, conn):
    msg = ("creating %s" % server_dict['name'])
    pbar = tqdm(total=100, desc=msg)
    server_obj = conn.compute.create_server(availability_zone=server_dict["availability_zone"],
                                            name=server_dict["name"], image_id=(server_dict["image"]).id,
                                            flavor_id=(server_dict["flavor"]).id,
                                            block_device_mapping_v2=server_dict["bdms"], networks=server_dict["nics"],
                                            config_drive=True)
    pbar.update(50)
    conn.compute.wait_for_server(server_obj, wait=3600)
    pbar.update(50)

def server_is_created(conn, server):
    server_obj = conn.compute.find_server(server.get("name"))
    if server_obj != None and server_obj.status == "ACTIVE":
        print("server %s is created and its status is ACTIVE" % server.get("name"))
        return True
    elif server_obj != None and server_obj.status == "ERROR":
        raise Exception("The status of %s is ERROR, please delete %s first" % (server['name'], server['name']))
    elif server_obj == None:
        return False
    else:
        pass

if __name__ == "__main__":
    # 将当前脚本执行目录设置为工作目录，并设置默认的配置文件
    cur_path = os.path.dirname(os.path.abspath(__file__))
    os.chdir(cur_path)
    default_conf_path = [os.path.join(cur_path, "config.ini")]

    CONF = cfg.CONF
    xls_path = cfg.StrOpt('xls', default=None, help='path to xls file')
    auth_file = cfg.StrOpt('auth', default=None, help='path to admin-openrc.sh auth file')
    green_pool_size = cfg.StrOpt('pool_size', default=None, help='size of greenpool')
    CONF.register_opt(xls_path)
    CONF.register_opt(auth_file)
    CONF.register_opt(green_pool_size)
    CONF(default_config_files=default_conf_path)
    set_sys_env(CONF["auth"])
    conn = openstack.connect(region_name='RegionOne')
    flavors = [flavor for flavor in conn.compute.flavors()]
    subnets = [subnet for subnet in conn.network.subnets()]
    df = pd.read_excel(io=os.path.abspath(CONF["xls"]), header=2)

    eventlet.monkey_patch(thread=False)
    pool = eventlet.GreenPool(CONF['pool_size'])

    # 提前创建flavor
    for index, row in df.iterrows():
        flavor_name = str(row['vcpus']) + "C." + str(row['ram']) + "G"
        flavor = get_flavor(flavors, row["vcpus"], row["ram"])
        if flavor == None:
            conn.create_flavor(flavor_name, row['ram']*1024, row['vcpus'], 0)
            time.sleep(2)

    # 遍历每一行来创建虚拟机
    for index, row in df.iterrows():
        server_dict = construct_nova_server_dict(index, row)
        if server_is_created(conn, server_dict) == False:
            conn.image.update_image_properties(image=server_dict["image"], cinder_img_volume_type=server_dict["vol_type"])
            while True:
                if server_dict["image"].properties.get("cinder_img_volume_type") == server_dict["vol_type"]:
                    break
            pool.spawn(create_server, server_dict, conn)

    pool.waitall()
    time.sleep(10)

    for index, row in df.iterrows():
        server_dict = construct_nova_server_dict(row)
        (out, err) = processutils.execute("nova", "list", "--name", server_dict['name'])
        print(out)
