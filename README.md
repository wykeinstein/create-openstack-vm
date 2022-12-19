# 使用说明
## 填写config.ini配置文件

```
[DEFAULT]
# openstack的认证文件
auth=/etc/kolla/admin-openrc.sh
# 虚拟机的模板文件，为xls格式。里面填写了虚拟机的网络、磁盘、内存、cpu等信息
xls=/root/work/虚拟机模板.xls
# 并发创建虚拟机的数量。3代表同一时刻，只有3台虚拟机在创建
pool_size=3
```

## 创建虚拟机

```
python create_vm.py --config-file config.ini
```