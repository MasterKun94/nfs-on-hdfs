# 介绍

本项目是一个基于[nfs4j](https://github.com/dCache/nfs4j)开发的NFS服务器，使用HDFS作为存储后端。

* 支持单机和集群两种部署模式；
* 支持nfsv4协议。

# 编译
执行maven命令：
```shell
cd nfs-on-hdfs
mvn clean package -DskipTests
```
生成的安装包为：
`target/nfs-server-bin.tar.gz`
# 部署说明

* [单机模式部署](docs/standlone-deploy.md)
* [集群模式部署](docs/cluster-deploy.md)
