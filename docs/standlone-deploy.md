# 单机模式部署
创建hdfs的nfs共享目录，属主为root，权限为755
创建各个用户的nfs私有目录
```shell
hdfs dfs -mkdir /user/root/nfs
hdfs dfs -chown root:root /user/root/nfs
hdfs dfs -chmod 755 /user/root/nfs
hdfs dfs -mkdir /user/root/nfs/user
hdfs dfs -chown root:root /user/root/nfs/user
hdfs dfs -chmod 755 /user/root/nfs/user
# /tmp
hdfs dfs -mkdir /user/root/nfs/tmp
hdfs dfs -chown root:root /user/root/nfs/tmp
hdfs dfs -chmod 777 /user/root/nfs/tmp
# /user/foo
hdfs dfs -mkdir /user/root/nfs/user/foo
hdfs dfs -chown foo:foo /user/root/nfs/user/foo
hdfs dfs -chmod 700 /user/root/nfs/user/foo
# /user/bar
hdfs dfs -mkdir /user/root/nfs/user/bar
hdfs dfs -chown bar:bar /user/root/nfs/user/bar
hdfs dfs -chmod 700 /user/root/nfs/user/bar
# /user/<user>
hdfs dfs -mkdir /user/root/nfs/user/<user>
hdfs dfs -chown <user>:<user> /user/root/nfs/user/<user>
hdfs dfs -chmod 700 /user/root/nfs/user/<user>
...
```
安装nfs-tuils
```shell
yum install -y nfs-utils
```
解压压缩包
```shell
tar -zvxf nfs-server-bin.tar.gz
cd nfs-server-1.0-SNAPSHOT
```

编辑nfs-server.yaml
* 修改rootDir为hdfs的nfs共享根目录；
* 修改kerberos配置，principal必须具备hdfs超级用户和模拟用户权限，例如hdfs；
* 配置hazelcast节点地址为本机。

```yaml
rootDir: "/user/root/nfs"
kerberos:
  dfsEnabled: true
  dfsPrincipal: "hdfs@HADOOP.COM"
  dfsKeytab: "/path/hdfs.keytab"
sftp:
  defaultUser: "user"
  defaultPassword: "password"
  addressInfos: [ ]
hazelcast:
  network:
    join:
      tcp-ip:
        enabled: true
        member-list:
          - "10.50.30.186"
```

编辑conf/id_mapping，将所有可能会访问nfs的系统用户都写到该文件下，后缀id不能重复，root必须是0，nfsnobody用于匿名访问
```csv
root,0
nobody,99
nginx,992
consul,994
nfsnobody,65534
hdfs,1001
admin,1002
deploy,1003
dataintel,1004
devmaster,1005
devops,1006
```

编辑conf/export，将所有之后可能会部署客户端的节点ip都按以下方式进行配置
```text
/ *(rw,insecure,no_root_squash,sec=sys)

```
编辑conf/env.sh，配置java17路径
```shell
CUSTOMED_JAVA_CMD='/usr/local/jdk-17/bin/java
--add-modules java.se
--add-exports java.base/jdk.internal.ref=ALL-UNNAMED
--add-opens java.base/java.lang=ALL-UNNAMED
--add-opens java.base/java.nio=ALL-UNNAMED
--add-opens java.base/sun.nio.ch=ALL-UNNAMED
--add-opens java.management/sun.management=ALL-UNNAMED
--add-opens jdk.management/com.sun.management.internal=ALL-UNNAMED'
```
启动服务，查看日志应当无报错
```shell
sh bin/service-daemon.sh start
```
按照上述的步骤，在其他的nfs-server端进行部署并启动

完成所有server端的部署和启动后，http访问，应当可以看到所有部署的nfs-server的ip地址
```text
http://<ip>:5701/hazelcast/rest/cluster
```
启动basic-client测试，无报错
```shell
sh bin/basic-client.sh <nfs-ip>:2049
>>> ls /
>>> cd user
```

# 客户端部署
安装nfs-tuils
```shell
yum install -y nfs-utils
```
在客户端节点创建nfs目录
```shell
sudo mkdir /<mount-dir>
```
编辑/etc/fstab，添加一行，其中
* server-ip表示nginx负载均衡节点ip；
* client-ip为客户端本地ip且必须在上文export配置文件中存在；
* mount-dir为刚才创建的nfs目录
```text
<server-ip>:/ <mount-dir> nfs4 soft,intr,timeo=60,retry=2,proto=tcp,lock,port=2049 0 0
```
挂载
```shell
mount -a
```
