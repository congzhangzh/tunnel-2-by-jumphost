# tunnel-2-by-jumphost

`tunnel-2-by-jumphost` 是一个用于通过跳板机连接内部 SSH 的 Python 工具，它适用于各种类似情况，如容器、Colab 等。

## 安装

您可以使用 pip 安装这个包：

```shell
pip install tunnel-2-by-jumphost
```

# 使用
## 服务器端
在跳板机上运行服务器端以接受外部连接并转发到目标内部 SSH。在跳板机上运行以下命令：

```shell
tunnel-2-by-jumphost-server s [public_2_external_port] [tunnel_port]
# tunnel-2-by-jumphost-server s 222 333
```

### [public_2_external_port] 
    用于接受外部连接。

### [tunnel_port] 
    用于接受跳转通道。

## 客户端
在您的本地机器上运行客户端以连接到跳板机并将数据传递到目标内部 SSH。在您的本地机器上运行以下命令：

```shell
tunnel-2-by-jumphost-client c [jump_host] [jump_host_tunnel_port] [relay_to_host] [relay_to_port]
#tunnel-2-by-jumphost-client c jump_host 333 localhost 22
```
### [jump_host]
    跳转服务器 IP 地址。
### [jump_host_tunnel_port]
    跳转服务器端口。
### [relay_to_host] 
    重定向到的主机。
### [relay_to_port] 
    重定向到的服务器。
# 示例
## 服务器端示例

在跳板机上运行服务器端：

```shell
tunnel-2-by-jumphost-server s [public_2_external_port] [tunnel_port]
# tunnel-2-by-jumphost-server s 222 333
```
这将在跳板机上启动服务器，它将在 192.168.1.100:8080 上接受外部连接，并将它们转发到 10.0.0.1:22。

## 客户端示例
在您的本地机器上运行客户端：

```shell
tunnel-2-by-jumphost-client c [jump_host] [jump_host_tunnel_port] [relay_to_host] [relay_to_port]
#tunnel-2-by-jumphost-client c jump_host 333 localhost 22
```

这将连接到跳板机 jump_host:333，然后将数据传递到内部 SSH 服务器 localhost:22

# 注意事项
确保可以从容器环境访问跳板机，请谨慎使用此工具，自行评估，使用此工具造成的任何问题，我没有责任。

# 许可证
本项目采用 MIT 许可证。有关详细信息，请参阅 LICENSE 文件。



