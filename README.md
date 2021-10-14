# yotta-analysis
## 1. 部署与配置：
在项目的main目录下编译：
```
$ go build -o analysis
```
配置文件为`yotta-analysis.yaml`（项目的main目录下有示例），默认可以放在`home`目录或抽查程序同目录下，各配置项说明如下：
```
#grpc绑定端口，默认为0.0.0.0:8080
bind-addr: ":8080"
#分析库的URL地址，默认为mongodb://127.0.0.1:27017/?connect=direct
analysisdb-url: "mongodb://127.0.0.1:27017/?connect=direct"
#同步库的URL地址，默认为127.0.0.1:2379
pd-urls: 
- "127.0.0.1:2379"
#ELK地址，默认为127.0.0.1:9200
es-urls:
- "http://127.0.0.1:9200"
#ELK连接账号
es-username: "test"
#ELK连接密码
es-password: "test"
#矿机跟踪服务地址，默认为主网服务地址https://dnrpc.yottachain.net
miner-tracker-url: "https://dnrpc.yottachain.net"
#矿机日志跟踪
miner-stat:
  #要连接的全部同步地址，默认值为空
  all-sync-urls:
  - "http://127.0.0.1:8091"
  - "http://127.0.0.1:8092"
  - "http://127.0.0.1:8093"
  - "http://127.0.0.1:8094"
  - "http://127.0.0.1:8095"
  #每次取多少条记录
  batch-size: 100
  #没有记录可取时的等待时间（秒）
  wait-time: 10
  #轮询到比当前时间提前多少秒时停止轮询
  skip-time: 180
#日志相关配置
logger:
  #日志输出类型：stdout为输出到标准输出流，file为输出到文件，默认为stdout，此时只有level属性起作用，其他属性会被忽略
  output: "file"
  #日志输出等级，默认为Info
  level: "Debug"
  #日志路径，默认值为./spotcheck.log，仅在output=file时有效
  file-path: "./spotcheck.log"
  #日志拆分间隔时间，默认为24（小时），仅在output=file时有效
  rotation-time: 24
  #日志最大保留时间，默认为240（10天），仅在output=file时有效
  max-age: 240
#其他抽查相关配置
misc:
  #抽查任务池大小，默认5000
  reckecking-pool-length: 5000
  #抽查任务等待队列长度，默认10000
  reckecking-queue-length: 10000
  #节点在多长时间内上报算作有效，默认3（分钟）
  avaliable-node-time-gap: 3
  #有效矿机版本号，大于该值则该矿机有效，默认0
  miner-version-threshold: 0
  #抽查分片时跳过该时间之前的分片，默认为0（格式为unix时间戳，单位为秒）
  spotcheck-start-time: 0
  #抽查分片时跳过该时间之后的分片，默认为0（格式为unix时间戳，单位为秒）
  spotcheck-end-time: 0
  #每台矿机两次抽查之间的时间间隔，默认60（分钟），间隔时间基于概率计算，有一定误差
  spotcheck-interval: 60
  #复核抽查任务时的连接超时，默认为10（秒）
  spotcheck-connect-timeout: 10
  #允许以该参数指定的值作为前缀的矿机地址为有效地址，默认为空，一般用于内网测试环境
  exclude-addr-prefix: "/ip4/172.17"
  #矿机跟踪失败时的等待时间，默认为5000（毫秒）
  retry-time-delay: 5000
  #每次从矿机跟踪程序获取的矿机信息条数，默认为2000
  miner-track-batch-size: 2000
  #两次从矿机跟踪服务获取矿机信息的时间间隔，默认为2（分钟）
  miner-track-interval: 2
  #抽查任务生成池的大小，默认为1000
  prepare-task-pool-size: 1000
  #计算c、d两个参数的时间间隔，默认为3（分钟）
  calculate-cd-interval: 3
```
启动服务：
```
$ nohup ./analysis &
```
## 2. 数据库配置：
analysis服务需要将各SN所属mongoDB数据库的分块分片数据同步至analysis库，需使用![yotta-sync-server](https://github.com/yottachain/yotta-sync-server)项目进行数据同步。该项目会将全部SN的metabase库中的blocks和shards集合同步至analysis服务所接入的tikv服务，除此之外还需要建立名称为`analysis`的分析库用于记录抽查过程中的数据，该库包含两个集合，分别为`SpotCheck`和`SpotCheckNode`，`SpotCheck`字段如下：
| 字段 | 类型 | 描述 |
| ---- | ---- | ---- |
| _id | string | 抽查任务ID，主键 |
| nid | int32 | 抽查任务所属矿机ID |
| vni |string |	被抽查分片 |
| status | int32 | 任务状态：0-任务已发出，1-任务复核中，2-校验完毕且有错误 |
| timestamp	| int64	| 发出任务时的时间戳 |

`SpotCheckNode`和yotta库的`Node`集合字段相同，只是多了一个`errorCount`字段（类型为`int32`）用以记录抽查连续出错次数

另外需要为`SpotCheck`集合添加索引：
```
mongoshell> db.SpotCheck.createIndex({nid: 1, timestamp: -1})
mongoshell> db.SpotCheck.createIndex({status: 1})
mongoshell> db.SpotCheck.createIndex({timestamp: -1})
```
analysis服务启动后，也会从矿机跟踪服务同步矿机信息至`analysis`库的`Node`集合，需要为`Node`集合建立索引：
```
mongoshell> db.Node.createIndex({status:1, poolOwner:1})
mongoshell> db.Node.createIndex({status:1, usedSpace:1, assignedSpace:1})
mongoshell> db.Node.createIndex({status:1, version:1, timestamp:1})
```