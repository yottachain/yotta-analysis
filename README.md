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
#同步库的URL地址，默认为mongodb://127.0.0.1:27017/?connect=direct
syncdb-url: "mongodb://127.0.0.1:27017/?connect=direct"
#消息队列相关配置
auramq:
  #本地订阅者接收队列长度，默认值1024
  subscriber-buffer-size: 1024
  #客户端ping服务端时间间隔，默认值30秒
  ping-wait: 30
  #客户端读超时时间，默认值60秒
  read-wait: 60
  #客户端写超时时间，默认值10秒
  write-wait: 10
  #矿机信息同步主题名称，默认值sync
  miner-sync-topic: "sync"
  #需要监听的全部SN消息队列端口地址列表
  all-sn-urls:
  - "ws://172.17.0.2:8787/ws"
  - "ws://172.17.0.3:8787/ws"
  - "ws://172.17.0.4:8787/ws"
  #鉴权用账号名，需要在BP事先建好，默认值为空
  account: "yottanalysis"
  #鉴权用私钥，为account在BP上的active私钥，默认值为空
  private-key: "5JU7Q3PBEV3ZBHKU5bbVibGxuPzYnwb5HXCGgTedtuhCsDc52j7"
  #MQ客户端ID，连接SN的MQ时使用，必须保证在MQ server端唯一，默认值为yottaanalysis
  client-id: "yottaanalysis"
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
  #第一阶段惩罚错误数，默认4（抽查连续错误4次执行第一阶段惩罚）
  punish-phase1: 4
  #第一阶段抵押金惩罚比例，默认1（1%）
  punish-phase1-percent: 1
  #第二阶段惩罚错误数，默认24（抽查连续错误24次执行第二阶段惩罚）
  punish-phase2: 24
  #第二阶段抵押金惩罚比例，默认10（10%）
  punish-phase2-percent: 10
  #第三阶段惩罚错误数，默认168（抽查连续错误168次执行第三阶段惩罚）
  punish-phase3: 168
  #第三阶段抵押金惩罚比例，默认50（50%）
  punish-phase3-percent: 50
  #抽查分片时跳过该时间之前的分片，默认为0（格式为unix时间戳，单位为秒）
  spotcheck-start-time: 0
  #抽查分片时跳过该时间之后的分片，默认为0（格式为unix时间戳，单位为秒）
  spotcheck-end-time: 0
  #每台矿机两次抽查之间的时间间隔，默认60（分钟），间隔时间基于概率计算，有一定误差
  spotcheck-interval: 60
  #复核抽查任务时的连接超时，默认为10（秒）
  spotcheck-connect-timeout: 10
  #矿机所属矿池的错误率大于该值时不进行惩罚，默认95（%）
  error-node-percent-threshold: 95
  #超过该时间没有上报的矿机被认为是失效矿机并作为参数用于计算矿池错误率，默认为4小时
  pool-error-miner-time-threshold: 14400
  #允许以该参数指定的值作为前缀的矿机地址为有效地址，默认为空，一般用于内网测试环境
  exclude-addr-prefix: "/ip4/172.17"
```
启动服务：
```
$ nohup ./analysis &
```
如果不想使用配置文件也可以通过命令行标志来设置参数，标志指定的值也可以覆盖掉配置文件中对应的属性：
```
$ ./analysis --bind-addr ":8080" --analysisdb-url "mongodb://127.0.0.1:27017/?connect=direct" --syncdb-url "mongodb://127.0.0.1:27017/?connect=direct" --auramq.subscriber-buffer-size "1024" --auramq.ping-wait "30" --auramq.read-wait "60" --auramq.write-wait "10" --auramq.miner-sync-topic "sync" --auramq.all-sn-urls "ws://172.17.0.2:8787/ws,ws://172.17.0.3:8787/ws,ws://172.17.0.4:8787/ws" --auramq.account "yottanalysis" --auramq.private-key "5JU7Q3PBEV3ZBHKU5bbVibGxuPzYnwb5HXCGgTedtuhCsDc52j7" --auramq.client-id "yottaanalysis" --logger.output "file" --logger.file-path "./spotcheck.log" --logger.rotation-time "24" --logger.max-age "240" --logger.level "Info" --misc.reckecking-pool-length "5000" --misc.reckecking-queue-length "10000" --misc.avaliable-node-time-gap "3" --misc.miner-version-threshold "0" --misc.punish-phase1 "4" --misc.punish-phase2 "24" --misc.punish-phase3 "168" --misc.punish-phase1-percent "1" --misc.punish-phase2-percent "10" --misc.punish-phase3-percent "50" --misc.spotcheck-start-time "0" --misc.spotcheck-end-time "0" --misc.spotcheck-interval "60" --misc.spotcheck-connect-timeout "10" --misc.error-node-percent-threshold "95" --misc.pool-error-miner-time-threshold "14400" --misc.exclude-addr-prefix "/ip4/172.17"
```
SN端目前测试版本只需要重新编译`YDTNMgmtJavaBinding`项目的`dev`分支并替换原有jar包即可

## 2. 数据库配置：
analysis服务需要将各SN所属mongoDB数据库的分块分片数据同步至analysis库，需使用![yotta-sync-server](https://github.com/yottachain/yotta-sync-server)项目进行数据同步。该项目会将全部SN的metabase库中的blocks和shards集合同步至analysis服务所接入mongoDB实例的metabase库，注意，在同步执行之前，需要为`shards`集合建立索引：
```
mongoshell> db.shards.createIndex({nodeId:1, _id:1})
```
除此之外还需要建立名称为`analysis`的分析库用于记录抽查过程中的数据，该库包含两个集合，分别为`SpotCheck`和`SpotCheckNode`，`SpotCheck`字段如下：
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
analysis服务启动后，也会从全部SN同步矿机信息至`analysis`库的`Node`集合，需要先将SN中全部矿机数据导入该集合：
在SN端：
```
$ mongoexport -h 127.0.0.1 --port 27017 -d yotta -c Node -o node.json
```
在分析服务器端：
```
$ mongoimport -h 127.0.0.1 --port 27017 -d analysis -c Node --file node.json
```

另外需要为`Node`集合建立索引：
```
mongoshell> db.Node.createIndex({status:1, poolOwner:1})
mongoshell> db.Node.createIndex({status:1, usedSpace:1, assignedSpace:1})
mongoshell> db.Node.createIndex({status:1, version:1, timestamp:1})
```

## 3. SN端修改：
SN端的`YTDNMgmtJavaBinding`库增加了连接analysis服务的相关参数，目前暂时通过环境变量设置：
* NODEMGMT_ANALYSISHOSTNAME: analysis服务的IP地址或域名，默认为127.0.0.1
* NODEMGMT_ANALYSISPORT: analysis服务监听的端口，默认为8080
* NODEMGMT_ANALYSISTIMEOUT: SN连接analysis服务的超时时间，默认为5000（5秒）
  
另外`YTDNMgmt`库增加了环境变量用以设定SN数据库中抵押空间和采购空间与BP上数据同步的时间间隔：
* NODEMGMT_BPSYNCINTERVAL默认值为10（分钟），当抽查程序惩罚扣抵押后，SN大概需要10分钟左右才会将新的抵押额同步到SN数据库。
  
以上四个环境变量均在SN端配置。

## 4. 抽查执行流程：
当服务全部启动后，矿机上报时其所属SN会有一定概率为其随机分配一台矿机的某一分片进行抽查，此抽查任务由该SN通过grpc请求向analysis服务获得，同时analysis服务在其analysis库的`SpotCheck`表中记录该任务
被分配任务的矿机向被抽查矿机请求指定的分片并校验，若校验失败则将该任务所属的任务ID和被抽查矿机ID回报至SN，SN则通过grpc将该消息反馈给analysis服务；若抽查校验通过则不进行回报。

analysis服务收到回报消息后首先检查回报的任务ID是否是被抽查矿机的最新一个任务，若不是则丢弃该回报消息，否则将该任务的状态置为1（`SpotCheck`集合的`status`字段设置为1），同时将该抽查任务加入复核队列。

复核时首先由analysis服务重新抽查该分片，若
* A. 复核时出现网络错误或其他未知错误则将该任务状态置为2，同时将`SpotCheckNode`表中所属矿机的`errorCount`字段增加1，此时如果`errorCount`的值达到`punish-phase1`、`punish-phase2`或`punish-phase3`指定的值则执行相应阶段的惩罚，当`punish-phase3`阶段完成后会将`SpotCheckNode`集合中对应矿机的`status`字段设置为2以表明该矿机需要重建。
* B. 若复核没有错误则将该任务的状态设置为0并结束复核。
* C. 若复核时发现分片校验失败，将该任务的状态设置为2，然后重新随机选取该矿机的100个分片进行校验，如果全部校验失败则直接进行`punish-phase3`阶段惩罚同时将该矿机标记为待重建状态（`SpotCheckNode`集合的`status`字段设置为2）；若部分失败则进行`punish-phase1`阶段惩罚。