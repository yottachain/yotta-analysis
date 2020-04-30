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
#由各SN同步过来的mongoDB连接URL数组，默认值为长度21的字符串数组，内容均为mongodb://127.0.0.1:27017/?connect=direct
mongodb-urls: 
- "mongodb://127.0.0.1:27017/?connect=direct"
- "mongodb://127.0.0.1:27017/?connect=direct"
- "mongodb://127.0.0.1:27017/?connect=direct"
- "mongodb://127.0.0.1:27017/?connect=direct"
- "mongodb://127.0.0.1:27017/?connect=direct"
#是否为数据库名添加索引编号，设置为true时mongodb-urls指定的数据库实例内的库名会依次加上索引值，比如yotta0、yotta1...，默认值为false，此配置一般用于docker多SN环境
dbname-indexed: false
#SN个数，默认值21
sn-count: 5
#EOS相关配置
eos:
  #EOS连接URL，默认值http://127.0.0.1:8888
  url: "http://127.0.0.1:8888"
  #BP账号，默认值为空，docker测试环境下为eosio
  bp-account: "eosio"
  #BP账号私钥，默认值为空，docker测试环境下为eosio的私钥
  bp-privatekey: "5KXAHdRJLJQksQpu3tqKKpZmEZiWvtB7SixV1JUp9vy7bXByvHp"
  #合约账号M，默认值为空，docker测试环境下为hddpool12345
  contract-ownerm: "hddpool12345"
  #合约账号D，默认值为空，docker测试环境下为hdddeposit12
  contract-ownerd: "hdddeposit12"
  #shadow账号，默认值为空，docker测试环境下为eosio
  shadow-account: "eosio"
#日志相关配置
logger:
  #日志路径，默认值为./spotcheck.log
  file-path: "./spotcheck.log"
  #日志拆分间隔时间，默认为24（小时）
  rotation-time: 24
  #日志最大保留时间，默认为240（10天）
  max-age: 240
  #日志输出等级，默认为Info
  level: "Debug"
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
  #抽查跳过该时间之前的分片，默认为0（格式为unix时间戳，单位为秒）
  spotcheck-skip-time: 0
  #每台矿机两次抽查之间的时间间隔，默认60（分钟），间隔时间基于概率计算，有一定误差
  spotcheck-interval: 60
  #复核抽查任务时的连接超时，默认为10（秒）
  spotcheck-connect-timeout: 10
  #矿机所属矿池的错误率大于该值时不进行惩罚，默认95（%）
  error-node-percent-threshold: 95
  #允许该参数指定的IP地址段作为矿机的有效地址，默认为空，一般用于内网测试环境
  exclude-addr-prefix: "/ip4/172.17"
```
启动服务：
```
$ nohup ./analysis &
```
SN端目前测试版本只需要重新编译`YDTNMgmtJavaBinding`项目的`dev`分支并替换原有jar包即可

## 2. 数据库配置：
analysis服务需要从各SN所属的mongoDB数据库获取数据，为减轻主库压力analysis最好连接各mongoDB的备节点，可将配置文件中的`mongodb-urls`属性设置为analysis服务要连接的各SN备节点数据库的URL，需要按SN编号依次配置，且个数必须与`sn-count`设置值相同，否则程序启动时会报错。当`dbname-indexed`属性为`false`时，analysis服务会使用原始数据库名获取数据，比如yotta库或metabase库，当`dbname-indexed`属性为true时，数据库名会加上所属SN的编号，比如yotta0、yotta1...，该配置是为了便于多SN docker环境下的测试，由于docker多SN测试环境将所有数据库存于同一mongoDB实例内，且用编号区分各SN所属数据库，此时就可以将`dbname-indexed`设置为true，同时将`mongodb-urls`全部设置为唯一的mongoDB URL即可。架构图如下：

![architecture.png](https://i.loli.net/2020/04/30/vFc6KSZ3YoQfuyJ.png)

除此之外还需要建立名称为`analysis`的分析库用于记录抽查过程中的数据，该库包含两个collection，分别为`SpotCheck`和`SpotCheckNode`，`SpotCheck`字段如下：
| 字段 | 类型 | 描述 |
| ---- | ---- | ---- |
| _id | string | 抽查任务ID，主键 |
| nid | int32 | 抽查任务所属矿机ID |
| vni |string |	被抽查分片 |
| status | int32 | 任务状态：0-任务已发出，1-任务复核中，2-校验完毕且有错误 |
| timestamp	| int64	| 发出任务时的时间戳 |
`SpotCheckNode`和yotta库的`Node`表字段相同，只是多了一个`errorCount`字段（类型为`int32`）用以记录抽查连续出错次数

另外需要为`SpotCheck`表添加索引：
```
mongoshell> db.SpotCheck.createIndex({nid: 1, timestamp: -1})
mongoshell> db.SpotCheck.createIndex({status: 1})
mongoshell> db.SpotCheck.createIndex({timestamp: -1})
```
除此之外，还需要为各SN所属yotta库中的`Shard`表建立索引：
```
mongoshell> db.Shards.createIndex({minerID:1, delete:1, _id:1})
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