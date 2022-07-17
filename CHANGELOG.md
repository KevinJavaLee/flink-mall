# 实时数据仓库

### 1.0.0(2022/07/09)

**Innitial** 

- feat(mall-logger):模拟接收日志数据，发送给kafka
- feat(flink-cdc):通过FlinkCdc 以Datastream和FlinkSQL形式获取

---



### **1.0.1(2022/07/14)**

 **Features**

- feat(realtime-data-warehouce): 增加了Kafka、Phoenix配置类、维度表Bean类

  (1)增加了Kafka消费、生产配置类
  (2)增加了Phoenix相关的驱动、Server配置类
  (3)维度配置表

---



### **1.0.2(2022/07/15)**

- feat(realtime-data-warehouce): 维度表Bean类

---



### **1.0.3(2022/07/16)**

- feat(realtime-data-warehouce): 增加了广播流处理函数、Phoenix类 Sink函数

  (1)广播流处理函数.
  (2)将Kafka中消费的数据，经过过滤处理后变成维度表保存在Phoenix中.
  (3)通过Kafka消费业务数据,然后通过FlinkCdc读取维度表的变化，将过滤的数据存储到Phoenix

- build(realtime-data-warehouce): hbase、日志文件配置
- test(realtime-data-warehouce): 测试Phoenix类
  测试Phoenix配置是否正确