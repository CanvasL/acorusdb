# TODO

## 当前优先级

- [ ] 把当前存储引擎逐步演进成 mini-LSM，而不是一步跳到完整版 LSM Tree。
- [ ] 在改存储结构的过程中，保持现有文本协议、WAL、恢复逻辑、tracing 日志和优雅停机语义不退化。
- [ ] 每次存储层重构前后都继续补测试，不靠手工验证兜底。

## 第一阶段：先把当前引擎整理好

- [x] 把 [`src/storage_engine.rs`](/Users/fan/MyProjects/acorusdb/src/storage_engine.rs) 里的内存结构从 `HashMap` 换成 `BTreeMap`。
- [x] 保持当前写路径不变，仍然是 `WAL -> memtable apply`。
- [x] 补测试，证明重启后和 flush 后的遍历顺序稳定。
- [x] 在引入磁盘有序表之前，先明确并写清楚 delete 的 tombstone 语义。

## 第二阶段：把当前单文件落盘结构演进成 SSTable V1

- [x] 把主路径代码、配置和文档里的 `snapshot` 命名逐步退场，统一成 `sstable`。
- [x] 把 [`src/sstable.rs`](/Users/fan/MyProjects/acorusdb/src/sstable.rs) 从“整张表序列化”继续演进成“更像 SSTable 的有序、不可变表文件”。
- [x] 先定义一个足够简单的 SSTable 文件格式：
  - [x] 文件头
  - [x] 按 key 排序的记录
  - [x] delete 对应的 tombstone 标记
- [x] 第一版先保证顺序可读和正确性，不着急做索引。
- [x] 补测试覆盖：
  - [x] 写出有序表
  - [x] 读取有序表
  - [x] 正确识别 tombstone
  - [x] 损坏 header / value tag / trailer 的报错定位
  - [x] WAL 损坏字段定位和 trailing fields 场景

## 第三阶段：引入 memtable flush

- [x] 在概念和代码层都把当前单文件 SSTable 更明确地转成 flush 产物。
- [x] 当 memtable 达到阈值时触发 flush。
- [x] flush 流程至少包含：
  - [x] 写出新的不可变 SSTable
  - [x] 同步文件和目录
  - [x] reset WAL
  - [x] 保证宕机恢复路径仍然正确
- [x] 补测试覆盖 `set/delete -> flush -> restart -> recover`。

## 第四阶段：支持多张表的读路径

- [x] `get` 查询顺序改成：
  - [x] 先查 memtable
  - [x] 再查最新 SSTable
  - [x] 再查更老的 SSTable
- [x] 维护表元数据，保证启动时知道有哪些 SSTable。
- [x] 第一版查找策略可以先简单，先不要过度优化。
- [x] 补多次 flush 后重启恢复的测试。

## 第五阶段：Compaction V1

- [ ] 用 SSTable merge compaction 替代现在“多 SSTable + flush 但不做 merge”的思路。
- [ ] 第一版只做手动触发或阈值触发，不做后台线程调度。
- [ ] 支持把新旧 SSTable merge 成一个新表。
- [ ] 在安全条件下丢弃旧值和无效 tombstone。
- [ ] 补测试覆盖：
  - [ ] 多表里重复 key 的覆盖关系
  - [ ] tombstone 重启后仍然生效
  - [ ] compact 后只保留最新值

## 第六阶段：元数据与恢复

- [ ] 增加 manifest 或等价元数据文件，用来记录 SSTable 列表。
- [ ] 启动恢复路径改成：
  - [ ] 先加载 manifest / 表列表
  - [ ] 再通过 WAL 回放恢复 memtable
- [ ] 保证新 SSTable 创建和旧文件替换过程具备 crash safety。
- [ ] 补 manifest 和 SSTable 损坏场景的恢复测试。

## 第七阶段：可选性能工作

- [ ] 给 SSTable 增加 sparse index。
- [ ] 只有当读放大真的开始明显时，再加 Bloom filter。
- [ ] 只有当磁盘格式稳定后，再考虑 block-based read。
- [ ] 增加 benchmark，测写入吞吐、重启耗时和点查性能。

## 当前不打算做的事

- [ ] 先不要做 leveled compaction。
- [ ] 先不要做后台 compaction 线程。
- [ ] 先不要为了 LSM 工作去实现 Redis 兼容 RESP。
- [ ] 先不要在基础 SSTable 正确性完成前就加 Bloom filter。

## 建议的下一步

- [ ] 开始第六阶段：先补 manifest，把当前多 SSTable 集合管理起来。
