# TODO

## 当前优先级

- [ ] 把当前存储引擎逐步演进成 mini-LSM，而不是一步跳到完整版 LSM Tree。
- [ ] 在改存储结构的过程中，保持现有文本协议、WAL、恢复逻辑、tracing 日志和优雅停机语义不退化。
- [ ] 每次存储层重构前后都继续补测试，不靠手工验证兜底。

## 第一阶段：先把当前引擎整理好

- [x] 把 [`src/storage/storage_engine.rs`](/Users/fan/MyProjects/acorusdb/src/storage/storage_engine.rs) 里的内存结构从 `HashMap` 换成 `BTreeMap`。
- [x] 保持当前写路径不变，仍然是 `WAL -> memtable apply`。
- [x] 补测试，证明重启后和 flush 后的遍历顺序稳定。
- [x] 在引入磁盘有序表之前，先明确并写清楚 delete 的 tombstone 语义。

## 第二阶段：把当前单文件落盘结构演进成 SSTable V1

- [x] 把主路径代码、配置和文档里的 `snapshot` 命名逐步退场，统一成 `sstable`。
- [x] 把 [`src/storage/sstable.rs`](/Users/fan/MyProjects/acorusdb/src/storage/sstable.rs) 从“整张表序列化”继续演进成“更像 SSTable 的有序、不可变表文件”。
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

- [x] 用 SSTable merge compaction 替代现在“多 SSTable + flush 但不做 merge”的思路。
- [x] 第一版只做手动触发或阈值触发，不做后台线程调度。
- [x] 支持把新旧 SSTable merge 成一个新表。
- [x] 在 merge 结果里只保留每个 key 的最新版本。
- [x] 在安全条件下清理无效 tombstone。
- [x] 补测试覆盖：
  - [x] 多表里重复 key 的覆盖关系
  - [x] tombstone compact 后重启仍然生效
  - [x] compact 后只保留最新值

## 第六阶段：元数据与恢复

- [x] 增加 manifest 元数据文件，用来记录 SSTable 列表。
- [x] 启动恢复路径改成：
  - [x] 先加载 manifest / 表列表
  - [x] 再通过 WAL 回放恢复 memtable
- [x] 保证新 SSTable 创建和 WAL reset 过程具备 crash safety。
- [x] 补恢复相关测试：
  - [x] manifest 列表驱动的 SSTable 加载测试
  - [x] orphan SSTable 不参与恢复测试
  - [x] SSTable 损坏定位测试
  - [x] manifest 文件损坏时的恢复 / 报错测试

## 第七阶段：读路径优化

- [ ] 给 SSTable 增加 sparse index，降低点查时整表加载的成本。
- [ ] 把当前点查从“整表加载”逐步改成“基于 index 的定点读取”。
- [ ] 补测试覆盖 index 构建、seek 查找和读路径不回退。

## 第八阶段：磁盘格式加固

- [ ] 给 SSTable 增加 checksum，提升磁盘数据损坏检测能力。
- [ ] 如果要继续扩展磁盘格式，先明确 version bump 策略。
- [ ] 只有当磁盘格式稳定后，再考虑 block-based read。
- [ ] 补损坏场景测试，覆盖 checksum 不匹配、截断和非法块边界。

## 第九阶段：性能测量与按需优化

- [ ] 增加 benchmark，测点查性能、写入吞吐、flush/compact 开销和重启耗时。
- [ ] 只有当读放大真的开始明显时，再加 Bloom filter。
- [ ] 根据 benchmark 结果再决定是否要继续做更多读路径缓存或批量读取优化。

## 第十阶段：Compaction V2 与后台调度

- [ ] 评估是否要把 full merge compaction 继续演进成 leveled compaction 或 tiered compaction。
- [ ] 评估是否要做后台 compaction 线程，避免前台写请求直接承担全部 merge 成本。
- [ ] 如果引入后台 compaction，补并发、恢复和优雅停机相关测试。
