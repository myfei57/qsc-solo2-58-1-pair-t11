# 键值文档在线迁移：上线与回滚手册

新结构上线不再停机重写。磁盘上同一逻辑键可同时存在两种布局：

```
<data-dir>/<key>.json      # 旧结构（legacy 投影），所有旧版本都能读
<data-dir>/v2/<key>.json   # 新结构信封（canonical，写入权威）
<data-dir>/v2/_migration.json  # 迁移游标/checkpoint（旧进程 glob 看不到）
```

`RecordStore.glob("*.json")` 非递归，旧进程天然看不到 `v2/`，前缀扫描不被污染。

## 前提：写转换器（每个改过结构的键前缀一个）

```python
from portpower.store import Converter, ConverterRegistry, VersionedStore

class ProfileConverter(Converter):
    def upgrade(self, key, legacy): ...   # 旧 -> 新（读侧兼容）
    def downgrade(self, key, document): ...  # 新 -> 旧等价结论（回退投影）
```

等价性按「旧代码实际读取的字段」定义，不要求字节相等；新增字段可在
downgrade 时丢弃，但语义字段丢了必须抛错（写会被阻断，见下）。

## 分阶段发布

1. **Expand（部署新二进制，dual_write=True，默认）**
   - 读：优先 v2，缺失/漂移时读旧文件并 upgrade；可配置读时懒落盘。
   - 写：先落 `_pending` 信封 → 写旧投影 → 翻转 pending。崩溃任意一步
     下次读自动修复（摘要钉住，无歧义）。
   - 启动后台 runner：`python -m portpower.store.migrate --data-dir D run`，
     也可在进程内 `MigrationRunner(store).run()`（分批、可 stop）。
   - 若有新字段无法投影到旧结构，`put` 会抛 `ConversionError`——在阶段 1
     这必须当作部署阻断错误，而不是降级为不写投影。

2. **观察（决定何时能停兼容层）**
   - `... status`：`progress`、`cursor`、`legacy_files`、`failures`、
     `poison_keys`、`completed_at`。
   - `... metrics`：
     - `store.reads_v1 / reads_v2`，派生 `compat_read_ratio`
       = reads_v1/(v1+v2)；
     - `reads_legacy_drift_healed`：回退后旧进程写过的键，被新版本
       重新升级的次数；
     - `reads_pending_recovered`：崩溃中断的双写恢复次数；
     - `lazy_upgrades`、`writes_without_legacy`；
     - runner：`migration.migrated/skipped/healed/recovered/failed`。
   - 停兼容层门槛（建议持续一个完整业务窗口满足）：
     1. `status.progress == 1.0`，`failures`/`poison_keys` 为空，
        `verify` 全绿（终态 digest 校验）；
     2. `compat_read_ratio == 0`（没有读还落在旧文件上）；
     3. `reads_legacy_drift_healed == 0`（没有实例还在回退态写旧文件）。

3. **Contract 第一步：关双写（dual_write=False 构建）**
   - 写只落 v2 并删除对应旧投影，避免回退读到过期旧文件。
   - 这一版仍能读旧文件（兼容读仍在），可安全回到阶段 1 的构建；
     但**不能回退到从未带兼容层的最原始版本**（它读不懂 v2 信封，
     也看不到 v2/ 目录）。回退目标只能是阶段 1 的构建。

4. **Contract 第二步：删旧文件、下线兼容代码**
   - 观察一个发布周期后发布清理版本：删除 legacy 文件、移除 upgrade/
     downgrade 注册（或保留 upgrade 一个版本做保险）。
   - 之后再做下一次结构演进时，信封里的 `_v` 字段已是新的起点。

## 回退

- **阶段 1 内回退**：直接起旧进程。它读写旧投影；期间产生的旧文件在
  新版本恢复后通过 `_src_sha256` 摘要不匹配被识别为 drift，读时
  自动重新 upgrade，旧进程的修改不丢（测试：
  `test_new_write_dual_projects_for_rolled_back_old_process`）。
- **阶段 2（关双写）后回退**：只能回退到阶段 1 构建；先跑一次
  `run`/`verify` 把期间纯 v2 的键补投影——注意关双写期间产生的新键没有
  旧投影，这也是为什么关双写要在迁移完成、且确认无回退实例后再做。
- 回退等价性测试：对每种文档做 fuzz/样例行——`downgrade(upgrade(v1))`
  后用旧读取逻辑断言字段等价。

## 失败语义与幂等

- runner 游标排序扫描，checkpoint 在 `v2/_migration.json`；重启从游标
  续跑，之后再做一次全量 digest 校验扫尾（游标之后被旧进程改动的键
  也能抓到）。
- 已迁移键的旧文件摘要记录在信封里：摘要一致 = skip，**重跑绝不二次
  转换**；摘要不一致 = 旧进程改过，才重新 upgrade。
- 单键转换失败记入 `failures` 并限次重试（默认 5 次），超过进
  `poison_keys` 并告警，不阻塞其他键、不阻塞在线读写。
- 单进程线程安全（与现有 `RecordStore` 一致，进程内 RLock）。多进程
  写同一目录不在当前范围内（原实现同样不支持）。
