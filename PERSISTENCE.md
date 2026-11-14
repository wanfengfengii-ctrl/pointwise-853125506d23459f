# 持久化边界说明

本工程把“读到了什么”和“发布了什么”拆成两层独立的 SQLite 提交，配合
SDK2 `fetch` 不消耗远端数据的特性，保证单工作者在任何边界被杀死后，重开
续采都不漏、不重、不半笔。

## 1. 升级边界（migration.upgrade，单次 SQLite 提交）

- 进入时先在事务外调用 `client.resume(checkpoint)` 得到映射游标
  （0→0、1→3、2→6、3→9）；resume 失败时连接上没有任何写入，旧库原样。
- `BEGIN IMMEDIATE` 之后在同一事务内完成：建 `documents` /
  `ingest_state` / `ingest_pending`、逐条复制 `legacy_items`
  （正文为原 payload 的 UTF-8 字节、墓碑保持 NULL）、写入映射后的
  `read_cursor`、`PRAGMA user_version=2`。
- `upgrade.before_commit` 在事务内触发；其后 `COMMIT`，提交成功后才触发
  `upgrade.after_commit`。已验证 `PRAGMA user_version` 参与事务：回滚会
  还原为 1。提交前任何异常（含 hook 抛出）或 SIGKILL，都由回滚/热日志
  恢复为完整可读的旧库；提交后（含 after_commit 抛错或被杀）是完整新版，
  不存在半迁移状态。
- `receipts` 表原地沿用，seq 顺序不中断；`legacy_items`、`legacy_state`
  仅作审计，升级后不再写入、不再被查询路径使用。
- 已升级（user_version=2）的库再次调用 upgrade 立即返回：不 resume、
  不建表、不改数据、不触发任何升级 hook。

## 2. 页面接纳边界（page.before_commit / page.after_commit）

- 每次 poll 至多 fetch 一页。整页事件逐行写入 `ingest_pending`，并在
  同一事务把 `ingest_state.read_cursor` 推进到 `next_cursor`、落入 `eof`。
- 页面与游标原子可见：提交前被杀，远端页未被消耗，重开后以旧 cursor
  重取同一页（这是唯一允许的重复 fetch）；提交后被杀，页面已接纳，
  重开后从 next_cursor 继续，已接纳页绝不重取。
- 暂存只是“已读未决”，upsert/delete 不会出现在 `documents` 中，因此
  读页不等于发布。

## 3. 事务发布边界（publish.before_commit / publish.after_commit）

- 每次 poll 先清扫上次遗留的已闭合事务，再接纳新页，再清扫本页新闭合的
  事务；终结事件按其 position 顺序处理，因此不同事务按 commit 位置而非
  begin 顺序选择胜者。
- 每个 commit 各自一个 SQLite 事务：同事务修改按事件顺序生效（同 ID
  最后一次写入胜出）、写一张回执（changes 为 upsert/delete 事件总数，
  零修改为 0）、删除该 tx 的全部暂存行，三者原子提交。未闭合的其他
  事务不阻塞该提交。
- abort 仅在独立事务中清空该 tx 暂存，不动 documents、不产生回执，
  也不触发额外 hook。
- 发布前被杀：documents/receipts 无该笔变更，暂存仍在，重开后重新发布
  且回执只生成一次；发布后被杀：资料、回执、暂存清理已同时落盘，重开
  不会补发 token（poll 返回的 published 只含本次新回执）。

## 4. 源结束边界

- `done` 仅在 eof 已持久接纳且 `ingest_pending` 无任何行时为 true。
- eof 时仍有未闭事务：保留暂存与已接纳游标，抛出含 `unfinished` 的
  `ValueError`；此前合法发布的事务不回滚，重复 poll 持续报错且不产生
  重复回执。
- 网络异常在 fetch 处原样传播，本地无写入，下一次 poll 用同一 cursor
  重取。

## 核验入口

- `python3 checks.py [demo]`：冻结的旧回归与录制演示，保持原断言。
- `python3 verify.py`：旧回归 + 四断点迁移、幂等/回滚、逐页发布、
  交错/跨页/abort/零修改、传输异常、真实文件库六个提交边界的 SIGKILL
  子进程重开续采（含 fetch 证据，证明已接纳页不重取）、SDK1 隔离。
- `python3 demo.py`：冻结旧库升级后的真实逐页资料、暂存与回执。
