# 门户事务导出升级约定

## 现在可以运行的工程
使用 Python 标准库和 SQLite。执行 `python3 checks.py` 核对逐行采集、旧库构造等价性和事件录制；`python3 checks.py demo` 展示实际记录、回执及新版事件页。
`samples.seed(db, checkpoint=3)` 是完整旧库构造入口，checkpoint 可为 0、1、2、3。它不调用待修改采集器，包含完整建表 SQL 和历史数据。默认库已有 x、gone 墓碑、h1/h2/h3 回执和断点 3。
连接由调用者用默认事务模式的 `sqlite3.connect(path)` 创建；所有公开调用进入时没有活动事务，函数不得关闭调用者连接。同一连接不跨线程使用。
没有外部服务、在线依赖或安装步骤；vendor 中两个客户端都是完整源码依赖，仅重放确定的门户导出。

## 冻结的旧格式与接口
旧 Schema 的完整定义是 samples.OLD_SCHEMA；PRAGMA user_version=1。
legacy_state 的唯一行 (singleton=1, checkpoint) 是已接纳的 SDK1 行数。
legacy_items(id TEXT PRIMARY KEY, payload TEXT, deleted INTEGER) 保存当前记录；deleted=1 时 payload 为 SQL NULL，否则是完整对象的 JSON 文本。记录 ID 大小写敏感，不做规范化。原文不能在迁移中改写。
receipts(seq INTEGER PRIMARY KEY, token TEXT UNIQUE, changes INTEGER) 是按发布先后排列的回执；旧回调每条 changes=1。
SDK1 Client.fetch(checkpoint, callback) 对下一条已提交修改调用一次 callback，返回 checkpoint+1；结束返回 None 且不调用 callback。回调格式和全部旧输入在 samples.ROWS，副本传给调用者。
Collector(db, client=None, hook=None).poll() 的签名保留，每次返回 {"cursor":整数,"published":[本次新发布的回执token],"done":布尔}。
Store(db).read(id) 返回 {"id":原ID,"deleted":布尔,"data":对象或null}；不存在返回 None。墓碑与不存在不同，正文内的 null 也不同于墓碑。
Store(db).receipts() 返回按 seq 升序的 {"seq":整数,"token":字符串,"changes":整数} 列表。
升级前继续支持上述正常调用；checks.py 的已有断言必须保留并通过。

## SDK2 的真实调用差异
Client.resume(legacy_checkpoint) 返回对应事件位置；映射 0→0、1→3、2→6、3→9。位置是下一条未读取事件的从零开始下标，不是提交事务数量。
Client.fetch(cursor) 不接收 callback，返回 Page(events, next_cursor, eof)。每条事件带 position 和 tx；位置连续且稳定，next_cursor 是这一页后的下一事件位置。eof 表示此录制已无更多事件。
begin 只有 kind、tx；upsert 另有 id 和完整对象 body；delete 另有 id；commit 另有唯一 token；abort 没有 token。全部字段以 vendor/sdk2.py 和 samples.EVENTS 为准。
源保证事件合法：事务先 begin，修改保持事务内顺序，最后恰有一个 commit 或 abort，tx 在该导出中不复用，续采区域的提交 token 与旧回执不重复。不同事务允许交错。可用 Client(events=事件列表, page_size=正整数) 组合有限测试源，事件 position 由客户端填入。
传输异常可重试，fetch 不消耗远端数据；相同 cursor 重取相同页。Client.calls 和 resume_calls 是调用证据，不承担本地恢复。
结束但仍有未闭事务的材料用于检验不完整导出；其他非法事件校验不在本题范围。

## 迁移入口与目标存储
新增 migration.upgrade(db, client=None, hook=None)，默认 client 为 SDK2 Client，成功返回 None。
仅在维护窗口升级，限定一个工作者，无旧进程竞争写入。升级须在一个 SQLite 事务内完成：按 resume 映射旧断点，真实复制全部现存记录及墓碑，保留 receipts 所有字段和顺序，再切换 PRAGMA user_version=2；重复升级不改变业务数据、回执和采集进度。
目标公开表是 documents(id TEXT PRIMARY KEY, body BLOB, deleted INTEGER NOT NULL) 和 ingest_state(singleton INTEGER PRIMARY KEY, read_cursor INTEGER NOT NULL, eof INTEGER NOT NULL)。singleton 固定为 1，eof 用 0/1，升级初始为 0。
documents 中未删除正文是旧 payload 原文的 UTF-8 字节，deleted=1 时 body 为 SQL NULL。SDK2 新写 body 是完整对象的 UTF-8 JSON 字节，不要求保留其空白格式。
legacy_items、legacy_state 和旧原文留作审计，升级后不得修改，也不得作为查询或续采的数据源。receipts 沿用原表。未决事务的持久暂存表自行设计，不限定内部表名。
提交前异常或进程退出，重开仍是完整可读的旧库；提交后退出或 after_commit hook 抛异常则是完整新版，不能留下半迁移状态。
升级后默认 Collector 使用 SDK2；同一 Store 对象的旧 read/receipts 接口立即读取新版结果，不再调用 SDK1。SDK2 从映射位置续采，不重新请求之前的历史。

## 新版发布规则
每次 poll 先处理上次遗留的可发布内容，再至多 fetch 一页，并处理该页可发布内容。返回的 cursor 是持久 read_cursor；published 只描述本次实际新提交的回执，崩溃前已发布的回执不补发返回，但 receipts 中必须恰好一份。
成功收到的整页事件与 read_cursor、eof 一起持久接纳；不得只推进游标而丢失暂存事件，也不得把每条 upsert 当成已发布资料。
仅 commit 可以发布整笔事务；同事务的修改按事件顺序生效，同一 ID 多次修改取该事务最后一次结果。不同事务按 commit 的 position 顺序覆盖，不按 begin 顺序或页号选择胜者。
发布的文档修改、一个回执及该事务暂存清理必须在同一 SQLite 提交中完成；changes 是该事务 upsert/delete 事件总数，包括同 ID 重复修改。零修改 commit 也有 changes=0 的回执。abort 不改文档、不生成回执，并清理该事务暂存。
跨页未闭事务必须持久保存；一个事务未结束不能阻塞已经提交的其他事务。正常结束后不保留已提交或已撤销事务的事件载荷。
done 仅在 eof 已持久接纳且没有未处理事务时为 true；若 eof 时仍有未闭事务，保留其暂存和已接纳进度，抛出包含 unfinished 的 ValueError，不能回滚其他已合法发布的事务。网络异常原样传播，不破坏此前持久接纳与发布的结果。

默认样例升级后不改变任何旧查询和回执。后续逐页预期：
|read_cursor|x|y|z|累计新增回执|
|---|---|---|---|---|
|12|旧公告|不存在|不存在|无|
|15|B先提交|不存在|不存在|b|
|18|A定稿|不存在|不存在|b,a|
|21|A定稿|不存在|不存在|b,a|
|24|墓碑|新资料，value=null|不存在|b,a,d|
|26|墓碑|新资料，value=null|不存在|b,a,d|
b、a、d 的 seq 分别为 4、5、6，changes 分别为 1、2、2。gone 墓碑和旧回执始终保留。游标 24 时 E 未结束，done 为 false；26 时 done 为 true。继续 poll 不产生新回执。

## 中断边界与交付
hook 是同步 callable(phase)。新版必须在迁移、整页接纳、单事务发布的 SQLite 提交紧前和紧后分别调用：
upgrade.before_commit / upgrade.after_commit；
page.before_commit / page.after_commit；
publish.before_commit / publish.after_commit。
before_commit 仍在该事务内，after_commit 已持久提交；升级重试命中已升级状态不再触发升级提交 hook。abort 清理不要求额外 hook。
单工作者在上述任一边界被终止后，关闭并重开库、重建 Collector 续采，查询和回执须等同不中断执行。页面已经接纳后不得重取该页；页提交前退出可以重取。不得用跳过未发布事务换取不重复读取。
保留 samples.py、vendor/ 和 checks.py。补充业务对照及真实文件数据库的子进程终止重开核验，并说明持久化边界。迁移失败回滚和重复升级也须可重跑核验。
.pointwise-runtime.json 是交付运行约定：version=1，kind=cli，setup 为 argv 数组列表，verify 与 demo 分别是单个 argv 数组。现在两者调用旧回归与材料演示；交付时 verify 还应实际核对升级、交错事务和恢复，demo 应展示冻结旧库升级后的逐页记录及回执，不只打印成功标志。可以更换这两个入口并保留旧回归调用。
