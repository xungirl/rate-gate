# rate-gate 面试讲解手册

这份文档是给你自己准备面试用的。先按「教学顺序」把代码一个文件一个文件走一遍，每部分分三层讲：

- **(a) 骨架**：这个文件干什么、核心流程是什么
- **(b) 为什么这样设计**：面试官最爱追问的部分
- **(c) Go 语法要点**：你从 Python/Java 过来，需要能讲清楚的 Go 写法

力气主要花在 (a)(b)，(c) 看懂、能解释即可。最后是 20 道左右高频面试题和基于这份代码的回答。

所有性能数字都在 `BENCHMARK.md`，面试时说数字一定要能对上那份文档。

---

## 0. 一分钟电梯陈述（先背熟）

> 我用 Go 写了一个分布式限流 API 网关。它是一个反向代理，放在后端服务前面，按 API key、用户 ID 或 IP 识别调用方，支持四种限流算法：令牌桶、固定窗口、滑动窗口日志、滑动窗口计数器，都实现同一个 `Limiter` 接口。状态可以放在进程内存（64 分片的 map）或 Redis 里。Redis 版本每个算法是一段原子执行的 Lua 脚本，并且用 Redis 服务器时间，所以多个网关实例共享同一个限额，也不受各机器时钟偏差影响。Redis 挂了有熔断器，以及可配置的 fail-open / fail-closed / 本地降级三种策略。有 Prometheus 指标、结构化日志、优雅关闭、热加载配置。测试覆盖了边界突发、并发不超限、多实例共享 Redis、故障模式，`go test -race` 重复 10 次全过。我还自己写了压测工具，测了网关相对直连的额外开销和多实例的限流精度。

---

## 1. 整体地图：一个请求经过了什么

```
客户端 → gateway.ServeHTTP
          1. 按路径最长前缀匹配路由 (routes.go: match)
          2. 提取 key：API key / user id / IP (keys.go)
          3. limiter.Allow(ctx, key)  ← 这里是核心
               - memory: 分片 map + 锁
               - redis : Resilient{ 熔断器 → RedisLimiter(Lua) → 失败策略 }
          4. 写 X-RateLimit-* 头
          5. 拒绝 → 429 + Retry-After；允许 → ReverseProxy 转发给上游
```

包的依赖方向是单向的：`clock` ← `limiter` ← `gateway` ← `cmd/gateway`。`limiter` 完全不知道 HTTP 的存在，所以可以单独测试；`gateway` 只认识 `Limiter` 接口，不知道背后是内存还是 Redis。

---

## 2. `internal/clock/clock.go` — 可控的时钟

**(a) 骨架**：定义 `Clock` 接口，只有一个方法 `Now()`。`Real` 返回真实时间，`Fake` 是测试用的假时钟，可以 `Advance(100ms)` 手动拨快。

**(b) 为什么**：限流算法全是和时间打交道的。如果代码里直接调 `time.Now()`，测「100ms 后补充一个令牌」就只能真的 `time.Sleep(100ms)`，测试又慢又不稳定（CI 机器忙时 sleep 不准）。把时间抽象成接口、依赖注入进去，测试里就能精确到纳秒地控制时间。这就是「依赖注入让代码可测试」的典型例子。

**(c) Go 语法**：
- `type Clock interface { Now() time.Time }` —— Go 的接口是**隐式实现**的：只要某个类型有 `Now() time.Time` 方法，它就自动是 `Clock`，不需要像 Java 那样写 `implements`。
- `type Real struct{}` 空结构体，不占内存，只为挂方法。
- `func (f *Fake) Now()` —— `(f *Fake)` 是**方法接收者**，相当于 Java 的 `this`。用指针接收者是因为要修改 `f.now`，并且结构体里有 `sync.Mutex`（锁不能被复制）。
- `defer f.mu.Unlock()` —— `defer` 在函数返回时执行，类似 Java 的 `finally`，保证一定解锁。

---

## 3. `internal/limiter/limiter.go` — 核心接口

**(a) 骨架**：
- `Algorithm`：算法名（字符串常量）。
- `Rule`：`Limit` 个请求 / `Window` 时间，`Burst` 是令牌桶容量。
- `Decision`：一次判断的结果，`Allowed`、`Remaining`、`ResetAfter`、`RetryAfter`、`Degraded`——正好对应要写的 HTTP 头。
- `Limiter` 接口：`Allow(ctx, key) (Decision, error)`。

**(b) 为什么**：
- 接口只有一个方法，越小越好实现、越好替换（Go 的习惯是「接口要小」）。四种算法 × 两种后端 = 8 个实现，再加一个装饰器 `Resilient`，全都满足这一个接口。
- 返回 `error`：内存实现永远不会出错，但 Redis 会（网络断了、超时），所以接口必须允许报错。
- 传 `context.Context`：Redis 调用需要超时控制和取消（客户端断开了就别再等 Redis）。
- `Decision` 里带够了写响应头的所有信息，网关就不需要知道算法细节。

**(c) Go 语法**：
- `type Algorithm string` —— 基于 string 定义新类型，比裸字符串更安全（函数签名写 `Algorithm`，传错类型编译器会提醒）。
- `const ( TokenBucket Algorithm = "token_bucket" ... )` —— 常量组，相当于 Java 的 enum 的简化版。
- 多返回值 `(Decision, error)` 是 Go 处理错误的标准方式：没有异常，错误就是一个普通的返回值，调用方必须 `if err != nil` 检查。
- `fmt.Errorf("%w: %q", ErrUnknownAlgorithm, s)` —— `%w` 包装错误，调用方可以用 `errors.Is(err, ErrUnknownAlgorithm)` 判断是不是这类错误（类似 Java 里 catch 特定异常类型）。

---

## 4. `internal/limiter/store.go` — 分片 map

**(a) 骨架**：一个并发安全的 `key → 状态` 的 map。内部有 64 个分片，每个分片 = 一把 `sync.Mutex` + 一个 `map`。key 用 `maphash` 算哈希决定去哪个分片。核心方法 `update(key, now, fn)`：拿到该分片的锁，取出（或新建）这个 key 的状态，调用 `fn` 修改它，`fn` 返回这个状态还要保留多久（TTL）。另外有一个后台 goroutine 定期 `sweep`，删除过期的 key。

**(b) 为什么**：
- **为什么分片**：如果整个 map 只用一把锁，所有请求（哪怕是不同用户）都要排队抢同一把锁，8 核也只能当 1 核用。分成 64 片后，不同用户大概率落在不同分片，可以并行。这和 Java 早期 `ConcurrentHashMap` 的分段锁（Segment）是同一个思路。
- **为什么不用 `sync.Map`**：`sync.Map` 适合「读多写少」或「key 集合基本不变」，限流每次请求都要写，而且需要「读-改-写」作为一个整体原子执行，`sync.Map` 做不到这种复合操作。
- **为什么用回调 `fn`**：把「读-改-写」整个放在锁里执行，调用方不可能忘记加锁或者在锁外修改状态。
- **为什么要过期清理**：不清理的话，每个出现过的 IP 都永远留在内存里，被扫描攻击（大量不同 IP）就会内存泄漏。TTL 设成「状态等价于全新 key 的时间」，比如令牌桶加满所需时间——过了这个时间，删掉和保留效果完全一样。
- **清理时一次只锁一个分片**，不会让所有请求同时停顿。

**(c) Go 语法**：
- `shardedStore[T any]` —— **泛型**（Go 1.18+）。`T` 是类型参数，令牌桶存 `tokenBucketState`，固定窗口存 `fixedWindowState`，共用同一份代码。相当于 Java 的 `ShardedStore<T>`。
- `shards [shardCount]shard[T]` —— 固定长度数组（不是切片），64 个分片在一块连续内存里。
- `go s.cleanupLoop(every)` —— `go` 关键字启动一个 goroutine（轻量级线程，几 KB 栈，可以开几十万个）。
- `select { case <-ticker.C: ... case <-s.stop: return }` —— `select` 同时等待多个 channel，哪个先来处理哪个。这里是「每隔一段时间清理一次，收到停止信号就退出」的标准写法。
- `close(s.stop)` 关闭 channel 会让所有在等它的 goroutine 都收到信号；`sync.Once` 保证只关一次（重复 close 会 panic）。
- `func(state *T) time.Duration` —— 函数是一等公民，可以作为参数传递（类似 Java 的 lambda）。

---

## 5. `internal/limiter/memory.go` — 四种算法（内存版）

**(a) 骨架**：`NewMemory(alg, rule, opts)` 根据算法名返回对应实现。每个算法都是同一个模式：取当前时间 → `store.update(key, ...)` 在锁里更新状态 → 填 `Decision`。

### 令牌桶 `memTokenBucket`
- 状态：`tokens`（浮点数）、`last`（上次补充时间）。
- 每次请求：先按经过的时间补充令牌 `tokens = min(容量, tokens + 经过时间 × 速率)`，够 1 个就扣掉放行，不够就拒绝，`RetryAfter = (1 - tokens) / 速率`。
- **关键点**：不需要后台定时器去「加令牌」，而是在请求到来时**惰性计算**（lazy refill）。百万个 key 也不需要百万个定时器。

### 固定窗口 `memFixedWindow`
- 状态：当前窗口编号 `window = 当前纳秒 / 窗口长度`，和计数 `count`。
- 窗口编号变了就清零。`count < limit` 就放行。
- 缺陷：窗口末尾和下一个窗口开头各发满 limit 个，短时间内放过 2 倍。

### 滑动窗口日志 `memSlidingLog`
- 状态：所有被允许请求的时间戳列表。
- 每次请求先删掉 `now - window` 之前的时间戳，剩下的数量 `< limit` 就放行并记录。
- 精确，但每个 key 内存是 O(limit)。

### 滑动窗口计数器 `memSlidingCounter`
- 状态：当前窗口计数 `curr`、上个窗口计数 `prev`。
- 估算：`estimate = prev × (上个窗口还和滑动窗口重叠的比例) + curr`。比如当前窗口过了 30%，上个窗口还有 70% 在滑动窗口里，就按 `prev × 0.7` 算。
- O(1) 内存，基本消除边界 2 倍问题，代价是「假设上个窗口里的请求是均匀分布的」这个近似。
- `slidingCounterRetry` 是把估算公式反解出来，算「最早什么时候能再放一个」，测试 `TestSlidingCounterRetryAfterIsAccurate` 验证了：RetryAfter 前 1ms 还是拒绝，到点就放行。

**(b) 为什么**：
- 四种都实现是为了能对比、能按场景选（见 README 的对比表）。固定窗口是作为基线，用测试证明它的缺陷，再展示滑动窗口如何修复。
- 时间用 `int64` 纳秒做整数运算（窗口编号、时间戳），避免浮点误差导致边界判断出错；只有令牌数这种天然连续的量才用浮点。
- 新 key 的令牌桶是满的（`started` 标记），这样新用户第一次就能用突发额度。
- 滑动日志删除过期时间戳用 `copy` 原地左移，不分配新内存。

**(c) Go 语法**：
- `switch alg { case TokenBucket: ... }` —— Go 的 switch 默认不贯穿（不用写 break）。
- `l.store.update(key, now, func(s *tokenBucketState) time.Duration { ... })` —— 匿名函数（闭包），可以读写外面的变量 `d`，所以回调里填好的 `Decision` 在外面能拿到。
- `var d Decision` —— 声明即零值初始化（`Allowed=false`、数字=0），Go 没有 null 的结构体。
- `max(0, res[1])`、`min(...)` 是 Go 1.21 内置函数。
- 接口检查：`NewMemory` 返回 `LimiterCloser`（嵌入了 `Limiter` 接口再加 `Close()`），这是**接口嵌入**，相当于 Java 的 `interface A extends B`。

---

## 6. `internal/limiter/lua/*.lua` + `redis.go` — Redis 版本

**(a) 骨架**：
- 四个 Lua 脚本，每个都是：`redis.call('TIME')` 取 Redis 服务器时间 → 读状态（`HMGET` 或 `ZSET`）→ 和内存版一样的算法逻辑 → 写回状态 → `PEXPIRE` 设置过期 → 返回 `{allowed, remaining, reset_us, retry_us}`。
- `redis.go`：`//go:embed` 把 Lua 文件编译进二进制；`redis.NewScript` 预先算好脚本的 SHA1；`Allow` 里调用 `script.Run`，返回 4 个整数，转换成 `Decision`。
- key 的格式：`rg:<路由>:<算法>:{<客户端>}`。

**(b) 为什么**（面试重点）：
- **为什么要 Lua / 原子性**：限流是「读 → 判断 → 写」。如果分成 GET、SET 两条命令，两个网关可能同时 GET 到 99（limit=100），都判断「还能放」，都 SET 成 100，结果放过了 101 个。Redis 执行 Lua 脚本时是单线程、不会被其他命令打断的，整个读-判断-写是一步完成的。
- **为什么不用 `INCR` + `EXPIRE`**：固定窗口勉强可以（INCR 本身原子），但 INCR 和 EXPIRE 是两条命令，中间进程崩了就留下一个永不过期的 key；而且令牌桶、滑动日志这种需要「先读再条件写」的逻辑，单条命令表达不了。也可以用 `MULTI/WATCH` 乐观锁，但冲突时要重试，热点 key 下重试风暴，Lua 更简单高效。
- **为什么用 Redis 的 TIME**（时钟偏差）：如果每个网关用自己的 `time.Now()` 传给 Redis，网关 A 比 B 快 300ms，那么对同一个 key，A 认为已经进入新窗口了，B 认为还没有，令牌桶里 A 会多补令牌——结果就不准了。用 Redis 的 TIME，所有网关读的是同一个时钟。代价：依赖 Redis 主节点的时钟；主从切换时如果两台机器时钟不同，会有一次跳变。Redis 5 以后脚本是「效果复制」（复制写命令的结果而不是脚本本身），所以脚本里调用 TIME 这种非确定性命令再写数据是允许的；Redis 7 只支持效果复制。
- **为什么 EVALSHA**：每次只传 40 字节的哈希而不是整个脚本；Redis 重启丢了脚本缓存时，go-redis 收到 NOSCRIPT 会自动退回 EVAL。
- **为什么 `{}` hash tag**：Redis Cluster 只对 `{}` 里的部分做哈希分片。每个脚本只操作一个 key，而且声明在 KEYS 里，所以是 Cluster 安全的。（固定窗口和滑动计数器我故意用一个 HASH 存 window/count，而不是每个窗口一个 key，就是为了不在脚本里动态拼 key。）
- **为什么 key 里带算法名**：热加载时改了算法，不会把旧算法的状态当成新算法的读。
- **为什么每次调用有超时（默认 50ms）**：Redis 慢了不能把网关拖死，超时就当失败交给熔断器和失败策略。
- **数字精度的坑**：Lua 5.1 默认把大数字转字符串时可能用 `%.14g`，微秒时间戳有 16 位，会丢精度，所以写入时显式 `string.format('%d', ts)`。我在真 Redis 里 `HGETALL` 检查过存进去的 `ts` 是完整的 16 位。
- **滑动日志的 member 唯一性**：ZSET 的 member 相同会覆盖，两个请求同一微秒到达就只记一个。所以 member 用「实例随机 ID + 自增序号」。

**(c) Go 语法**：
- `//go:embed lua/token_bucket.lua` 加 `var tokenBucketLua string` —— 编译时把文件内容放进变量。需要 `import _ "embed"`（下划线导入：只要包的副作用，不直接用它）。
- `var scripts = map[Algorithm]*redis.Script{...}` —— 包级变量，程序启动时初始化一次。
- `atomic.Uint64` 的 `Add(1)` —— 无锁的原子自增，多个 goroutine 并发调用安全。
- `[]any{...}` —— `any` 就是 `interface{}`，相当于 Java 的 `Object`。
- `redis.Scripter` 是 go-redis 的接口，`*redis.Client` 和 `*redis.ClusterClient` 都满足，所以单机/集群都能传进来。

---

## 7. `internal/breaker/breaker.go` — 熔断器

**(a) 骨架**：三个状态。
- `Closed`（正常）：调用都放行，连续失败达到阈值（默认 5）→ `Open`。
- `Open`（熔断）：直接拒绝调用（走降级），过了冷却时间（默认 2s）→ `HalfOpen`。
- `HalfOpen`（试探）：只放一个探测请求，成功 → `Closed`，失败 → 重新 `Open`。
- API：`Allow()` 问能不能调用；调用完报告 `Success()` / `Failure()` / `Discard()`。

**(b) 为什么**：
- Redis 挂了时，没有熔断器的话，每个请求都要等 50ms 超时才失败，网关延迟暴涨、goroutine 堆积。熔断后直接走降级，延迟几乎为零，也给 Redis 恢复的空间（不被重试打爆）。
- 半开状态只放一个探测，防止 Redis 刚恢复就被所有请求一下子压垮。
- `Discard()`：客户端自己断开导致的 context 取消，不代表 Redis 有问题，不能算失败，否则客户端大量断开会误触发熔断。有测试 `TestCancelledRequestDoesNotTripBreaker`。
- 用「连续失败次数」而不是「失败率」：简单、好解释；缺点是偶发失败夹在成功中间不会触发。生产级的可以用滑动窗口失败率（比如 sony/gobreaker）。
- 一个熔断器被所有 Redis limiter 共享，因为它们依赖同一个 Redis。

**(c) Go 语法**：
- `const ( Closed State = iota; Open; HalfOpen )` —— `iota` 自动递增 0、1、2，是 Go 写枚举的惯用法。
- `func (s State) String() string` —— 实现了 `fmt.Stringer` 接口，打印时自动调用（类似 Java 的 `toString()`）。
- `onChange func(State)` —— 函数类型的字段，作为回调，main 里用它更新 Prometheus 指标和打日志，breaker 包本身不依赖 Prometheus。

---

## 8. `internal/limiter/resilient.go` — 失败策略（装饰器）

**(a) 骨架**：`Resilient` 包了一个主 limiter（Redis）、一个可选的本地 limiter、一个熔断器、一个失败模式。`Allow`：熔断器允许 → 调 Redis → 成功就返回；失败（或熔断中）→ 按模式降级：
- `fail_open`：放行，`Degraded=true`
- `fail_closed`：返回 `ErrLimiterUnavailable`，网关回 503
- `local_fallback`：用本地内存 limiter 判断

**(b) 为什么**：
- **装饰器模式**：`Resilient` 本身也实现 `Limiter` 接口，网关完全不知道 Redis 可能会挂，关注点分离。
- **三种策略是业务决策**：防止滥用、保公平的限流，宁可放行（fail-open）也不要因为 Redis 挂了让全站不可用；保护脆弱下游或者付费配额，宁可拒绝（fail-closed）。默认 `local_fallback` 是折中：每个实例各自限流，N 个实例最多放 N 倍，但仍然有上限。
- 为什么 fail-closed 回 503 而不是 429：429 的意思是「你发太多了」，而这时是「我们自己的限流组件坏了」，503 语义更准确，客户端/监控也能区分。

**(c) Go 语法**：
- `errors.New` 定义哨兵错误 `ErrLimiterUnavailable`，网关用 `errors.Is` 判断。
- `switch { case err == nil: ... case ctx.Err() != nil: ... default: ... }` —— 无条件 switch，相当于 if-else if 链，更整齐。
- `ResilientHooks` 结构体里放函数字段，nil 就不调用——一种轻量的「可选回调」写法。

---

## 9. `internal/config/config.go` — 配置

**(a) 骨架**：YAML → `Config` 结构体 → `applyDefaults()` 填默认值 → `Validate()` 校验并把所有错误一次性返回。

**(b) 为什么**：
- `dec.KnownFields(true)`：YAML 里拼错字段名（`limt: 100`）直接报错，而不是悄悄忽略然后限流不生效——这种 bug 在生产上很难发现。
- 校验收集**所有**错误再用 `errors.Join` 一起返回，用户一次就能看到全部问题。
- 有个测试 `TestShippedConfigsAreValid` 加载仓库里所有 YAML，保证示例配置永远和代码同步。

**(c) Go 语法**：
- 结构体标签 `` `yaml:"path_prefix"` ``：告诉 YAML 库字段对应的键名（类似 Java 的注解 `@JsonProperty`）。
- `time.Duration` 字段可以直接从 `"1s"`、`"500ms"` 解析。
- `*RateLimit` 指针字段：nil 表示「这个路由不限流」，用指针区分「没配置」和「配置了零值」。
- `add := func(format string, a ...any) {...}` —— 局部闭包 + 可变参数 `...any`。

---

## 10. `internal/gateway/` — HTTP 层

### `keys.go`
**(a)** `newKeyFunc` 根据 `key_by` 返回一个提取函数，返回 `(key, id)`：`key` 带类型前缀（`key:abc`、`ip:1.2.3.4`）用于计数，`id` 是原始值用于查找 override。没有对应 header 时退回按 IP。
**(b)** 加前缀是为了 API key 恰好等于某个 IP 字符串时不会撞到同一个计数器。没带 key 退回按 IP，否则所有匿名请求要么不限流、要么共享一个桶。`X-Forwarded-For` 取**最右边**一项：左边的是客户端自己写的，可以伪造；最右边是我们自己的负载均衡器加的。默认不信任 XFF。

### `routes.go`
**(a)** `buildTable` 把配置变成一张不可变的路由表：每个路由有自己的 `ReverseProxy`、默认 limiter、override limiter map，和预先解析好的 Prometheus 计数器。路由按前缀长度降序排，匹配时第一个命中的就是最长前缀。`newLimiter` 根据后端类型创建内存 limiter，或者 `Resilient(Redis + 本地兜底)`。
**(b)**
- 路由表**不可变** + 热加载时整体替换指针，请求路径上不需要任何锁。
- Prometheus 的 `WithLabelValues` 每次调用要做哈希查找，所以在构建时就解析好每个路由的 counter，请求时直接 `Inc()`。
- `ReverseProxy` 用 `Rewrite`（Go 1.20+）而不是旧的 `Director`：`Rewrite` 会先清掉客户端发来的 hop-by-hop 头和伪造的 `X-Forwarded-*` 头，更安全。
- **BufferPool**：`ReverseProxy` 默认每个请求新分配 32KB 缓冲区来拷贝响应体。加了 `sync.Pool` 复用之后，proxy-only 路由的吞吐量在一次 3 秒快测里从约 30.6k 提到 53.0k req/s（BENCHMARK.md 的 Experiments 一节）。这是「测出来再优化」的好例子。
- Transport 的 `MaxIdleConnsPerHost` 从默认 2 调到 1024：默认值下高并发时大部分请求都要新建 TCP 连接。

### `gateway.go`
**(a)** `ServeHTTP`：匹配路由 → 没匹配到 404 → 有限流就 `checkLimit` → 放行就转发。`checkLimit`：提取 key → 选 limiter → 计时调用 `Allow` → 根据结果回 503 / 429 / 继续，并写头。`Reload`：校验后端配置没变 → 建新表 → `atomic.Pointer.Swap` → 关闭旧表的清理 goroutine。
**(b)**
- 头部语义：`X-RateLimit-Reset` 用「还有几秒」而不是 Unix 时间戳，不依赖客户端时钟；`Retry-After` 向上取整，保证客户端等够这个秒数再来一定不会早。
- 被限流的请求**不会转发**到上游（测试里统计了上游收到的请求数）。
- fail-open 时没有真实的限额信息，所以不写 `X-RateLimit-*`，只写 `X-RateLimit-Degraded: true`。
- 热加载不允许改后端（内存↔Redis、Redis 地址），因为 Redis 连接和熔断器是全局共享的，换掉它们需要处理在途请求，复杂度不值得，直接要求重启。
- 访问日志默认关闭，因为高 QPS 下每个请求一行 JSON 很贵；开关是 `atomic.Bool`，热加载可以改。

**(c) Go 语法**：
- `http.Handler` 接口只有 `ServeHTTP(w, r)` 一个方法；`Gateway` 实现了它，就能直接 `mux.Handle("/", gw)`。
- `http.HandlerFunc(func(w, r) {...})` 把普通函数转成 Handler（函数类型也可以有方法）。
- `atomic.Pointer[routeTable]` —— 泛型原子指针，`Load()` / `Store()` / `Swap()` 都是无锁的。
- `statusRecorder` 通过**结构体嵌入** `http.ResponseWriter` 自动获得它的所有方法，只覆盖 `WriteHeader`——Go 用组合代替继承。`Unwrap()` 让 `http.ResponseController` 能找到底层的 writer。
- `defer func() { ... }()` 在函数结束时记访问日志，此时状态码已经知道了。

---

## 11. `internal/metrics/metrics.go` — 监控指标

**(a)** 定义所有 Prometheus 指标并注册到一个**私有** Registry。
**(b)** 用私有 Registry 而不是全局默认的，每个测试可以新建一套指标，不会出现「重复注册」panic。延迟直方图的桶从 10µs 到约 160ms 指数增长：内存判断落在最前面几个桶，Redis 往返落在中间。`rategate_redis_errors_total`、`rategate_breaker_state`、`rategate_degraded_decisions_total` 是 Redis 出问题时值班的人最先看的三个指标。
**(c)** `prometheus.NewCounterVec(opts, []string{"route", "decision"})` 带标签的计数器；`promhttp.HandlerFor` 生成 `/metrics` 的 handler。

---

## 12. `cmd/gateway/main.go` — 启动、优雅关闭、热加载

**(a)** 读配置 → 建 logger、metrics → 如果是 Redis 后端，建 Redis 客户端和熔断器 → 建网关 → 注册 `/metrics`、`/healthz`、`/` → 启动 HTTP 服务。`SIGHUP` 重新读配置并 `Reload`；`SIGINT/SIGTERM` 触发 `srv.Shutdown`。
**(b)**
- **优雅关闭**：`Shutdown` 先停止接受新连接，再等在途请求处理完（最多 `shutdown_timeout`），K8s 滚动发布时不会砍断正在处理的请求。
- Redis 客户端 `MaxRetries: -1`（不重试）：重试会放大故障，交给熔断器和失败策略处理。`ReadTimeout` 和每次调用的超时一致，Redis 卡住时快速失败。
- 启动时 Redis 连不上**不退出**，只打警告：失败策略本来就是为这种情况设计的；否则 Redis 一挂网关就起不来，雪上加霜。
- `run()` 返回 error、`main()` 只负责打印退出：这样 `defer` 能正常执行（`os.Exit` 会跳过 defer）。
- `/healthz` 在 Redis 熔断时仍然返回 200，body 里写 `degraded`：它是存活检查，网关本身还能工作，如果返回 500 会被 K8s 重启，没有任何帮助。

**(c) Go 语法**：
- `signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)` 收到信号时 ctx 被取消。
- `errCh := make(chan error, 1)` 带缓冲的 channel；`select { case err := <-errCh: ... case <-ctx.Done(): }` 同时等「启动失败」和「收到关闭信号」。
- `flag.String(...)` 返回指针，用 `*configPath` 取值。

---

## 13. `cmd/loadgen/main.go` — 自己写的压测工具

**(a)** 两种模式：
- **闭环**（默认）：`-c` 个 worker，每个发完一个请求马上发下一个，测最大吞吐量和这个并发下的延迟。
- **开环**（`-rate R`）：每秒总共发 R 个请求，不管响应快慢，用于精度测试（「offer 6 倍于限额的流量，看最终放过多少」）。

每个 worker 把延迟记在自己的切片里（无锁），结束后合并排序，按 nearest-rank 求 p50/p90/p99。先跑一段不计入的 warm-up（建立连接、填满连接池、消耗掉令牌桶的初始突发）。

**(b)**
- 每个 worker 自己的切片：避免多个 goroutine 抢同一把锁记录结果，压测工具本身不能成为瓶颈。
- 响应体要读完再关闭：否则 HTTP 连接不能复用，每个请求都新建 TCP 连接，测的就是建连开销了。
- 开环模式每 1ms 补发「到现在为止应该发的数量」，不依赖高精度定时器。
- 已知局限：开环模式下 worker 全忙时会排队（coordinated omission），所以延迟数字我只用闭环模式的；开环只用来数「放过了多少」。

---

## 14. 测试（面试时说「我是怎么证明它是对的」）

| 测试 | 证明了什么 |
|---|---|
| `TestAllAlgorithmsEnforceLimit` | 4 算法 × 2 后端（内存 / miniredis）基本限额、Remaining 递减、RetryAfter 合理 |
| `TestTokenBucketBurstThenSteadyRate` | 突发 20 个 → RetryAfter 恰好 100ms → 补充 1 个 → 1 秒补 10 个 → 空闲一小时也最多 20 个 |
| `TestBoundaryBurst` | 固定窗口在边界 100ms 内放 20 个（2 倍），滑动窗口只放 10 个 |
| `TestSlidingLogNeverExceedsLimitInAnyWindow` | 检查每一个 1 秒区间：滑动日志从不超过 50，固定窗口最多到 100 |
| `TestConcurrentNeverExceedsLimit` | 100 个 goroutine 同时打同一个 key，正好放 100 个（内存、miniredis、真 Redis） |
| `TestMultiInstanceSharedRedis` | 3 个独立网关实例共享 Redis，300 个并发请求，合计正好放 50 个；内存后端对照组放了 150 个 |
| `TestRedisDownFailureModes` | Redis 不可达时三种模式的行为，熔断后 Redis 只被调用 3 次，`/healthz` 显示 degraded |
| `TestBreakerStopsCallingRedis` / `TestRecoversWhenRedisReturns` | 熔断期间不再调 Redis；Redis 恢复后自动闭合 |
| `TestReloadUnderLoad` | 请求进行中反复热加载，配合 `-race` 检查数据竞争 |

测试技巧：
- 同一份测试代码跑在多个后端上（`backends(t)` 返回内存 + miniredis），内存用假时钟，miniredis 用 `SetTime` + `FastForward` 控制 Redis 的 TIME 和 TTL。
- 设置 `RATEGATE_REDIS_ADDR` 环境变量时，额外跑真 Redis 测试，否则自动跳过。
- `go test -race -count=10` 用来查不稳定测试。我真遇到过：第一次跑 10 遍时有测试失败，原因是网关测试用了 `http.DefaultClient`（每个 host 只保留 2 个空闲连接），并发测试大量新建连接，留下几千个 TIME_WAIT，把本机临时端口耗尽了（`can't assign requested address`）。换成连接池更大的测试客户端后，10 遍全过。这是个很好的面试故事。

---

## 15. 高频面试题

**Q1. 四种算法怎么比较？你默认用哪个？**
令牌桶：O(1) 状态，允许 burst 个突发然后按速率平滑，最常用（AWS API Gateway、Stripe 都是这类）。固定窗口：最简单，一个计数器，但边界可以放过 2 倍。滑动日志：精确，但每个 key 存 O(limit) 个时间戳，适合小而严格的限额（如登录 5 次/分钟）。滑动计数器：O(1) 内存，用上个窗口加权估算，基本消除边界问题，是近似值。我默认令牌桶，因为真实客户端流量本来就是突发的，而且它的状态最小。

**Q2. 固定窗口的边界问题具体是什么？你怎么证明滑动窗口解决了？**
限额 10/秒，客户端在 0.9 秒发 10 个、1.0 秒再发 10 个，两次各属于不同的窗口，全部放行——100ms 内放了 20 个。`TestBoundaryBurst` 就是这个场景：固定窗口放 20 个，滑动日志和滑动计数器都只放 10 个。另一个测试检查 5 秒内每个 1 秒区间，固定窗口最多放到 100（限额 50），滑动日志从不超过 50。

**Q3. 为什么用 Lua 脚本？不用会怎样？**
限流是读-判断-写。分成两条命令时，两个网关同时 GET 到 99，都判断能放，都写 100，就放了 101 个——典型的 check-then-act 竞态。Redis 单线程执行 Lua，整个脚本中间不会插入其他命令，所以是原子的。测试里 100 个 goroutine 打同一个 key 正好放 100 个，3 个网关实例共享 Redis 合计正好放 50 个。

**Q4. 除了 Lua 还有什么办法保证原子性？**
(1) `INCR` 本身是原子的，固定窗口可以用 INCR + EXPIRE，但两条命令之间崩溃会留下永不过期的 key（可以用 `SET NX EX` 先建 key 规避），且令牌桶这种条件写做不到。(2) `WATCH/MULTI/EXEC` 乐观锁，冲突时重试，热点 key 下重试很多。(3) Redis 7 的 Functions，本质也是服务器端脚本。(4) 分布式锁，太重，每次请求多两次往返。Lua 是一次往返、无重试、逻辑最清楚。

**Q5. 内存版本有什么竞态问题？你怎么处理的？**
多个 goroutine 同时更新同一个 key 的计数会丢更新。我用分片锁：同一个 key 永远落在同一个分片，`update` 在该分片的锁里完成整个读-改-写。不同 key 落在不同分片可以并行。`go test -race` 检查数据竞争，并发测试检查结果正确性（竞态检测器只能发现内存访问冲突，发现不了逻辑上的 check-then-act 问题，所以两种测试都需要）。

**Q6. 多台网关的时钟不一致怎么办？**
所有时间都用 Redis 的 `TIME` 命令，所有网关读的是同一个时钟，网关机器之间的时钟偏差完全不影响结果。代价是依赖 Redis 主节点的时钟，主从切换时如果新主的时钟不同会跳变一次，影响一个窗口左右。另一个方案是网关把自己的时间作为参数传进去，那样就需要所有网关用 NTP 严格同步，而且偏差会直接变成误差。响应头里的 Reset 也用「还有几秒」而不是绝对时间戳，不依赖客户端时钟。

**Q7. Redis 挂了会怎样？**
每次 Redis 调用有 50ms 超时。连续 5 次失败熔断器打开，之后 2 秒内直接走降级，不再等超时；2 秒后放一个探测请求，成功就恢复。降级按配置：fail-open 放行、fail-closed 回 503、local-fallback 用本地内存限流（N 个实例最多放 N 倍）。降级响应带 `X-RateLimit-Degraded: true`，有指标 `rategate_degraded_decisions_total` 和 `rategate_redis_errors_total`，`/healthz` 显示 degraded。这些都有测试。

**Q8. fail-open 和 fail-closed 怎么选？**
看限流保护的是什么。如果是防止单个用户占用过多资源、保证公平，Redis 挂了时放行影响不大，fail-open 保可用性。如果下游很脆弱（不限流就会被打挂）或者限额是付费合同，fail-closed。中间方案是本地降级：有上限，但比全局限额宽松 N 倍。

**Q9. 热点 key 怎么办？比如一个大客户每秒几万请求？**
同一个 key 的所有请求都会落到 Redis 的同一个分片、在同一个 key 上串行执行，成为瓶颈。办法：(1) 本地预取：每个网关一次从 Redis 拿一批令牌（比如 100 个），本地消耗完再去拿，Redis 调用量降 100 倍，代价是精度略降。(2) 把一个 key 拆成 N 个子 key，每个子 key 限额 1/N，请求随机打到一个子 key。(3) 对超大客户在网关本地先做一层粗粒度限流。这个项目里没实现，在 README 的 Limitations 里写了。

**Q10. 扩展到很多网关实例时，瓶颈在哪？**
网关本身是无状态的，可以水平扩展，限流状态全在 Redis。瓶颈变成 Redis：单个 Redis 每秒能执行的脚本数有上限：本机微基准里，128 个并发调用方对一个本地 Redis，令牌桶脚本约 17,150 ns/op，折合每秒约 5.8 万次判断（`BENCHMARK.md` 第 3 节）。办法：用 Redis Cluster 按 key 分片——我的脚本每个只操作一个 key，并且用了 `{}` hash tag，是 Cluster 安全的（但我没有在 Cluster 上实际测试过）；或者上面说的本地预取减少调用次数。

**Q11. 你怎么测网关的额外开销？**
自己写的 `loadgen`，闭环模式，同样的并发数、同样的时长，分别打四个目标：直连上游（基线）、经过网关但不限流（只有代理开销）、网关 + 内存限流、网关 + Redis 限流。对比吞吐量和 p50/p99。还有 `go test -bench` 微基准单独测每个算法的单次判断耗时。数字在 `BENCHMARK.md`，要强调：全部在一台 8 核笔记本上跑，压测工具、网关、上游、Redis 抢同一组 CPU，走 localhost，所以绝对值偏保守，看的是相对开销。结论是：开销主要来自多一跳 HTTP 代理，限流判断本身（尤其内存版）占比很小。具体数字（64 并发，BENCHMARK.md 第 1 节）：直连 118,978 req/s、p50 0.320ms；只有代理 43,366 req/s、p50 0.922ms；代理+内存限流 42,939 req/s、p50 0.942ms；代理+Redis 限流 24,396 req/s、p50 2.323ms。还要主动说：我重复跑了 3 次，同一组数在不同时段能差 15-26%，所以我只敢说「内存限流的开销在噪声以内」「Redis 版大约是内存版吞吐的 53-61%」，不说精确百分比。

**Q12. 精度怎么测的？**
3 个网关实例共享一个 Redis，每个路由一种算法，限额都是 1000 次/秒，用开环模式以 6000 次/秒的总速率、同一个 API key 轮流打 3 个实例 10 秒，统计每秒 200 的数量。共享 Redis 时，令牌桶、固定窗口、滑动日志 10 秒内都正好放行 10,000 个（1000.0/秒），滑动计数器放行 9,990 个（因为它要求 `估算值 + 1 <= limit`，会略微偏保守）；作为对照，内存后端的 3 个实例各自限流，合计放行约 3 倍（2983-3000/秒）。

**Q13. 限流 key 怎么选？按 IP 有什么问题？**
有 API key 优先按 API key（最准确，对应计费主体）；登录前的接口只能按 IP。按 IP 的问题：公司/学校 NAT 后面成百上千个用户共享一个 IP 会被误伤；攻击者可以换 IP（代理池）绕过；在负载均衡后面看到的都是 LB 的 IP，需要信任 `X-Forwarded-For`，而 XFF 左边的值客户端可以伪造，所以我取最右边那个（我们自己 LB 写的）并且默认关闭。

**Q14. 为什么返回 429 还要写 Retry-After 和 X-RateLimit-* 头？**
让行为良好的客户端能自我调节：看到 Remaining 快没了就慢下来，被拒绝后等 Retry-After 秒再重试，而不是立刻重试形成重试风暴。`Retry-After` 向上取整，保证客户端等够了再来一定能成功（滑动计数器有一个测试专门验证 RetryAfter 前 1ms 拒绝、到点放行）。

**Q15. 内存会无限增长吗？**
不会。内存版每个 key 有过期时间（等于「状态恢复到全新」所需时间），后台 goroutine 定期一个分片一个分片地清理，有测试验证 100 个 key 过期后全部被清掉。Redis 版每个 key 都有 `PEXPIRE`，空闲客户端的 key 自动消失。

**Q16. 热加载怎么做到不影响在途请求？**
配置会被构建成一张不可变的路由表，热加载时建一张新表，用 `atomic.Pointer` 原子替换。正在处理的请求手里拿的还是旧表的指针，继续正常用完；新请求拿到新表。请求路径上没有任何锁。有个测试在 8 个 goroutine 持续发请求的同时热加载 20 次，用 `-race` 跑。

**Q17. 优雅关闭怎么实现的？为什么重要？**
收到 SIGTERM 后调用 `http.Server.Shutdown`：停止接受新连接，等正在处理的请求完成，最多等 10 秒。K8s 滚动发布、扩缩容时会给 Pod 发 SIGTERM，没有优雅关闭的话正在进行的请求会直接断掉，客户端看到 502。

**Q18. 系统设计题：设计一个限流器（Design a rate limiter）。**
按这个项目的结构讲：
1. 需求澄清：限流维度（用户/IP/API key）、规则（每秒/每分钟）、单机还是分布式、精度要求、Redis 挂了怎么办、延迟预算。
2. 放在哪：API 网关/中间件层（本项目），而不是每个服务各写一份。
3. 算法选择：讲 Q1 的对比，默认令牌桶。
4. 存储：单机用内存；分布式用 Redis + Lua 保证原子，Redis TIME 解决时钟偏差，每个 key 设过期。
5. 返回：429 + Retry-After + X-RateLimit-* 头。
6. 可靠性：超时 + 熔断 + fail-open/closed/本地降级。
7. 扩展：Redis Cluster 按 key 分片、热点 key 本地预取、规则热加载。
8. 监控：放行/拒绝计数、限流延迟、Redis 错误、熔断状态。

**Q19. 为什么用 Go？goroutine 在这里有什么好处？**
`net/http` 每个连接一个 goroutine，几 KB 栈，成千上万并发连接也不需要线程池调优；标准库自带生产级的 `httputil.ReverseProxy`；`-race` 竞态检测器内置；编译成单个静态二进制，部署简单。

**Q20. 如果让你继续改进，你会做什么？**
(1) 热点 key 的本地令牌预取；(2) 在 Redis Cluster 上实测；(3) 本地降级时按实例数分摊限额（需要服务发现知道 N）；(4) 熔断器改成滑动窗口失败率；(5) 支持多维度组合限流（同时按用户和按 IP）；(6) 把压测放到多台机器上，排除 localhost 和 CPU 争用的影响。

**Q21. 你的测试是怎么避免「靠 sleep 等时间」的？**
时钟抽象成接口，内存版测试注入假时钟，`Advance(100ms)` 就是 100ms 过去了；Redis 版用 miniredis 的 `SetTime` 控制脚本里 `TIME` 的返回值、`FastForward` 让 TTL 过期。所以所有算法测试都是确定性的，不 sleep，跑得快也不会因为机器负载而随机失败。

**Q22. miniredis 和真 Redis 有什么不同？你为什么两个都用？**
miniredis 是 Go 写的内存模拟器，Lua 用的是 gopher-lua 而不是真 Redis 的 Lua 5.1，数字格式、某些命令细节可能不同；优点是不需要安装，并且可以控制时间。所以算法逻辑测试用 miniredis（可控时间），另外用真 Redis 跑并发、多实例和基本行为测试，确认脚本在真实环境里也对。
