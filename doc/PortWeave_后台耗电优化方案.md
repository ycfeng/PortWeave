# PortWeave 后台耗电优化方案

> 状态：方案草案（待评审）
> 日期：2026-11-06
> 代码基线：`fdd35bb`（master）
> 关联文档：《PortWeave_Android_端口映射应用开发设计文档》§13.5 进程恢复、§13.6 网络变化、§13.7 电量策略

---

## 1. 背景与目标

### 1.1 现象

应用在后台运行一段时间后，部分安卓 ROM 弹出系统提示：“后台频繁刷新，耗电过快，建议结束运行”。用户一旦点了“结束运行”，系统会直接杀掉进程，所有映射随之中断。`START_STICKY` 拉起服务后很可能再次触发提示，体验很差。

### 1.2 目标

1. **消除应用自己制造的高频周期唤醒**。这是本次的主因，详见 §3。
2. **抑制无效握手和重连风暴**，包括服务端不可达、网络抖动等场景。
3. 在不牺牲可用性的前提下，**按“界面可见 / 后台亮屏 / 熄屏 / Doze”分档降频**。
4. 用厂商白名单引导做兜底，但不把它当主要手段。

### 1.3 非目标

不做任何“对抗系统检测”的保活手段，比如双进程互拉、无声音频、1 像素 Activity、伪装前台服务类型、高频 Alarm 拉活。这些手段违反应用商店政策，而且会让耗电检测更容易命中。§4.10 有详细说明。

---

## 2. 系统提示的触发机制（推断）

各厂商没有公开具体的检测规则和阈值。根据公开资料和常见实现，这类“后台高耗电 / 频繁刷新”检测一般统计应用**处在后台时**的以下指标。后台包括两种情况：亮屏但用户在用别的应用，以及熄屏。

| 维度 | 典型统计口径 | 与本应用的关系 |
|---|---|---|
| 后台 CPU 时间 / 占比 | 一个窗口期内进程累计占用的 CPU 时间 | 500ms 级协程轮询、反复握手 |
| 唤醒次数 | 定时器 / Alarm / WakeLock 唤醒 CPU 的次数 | 周期性 `delay` 循环、保活定时器 |
| 网络活动频度 | 收发包次数、移动射频激活次数 | 多隧道心跳相位不齐、重连握手 |
| 通知刷新频率 | 前台服务通知 `notify()` 的次数（部分 ROM 统计） | 重连抖动导致通知反复更新 |
| 自启动 / 重启 | 被杀后被拉起的次数 | `START_STICKY` 恢复 |

关键判断：

- 提示里写的是“**频繁**刷新”，说明问题在于**单位时间内的周期性活动次数**，跟单次流量大小关系不大。所以优化的重点是**降频、合并、事件驱动**。
- 前台服务**不能**豁免厂商自己的耗电监控。`REQUEST_IGNORE_BATTERY_OPTIMIZATIONS` 只影响 AOSP 的 Doze 和 App Standby，多数 ROM 上不影响厂商的耗电提醒。
- 本应用不持有 WakeLock。熄屏且 CPU 挂起时，协程定时器不会主动唤醒 CPU。但设备一旦因为别的原因醒来（网络包、其他应用、系统任务），所有到期的定时器会**集中执行**，而每次执行都会计入本应用的 CPU 时间和唤醒次数。

---

## 3. 现状排查

### 3.1 后台周期性工作清单

“每小时次数”按**单条隧道**、按代码里的常量估算。

| # | 来源 | 位置 | 触发条件 | 间隔 | 每小时次数 | 影响 |
|---|---|---|---|---|---|---|
| 1 | SSH 断线检测轮询 | `tunnel-engine/.../ssh/SshTunnelBackend.kt:266`<br>`tunnel-engine/.../RuntimeWorkPolicy.kt:23` | **亮屏（即使应用在后台）**，或隧道上有活动连接 | 500ms | **~7,200** | 🔴 高 |
| 1′ | 同上 | 同上 | 熄屏且没有活动连接 | 5s | ~720 | 🟠 中 |
| 2 | SSH 外层重连 | `app/.../service/SshRuntimeRunner.kt:140`、`:377` | 有网络但服务端不可达，或可重试错误 | 全抖动指数退避，上限 60s（均值约 30s） | **~120 次握手** | 🔴 高 |
| 3 | STCP 控制连接内层重连 | `frpc-stcp-visitor/.../control/FrpControlClient.kt:800`<br>`FrpControlModels.kt:174` | 服务端不可达 | 指数退避，上限 **20s（硬编码上限）** | **~180 次 TCP+TLS 握手** | 🔴 高 |
| 4 | 网络变化全量重建 | `SshRuntimeRunner.kt:220` | 默认网络切换或 VALIDATED 状态变化 | 事件触发 | 弱网下可能很频繁 | 🟠 中 |
| 5 | “稳定 60s 即重置退避” | `SshRuntimeRunner.kt:375` | 连接每次撑过 1 分钟就断（NAT 回收、弱网） | — | 退避一直停在 1–2s | 🟠 中 |
| 6 | SSH 保活 | `SshTunnelBackend.kt:124` | 一直运行 | 默认 30s（`keepAliveSeconds`） | 120，另加服务端 ClientAlive | 🟠 中 |
| 7 | yamux 保活 | `frpc-stcp-visitor/.../transport/YamuxTransportAdapter.kt:24` | STCP 一直运行 | 30s | 120，另加 frps 侧保活 | 🟠 中 |
| 8 | SSH 流量指标发布 | `tunnel-engine/.../ssh/SshMetricsTracker.kt:30` | 后台且有流量 | 5s | ≤720 | 🟡 低 |
| 9 | 前台通知刷新 | `app/.../service/TunnelForegroundService.kt:145` | 就绪数、活跃数或可恢复标志变化 | 事件触发 | 每次重连至少 2 次 | 🟡 低 |
| 10 | 事件日志落库 | `SshRuntimeRunner.kt:477` | 每次启动、重试、网络变化 | 事件触发 | 跟重连次数同一量级，每次一条 SQLite 写 | 🟡 低 |
| 11 | STCP 本地端口绑定重试 | `frpc-stcp-visitor/.../KotlinFrpcStcpVisitorClient.kt:874`（`VISITOR_BIND_RETRY_DELAY_MILLIS = 10s`） | 本地端口被占用 | 10s，没有上限 | 360 | 🟡 低 |
| 12 | Gomobile 客户端状态轮询 | `tunnel-engine/.../stcp/FrpcStcpBackend.kt:40` | 只在客户端不是 Observable 时 | 500ms | 7,200 | ⚪ 生产默认用 Kotlin 客户端，不会触发 |

现有实现里**已经做对**的部分，应当保留：

- 不持有 WakeLock / WifiLock，没有使用 AlarmManager 或 JobScheduler。
- 无网络时挂起，等待 `NetworkCallback`，而不是计时轮询（`SshRuntimeRunner.kt:110`）。
- UI 统一用 `collectAsStateWithLifecycle`，不可见时停止收集。
- 端口协议探测只在首页可见时进行。
- 通知渠道为 `IMPORTANCE_LOW`，模型没变时不重复 `notify()`。
- STCP Kotlin 客户端用 `observeState()` 事件流，不轮询。

### 3.2 主要结论

1. **头号嫌疑：断线检测轮询用“是否亮屏”作为高频条件**。`RuntimeWorkPolicy.checkInterval()` 只要 `interactive == true` 就返回 500ms。用户把应用切到后台继续用手机时，**每条 SSH 隧道每秒有 2 次协程唤醒**。熄屏时，只要客户端还保持长连接（IDE、浏览器、WebSocket 等），同样是 500ms。这跟“后台运行一段时间后提示”的现象完全吻合。轮询的目的只是检查 `session.isConnected` 这个布尔值，可以完全换成事件驱动。
2. **第二嫌疑：两层无限重试**。服务端不可达时，外层 runner（平均约 30s 一次）和 STCP 内层 control client（最长 20s 一次）都会**一直**重试，而且不区分前后台。每次都要做 DNS、TCP，再加 TLS 或 SSH KEX 握手，CPU 开销大，还会激活射频。
3. **网络抖动会被放大**。任何网络状态变化都会 cancel 并 invalidate **所有**隧道，再整树重建。STCP 客户端本来提供了只替换传输层的轻量接口 `reconnectAfterDefaultNetworkChange()`，但目前没有地方调用。
4. **心跳固定且相位随机**。N 条隧道各自按 30s 心跳、相位互不相关，导致射频在 30s 内被激活 N 次。

---

## 4. 优化方案

优先级说明：**P0** 是止血项，收益最大；**P1** 进一步降耗；**P2** 是产品化和兜底。

### 4.0 公共基础：运行档位 `RuntimeMode`

后续各项都依赖一个统一的档位，由 `RuntimeWorkPolicy` 现有的 `visible`、`interactive`、`deviceIdle` 三个状态推导，再加上系统省电模式：

```kotlin
enum class RuntimeMode { VISIBLE, BACKGROUND, SCREEN_OFF, DEVICE_IDLE }

class RuntimeWorkPolicy(...) {
    val interactive = MutableStateFlow(true)
    val deviceIdle = MutableStateFlow(false)
    val visible = MutableStateFlow(false)
    val powerSave = MutableStateFlow(false)          // 新增：PowerManager.isPowerSaveMode

    fun mode(): RuntimeMode = when {
        visible.value -> RuntimeMode.VISIBLE
        deviceIdle.value -> RuntimeMode.DEVICE_IDLE
        !interactive.value || powerSave.value -> RuntimeMode.SCREEN_OFF
        else -> RuntimeMode.BACKGROUND
    }

    /** 档位变“更活跃”时触发：界面可见、亮屏、退出 Doze。用于提前结束等待。 */
    val wakeTriggers: Flow<Unit> = merge(
        visible.filter { it }, interactive.filter { it }, deviceIdle.filter { !it },
    ).map { }
}
```

`RuntimePowerMonitor` 需要额外注册 `PowerManager.ACTION_POWER_SAVE_MODE_CHANGED`，用来更新 `powerSave`。

---

### 4.1 【P0】SSH 断线检测改为事件驱动

**问题**：`SshBackendHandle.awaitConnectionLost()` 以 500ms 或 5s 的间隔轮询 `session.isConnected`。

**A. 短期止血（改动小，可以先单独发版）**

把高频条件从“亮屏”改成“界面可见”，后台只保留低频兜底。已在阶段一实现：

```kotlin
fun checkInterval(activeConnections: Int?): Long = when {
    visible.value -> 500L                                      // 前台：行为不变
    interactive.value && activeConnections != 0 -> 5_000L      // 后台亮屏，且隧道正在被使用
    else -> 30_000L                                            // 后台空闲，或熄屏
}
// awaitNextCheck() 同时监听 visible、interactive、metrics：
// 间隔一变就立刻结束等待，所以回到前台、或最后一条连接关闭时会马上检查一次。
```

估算效果（单条 SSH 隧道）：
- 后台亮屏空闲：约 7,200 → 120 次/小时
- 熄屏且有长连接：约 7,200 → 120 次/小时
- 后台亮屏且正在使用：约 7,200 → 720 次/小时

代价：后台空闲时断线最多晚 30s 才被发现，后台使用中最多晚 5s。会话断开时，上面的转发连接通常会一起关闭，活动连接数降到 0 会提前唤醒检查，所以实际延迟一般更短。这个代价只存在于步骤 B 完成之前。

**B. 根治：通过 JSch `SocketFactory` 感知断开**

断线最终都会表现为以下三种情况之一：底层 socket **读到 EOF**、**读抛出 IOException**、**被 close**。JSch 在保活失败或调用 `disconnect()` 时也会关闭 socket。所以可以包装 socket，在这三种情况下完成一个 `CompletableDeferred`：

```kotlin
internal class DisconnectAwareSocketFactory(
    private val connectTimeoutMillis: Int,
) : com.jcraft.jsch.SocketFactory {
    val lost = CompletableDeferred<Unit>()

    override fun createSocket(host: String, port: Int): Socket {
        val socket = object : Socket() {
            override fun close() { try { super.close() } finally { lost.complete(Unit) } }
        }
        try {
            socket.connect(InetSocketAddress(host, port), connectTimeoutMillis)
        } catch (failure: Exception) {
            runCatching { socket.close() }
            throw failure
        }
        return socket
    }

    override fun getInputStream(socket: Socket): InputStream =
        object : FilterInputStream(socket.getInputStream()) {
            override fun read(): Int = observe { super.read() }
            override fun read(b: ByteArray, off: Int, len: Int): Int = observe { super.read(b, off, len) }
            private fun observe(block: () -> Int): Int = try {
                block().also { if (it < 0) lost.complete(Unit) }
            } catch (timeout: SocketTimeoutException) {
                throw timeout                  // JSch 用 SO_TIMEOUT 驱动保活，读超时不代表断线
            } catch (failure: IOException) {
                lost.complete(Unit); throw failure
            }
        }

    override fun getOutputStream(socket: Socket): OutputStream = socket.getOutputStream()
}
```

```kotlin
// SshBackendHandle
override suspend fun awaitConnectionLost() {
    while (session.isConnected) {
        workPolicy.counters.increment("sshStateChecks")
        withTimeoutOrNull(SafetyCheckMillis /* 60_000 */) { session.connectionLost.await() }
    }
}
```

实现时要注意：

- 在 `SshSessionHandle` 上新增 `val connectionLost: Deferred<Unit>`。`JschSessionHandle` 在 `create()` 时调用 `session.setSocketFactory(factory)`。测试用的 fake 也要实现这个属性。
- 设置了自定义 `SocketFactory` 后，JSch 不再负责连接超时，所以在 `createSocket` 里用 `connect(addr, timeout)` 自行处理。取消逻辑不变，仍由 `connectCancellable` 里的 `disconnect()` 关闭 socket。
- `SocketTimeoutException` 是 JSch 实现 `serverAliveInterval` 的机制，**不能**当作断线。
- 要对照 `com.github.mwiede:jsch:2.28.3` 的源码确认两点：`socket_factory` 非空时 `Session.connect` 的分支行为，以及 `disconnect()` 会关闭 socket。
- 保留 60s 兜底轮询，防止某条异常路径漏发信号。

**测试**：

- `SshTunnelBackendTest`：服务端主动关闭连接后，`awaitConnectionLost()` 应在 1s 内返回。
- `SshTunnelIntegrationTest`：杀掉 sshd 的会话子进程，验证能快速恢复。

**收益**：后台检测唤醒从约 7,200 次/小时降到约 60 次/小时（只剩兜底）。断线发现从“最多 500ms 或 5s”变为**即时**。

**实现说明（阶段二，已完成）**：

- 核对 `jsch-2.28.3` 源码后做了简化：会话的所有结束路径（读循环 EOF 或 IOException、保活次数用尽、主动 `disconnect()`）都会进入 `Session.disconnect()`，它先把 `isConnected` 置为 false，再关闭 socket。所以只在 `Socket.close()` 上发信号就足够，**不需要再包装 InputStream**。见 `tunnel-engine/.../ssh/DisconnectAwareSocketFactory.kt`。
- 只有 TCP 建连成功后才会发信号。建连失败直接抛异常；JSch 会包装成 `JSchException("Session.connect: …", cause)`，原始异常保留在 cause 中，所以 `mapFailure` 的错误映射（DNS、拒绝连接、超时）不受影响。
- 安装 factory 后，JSch 不再负责连接超时，改由 `JschSessionHandle.connect()` 把 `connectTimeoutSeconds` 传给 factory。
- 顺带修复了一个问题：在 TCP 建连阶段 `Session.disconnect()` 什么也不做，所以以前在建连阶段取消启动，要等连接超时才会真正结束。现在 `disconnect()` 会额外调用 `factory.abort()`，立即关闭 socket。
- 兜底检查改为 60s 一次。没有信号的会话（例如测试用的 fake）仍然走阶段一的分档轮询。
- 测试：
  - `DisconnectAwareSocketFactoryTest`：关闭 socket 时发信号、建连失败不发信号、`abort()`、兜底计数。
  - `SshTunnelIntegrationTest.serverSideDisconnectIsReportedImmediatelyWithoutPolling`：用真实 sshd 从服务端断开会话，2s 内感知，期间只检查 1 次。按旧逻辑，后台空闲时最多要 30s。

---

### 4.2 【P0】按档位分级的重连策略，加入“休眠等待”

**问题**：

- 退避上限固定 60s（外层）和 20s（STCP 内层），而且永不放弃。
- 判定稳定的窗口只有 60s，连接“活一会儿就断”时退避会被反复重置。
- 两层重试叠加。

**方案**：

1. **按档位设置退避参数**

   | 档位 | 退避上限 | 连续失败多少次后进入休眠 | 说明 |
   |---|---|---|---|
   | `VISIBLE` | 60s | 不休眠 | 维持现有体验 |
   | `BACKGROUND` | 5 min | 20 次 | 亮屏但用户在用别的应用 |
   | `SCREEN_OFF` | 15 min | 10 次 | 熄屏或系统省电模式 |
   | `DEVICE_IDLE` | 不主动计时 | 立即 | 只等触发器 |

2. **休眠等待**：进入休眠后不再设定时器。状态显示为“等待网络变化或回到前台后重试”，通知文案也同步更新。以下触发器会**立即**发起一轮重试：
   - 默认网络标识变化（见 §4.3）
   - `RuntimeWorkPolicy.wakeTriggers`：界面可见、亮屏、退出 Doze
   - 通知上的“立即重试”按钮（现有的 `ActionRetry`）

   ```kotlin
   // SshRuntimeRunner：用这个函数替换所有裸 delay(wait)
   private suspend fun awaitRetry(waitMillis: Long?) {
       val triggers = merge(policy.wakeTriggers, networkChanges, userRetry)
       if (waitMillis == null) triggers.first()                 // 休眠：只等触发器
       else withTimeoutOrNull(waitMillis) { triggers.first() }  // 普通退避：可以被提前唤醒
   }
   ```

3. **切到更活跃的档位时提前重试**：应用从后台回到前台时，如果当前剩余等待时间超过 `VISIBLE` 档的上限，立即重试，避免用户看到“已经等了 10 分钟”。

4. **稳定判定**：从 60s 改为 5min。也可以改为衰减方式：每稳定 1 分钟，`attempt` 减 1。另外统计每小时的断开次数，超过阈值（例如 6 次/小时）时即使在后台亮屏也使用 `SCREEN_OFF` 档参数。

5. **只保留一层驱动重试**：STCP 内层 `FrpControlClient` 的重连也要服从同一套策略：
   - 不要在库里依赖 Android。在 `FrpControlConfig` 中新增一个注入点 `reconnectDelay: (attempt: Int) -> Long`。默认值保持现有行为（1s 起步，上限 20s，与 Go 版 frpc 一致），这样 interop 契约测试不受影响。
   - 也可以放开 `MAXIMUM_RECONNECT_DELAY_MILLIS` 的校验上限，由引擎在档位切换时调整。
   - 引擎侧通过 `FrpcSessionConfig` 透传档位策略。

6. **批量对齐**：在后台，多个根配置的重试安排在**同一时刻**一起发起，一次射频激活完成所有尝试。这里不用逐条抖动，因为单客户端隧道数量少，不存在惊群问题。前台仍保留抖动。

**收益**：服务端不可达、熄屏 2 小时的场景下，握手次数从约 600 次（两层合计）降到首小时 ≤15 次，之后基本为 0。

**实现说明（阶段二，已完成）**：

- **档位与退避**：`RuntimeMode`（`tunnel-engine/.../RuntimeWorkPolicy.kt`）按上表实现。系统省电模式归入 `SCREEN_OFF`。`DEVICE_IDLE` 下第一次失败就进入休眠。
  - `retryDelay()` 的上限取 `min(档位上限, 1s × 2^(n-1))`。
  - 前台保留全抖动。
  - 后台只要上限达到 30s，就不再抖动，直接取上限并**向上对齐到 30s 网格**（对应 §4.2-6 的批量对齐），这样多条隧道会在同一个时刻重试。30s 以内的短等待仍然抖动。
- **休眠与提前唤醒**：`awaitRetry()` 替换了 runner 里所有裸 `delay`。
  - 休眠等待只有触发器能结束：界面可见、亮屏、退出 Doze、真实的网络变化。
  - 定时等待在两种情况下提前结束：网络变化；或者切换到的新档位上限小于当前等待时长（对应 §4.2-3）。
  - 用户点“立即重试”和网络变化，仍然走原有的“取消后重新恢复”路径。
- **状态展示**：休眠时状态是 `TunnelState.Reconnecting(dormant = true)`。首页和详情页显示“暂停重连 · 网络变化或打开应用后继续”。日志的 messageKey 为 `retry_dormant`。
- **修复：退避次数来源**：断线后的退避次数以前取自 `RuntimeSnapshot.reconnectCount`，它是 UI 用的历史最大值。所以阶段一的“稳定 5 分钟后重置”只能撑过一次断线，下一次又会从历史最大值接着算。现在每条隧道单独记录“连续断线次数”，停止隧道时清零。见测试 `lossBackoffRestartsFromOneAfterStablePeriodAndKeepsCountingFromThere`。
- **STCP 内层重试**：`FrpcSessionConfig` 带 `@Serializable`，不能挂回调。所以改为新增公开的 `FrpcReconnectGate` 接口，通过 `KotlinFrpcStcpVisitorClient(context, gate)` 注入，再经 `FrpControlConfig.reconnectGate` 接管 `FrpControlClient` 的重连等待。
  - 不注入时（默认为 null）保持 frpc 原有时序。`MAXIMUM_RECONNECT_DELAY_MILLIS` 的校验没有改动。
  - 引擎侧的 `awaitTransportReconnect()`：前台沿用库自带的延迟，后台按档位退避并支持休眠。
- **未做**：§4.2-4 后半段“按每小时断线次数升档”。这一项与自适应心跳关系更紧，并入阶段三。

---

### 4.3 【P0】网络变化去抖，并区分“换网”和“抖动”

**问题**：

- `NetworkMonitor` 每次可用性或网络对象变化都会发布状态，而可用性要求 `VALIDATED`。弱网或认证门户场景下 `VALIDATED` 会来回跳。
- `onNetworkStatus` 每次都对所有隧道做 invalidate 再整树重建。

**方案**：

1. 在 `NetworkStatus` 中增加 `networkHandle: Long?`（取自 `Network.getNetworkHandle()`），用来区分**真正换网**和**同一网络上的能力抖动**。
2. 去抖：把切换过程中连续触发的 `onLost`、`onAvailable`、`onCapabilitiesChanged` 合并成一次处理。

   ```kotlin
   @OptIn(FlowPreview::class)
   networkJob = scope.launch {
       networkMonitor.status.drop(1)
           .debounce(NetworkSettleMillis)            // 建议 1_500–3_000ms
           .distinctUntilChanged { a, b -> a.available == b.available && a.networkHandle == b.networkHandle }
           .collectLatest(runner::onNetworkStatus)
   }
   ```

3. 按情况分别处理：

   | 情况 | 处理 |
   |---|---|
   | 同一网络上 `VALIDATED` 消失 | **不拆连接**，由保活判定。只更新 UI 上的“网络受限”提示 |
   | 默认网络换了（handle 变化） | SSH：重建。TCP 绑定在旧网络上，必须重建。<br>STCP：调用 `client.reconnectAfterDefaultNetworkChange()`，只替换传输层，**保留 visitor 和本地监听**，不重建依赖树 |
   | 从不可用变为可用 | 对 desired roots 只恢复一次。休眠中的重试被唤醒 |
   | 从可用变为不可用 | 维持现有逻辑：标记为 `Blocked(NETWORK_UNAVAILABLE)`，挂起等待 |

**收益**：每次 Wi-Fi 与蜂窝之间的切换，每条隧道最多重建一次。STCP 不再反复解绑和重绑本地端口，同网抖动不再触发重建。

**实现说明（阶段二，已完成）**：

- `NetworkStatus` 新增 `networkHandle` 字段。服务端对网络状态做 2s 去抖（`NetworkSettleMillis`）。
- 去重没有用 `distinctUntilChanged`。runner 把新状态与“当前隧道所基于的网络”（`settledNetwork`）比较，而不是与上一条原始状态比较。这样，去抖窗口内来回跳一次的状态不会被误判为网络变化。
- 按情况处理：

  | 情况 | 处理 |
  |---|---|
  | `available` 和 handle 都没变 | 什么都不做。如果之前处于“同网受限”，则唤醒等待中的重试 |
  | 同一 handle 上失去 VALIDATED | 不拆连接，只记 `networkValidationLost` 计数 |
  | 在两个可用网络之间切换 | 对直连远端、支持就地重连的 handle 调用 `reconnectTransport()`（STCP 且 `autoRecover`，15s 超时），其余重建。就地重连节点的下游，由已有的 epoch 机制重建 |
  | 其他情况 | 保持原逻辑 |

- 测试：
  - `networkSwitchReDialsCapableTransportsInPlaceAndRebuildsTheRest`
  - `validationFlapOnTheSameNetworkKeepsTunnelsRunning`

---

### 4.4 【P1】保活自适应与心跳对齐

**问题**：SSH 和 yamux 都是固定 30s 心跳。N 条隧道相位不同，射频反复被激活。

**方案**：

1. **分档调整心跳间隔**
   - SSH：用户配置的 `keepAliveSeconds` 作为 `VISIBLE` 档的取值。进入后台或熄屏时，在运行中调大到 `max(用户值, 自适应值)`。JSch 的 `Session.setServerAliveInterval()` 在已连接状态下会同步更新 socket 的 `SO_TIMEOUT`，需要在 2.28.3 上验证。建议同时把 `ServerAliveCountMax` 设为 3，避免一次丢包就断开。
   - yamux：把 `YamuxConfig.keepAliveIntervalMillis` 改成可以在运行中读取的 provider，`runKeepAlive()` 每一轮重新读取。后台取 60–120s。

2. **智能心跳，防止 NAT 回收**
   - 心跳太长会被运营商或路由器的 NAT 回收，换来代价更高的重连，所以不能简单地拉长。
   - 做法：后台从 60s 起逐级上探（60、90、120、180s……，**上限建议 4–5 分钟**）。一旦出现“空闲期间断线”，就回退一档，并按“网络类型（加上 Wi-Fi 的 networkHandle）”记住该网络的安全值。
   - 新增设置项“后台保活策略：自适应（默认）/ 跟随配置值”，给有特殊需求的用户留一个口子。

3. **多隧道心跳对齐**：在 `tunnel-engine` 里新增 `HeartbeatCoordinator`，按统一的时间窗（周期的整数倍）一起触发所有会话的保活，让多条隧道的心跳落在同一个射频激活窗口内。这需要各后端暴露主动保活接口（现有公开 API：JSch `Session.sendKeepAliveMsg()`、`YamuxClientSession.ping()`），并关闭各自内部的保活定时器，只保留超时判定。这项改动最大，可以排在最后。

4. **服务端也要配合**：服务端发出的心跳同样会唤醒手机，见 §5。

**实现说明（阶段三，已完成）**：

- **`HeartbeatCoordinator`**（`tunnel-engine/.../HeartbeatCoordinator.kt`）统一决定所有隧道的保活时机：
  - 前台，或选了“实时”档：使用配置值。
  - 其他情况：沿阶梯 60、90、120、180、240s 自适应。按网络（`networkKey`，即 networkHandle）分别记忆，只保存在内存里，最多记 8 个网络。
  - 后台空闲时，连续 4 次保活得到响应就升一档（省电档从 90s 起步，2 次就升）。空闲时一次保活没有响应，就把该网络的上限定为“失败档位减一”，以后不再试探这一档。
  - 永远不会短于用户配置的间隔。
- **对齐**：每次保活都对齐到“间隔整数倍”的墙钟时刻，所以多条 SSH 和 STCP 隧道会同时发出保活。界面变为可见时立即补发一次，相当于打开应用时做一次存活检查。
- **SSH**：新增 `SshHeartbeat`。
  - 由协调器驱动 `Session.sendKeepAliveMsg()`。socket factory 在读流上计数，用来判断有没有收到响应：响应超时取配置值，限制在 5–30s。超时就断开会话，交给 runner 恢复。
  - JSch 自带的保活改作兜底：`serverAliveInterval` 设为“间隔 + 响应超时 + 5s”，只有在驱动停止工作时才会触发。已核对 2.28.3 源码：已连接时调用 `setServerAliveInterval` 会立即 `setSoTimeout`，从下一次读开始生效。
  - 没有把 `ServerAliveCountMax` 改为 3。响应超时由驱动自己判定，JSch 的计数只作兜底。
- **yamux**：库里新增可选接口 `FrpcKeepAliveScheduler`，通过 `KotlinFrpcStcpVisitorClient(context, gate, scheduler)` 注入，并经 `YamuxConfig.keepAliveScheduler` 替换 `runKeepAlive()` 里的固定 `delay`。ping 的结果会回报给协调器。不注入时，默认行为与原来一致。
- **设置项**：方案里的“后台保活策略：自适应 / 跟随配置值”合并进 §4.8 的“省电档位”。“实时”档就是跟随配置值。

---

### 4.5 【P1】前台通知刷新节流

**问题**：STCP 重连时 `ready` 计数来回跳，每轮至少两次 `notify()`。部分 ROM 会把高频的通知刷新计入“刷新”次数。

**方案**：

- **熄屏时不刷新**。没有人能看到通知，等 `SCREEN_ON` 时再补一次最新状态。
- 后台亮屏时，两次刷新**至少间隔 10s**，以最新的值为准。持续时间很短的 `Reconnecting`（不到 10s 就恢复）不会体现在通知上。
- 加上 `setOnlyAlertOnce(true)`。渠道已经是 LOW，这一项只是保险。

```kotlin
notificationJob = scope.launch(Dispatchers.Main.immediate) {
    combine(
        stateStore.states.map(TunnelNotificationModel::from).distinctUntilChanged(),
        policy.interactive,
    ) { model, screenOn -> model.takeIf { screenOn } }
        .filterNotNull()
        .conflate()
        .collect { model ->
            if (model == lastNotificationModel) return@collect
            lastNotificationModel = model
            notificationManager.notify(NotificationId, notification(model))
            container.workPolicy.counters.increment("notificationUpdates")
            if (!policy.visible.value) delay(NotificationMinIntervalMillis)   // 10_000
        }
}
```

---

### 4.6 【P1】后台降低事件日志和指标的频率

1. **事件日志**（`SshRuntimeRunner.record`）：
   - 后台时，同一个 `(configId, code)` 每分钟最多写库 1 条，被抑制的次数计入 `workCounters["suppressedEvents"]`。
   - `ERROR` 级别和终态事件（`FAILED`、`STOPPED`）立即写入。
   - 也可以先放在内存缓冲里，每 5 分钟或回到前台时在一个 Room 事务中批量插入。

2. **SSH 指标**（`SshMetricsTracker`）：界面不可见时不再定时发布，只在 `visible` 变为 true 或隧道停止时发布一次。字节计数一直保存在原子变量里，不会丢。

3. **`HomeViewModel.init`**：这里的 `combine(...).collect` 常驻在 `viewModelScope` 中。可以在 `visible == false` 时用 `flatMapLatest` 切到 `emptyFlow()`。成本很低，可以顺手改。

**实现说明（阶段二）**：

- 第 1 项已完成，没有做内存缓冲批量写入。`ERROR` 级别和 `FAILED`、`STOPPED` 事件始终写入。
- 第 2 项已完成：
  - 后台**完全没有指标定时器**，回到前台或会话关闭时补发一次。
  - 前台每秒合并发布一次。
  - 连接数变化仍然立即发布。
- 第 3 项**没有做**，原因有两点：
  - 这个收集器只在状态变化时运行，没有定时器，实际节省的开销可以忽略。
  - 现有 `HomeViewModelPowerTest` 要求界面隐藏期间隧道重启后，过期的协议标记也要被清除；改成切到 `emptyFlow()` 会破坏这一点。

---

### 4.7 【P1】端口占用重试设置上限

`VISITOR_BIND_RETRY_DELAY_MILLIS = 10s`，而且没有次数上限。改为：

- 指数退避：10s、20s、40s……最长 5 分钟。
- 后台连续失败 N 次后转为 `Failed(PORT_IN_USE)`，等待用户操作。界面回到前台时自动再试一次。

**实现说明（阶段二，已完成）**：

- 第一条已实现：第一次仍然等 10s（所以现有测试不受影响），之后翻倍，最长 5 分钟；绑定成功后重新从 10s 开始。
- 第二条不需要在库里实现：引擎把 visitor 的 `failed` 状态视为连接丢失，随后由 runner 按 §4.2 的档位退避和休眠处理；启动阶段的端口冲突同样由 runner 按档位重试。

---

### 4.8 【P2】可选省电模式：空闲时断开远端，按需重连

这是最彻底的省电手段，空闲时网络唤醒约为 0。默认关闭，按配置单独开启。

- **SSH**：本地 `ServerSocket` 一直监听，但如果“没有活动连接、界面不可见”已持续 N 分钟（默认 5 分钟），就断开 SSH 会话。新连接 `accept` 时再重建会话，首个连接会多出约 1–3s 的延迟。状态显示为 `Ready(Standby)`。
  - 需要重构：`LoopbackForwarder` 的生命周期要和 `Session` 解耦，监听器归 handle 所有，会话可以随时替换。
- **STCP**：本地监听在 visitor 库内部，需要库支持“按需建立控制连接”。改动较大，放到后续版本。
- 设置项可以合并进一个“省电档位”：

  | 档位 | 心跳 | 后台重连 | 空闲断开 |
  |---|---|---|---|
  | 实时 | 跟随配置值 | `BACKGROUND` 档 | 关 |
  | 均衡（默认） | 自适应 | §4.2 默认参数 | 关 |
  | 省电 | 自适应，上探更激进 | 更早进入休眠 | 开（5 分钟） |

**实现说明（阶段三，已完成 SSH 部分）**：

- 设置 → 后台运行 → 省电档位：实时 / 均衡（默认）/ 省电，保存在 DataStore（`power_profile`）。服务启动后同步到 `RuntimeWorkPolicy.profile`。
  - 实时：熄屏时按 `BACKGROUND` 档退避，保活跟随配置值。
  - 省电：休眠阈值减半；保活从 90s 起步，2 次成功就升一档；SSH 空闲断开。
- **SSH 待机**：`JschSessionHandle` 改成可以替换会话。它记录首个会话的配置（主机密钥库、算法配置、保活、超时、认证方式），不保存任何秘密。
  - 进入待机的条件：省电档、界面不可见、没有活动连接，持续 5 分钟。进入后断开 SSH 会话，并清除 JSch 中的身份。本地端口继续监听，也不再有保活。
  - 有客户端连上来时，在该连接的转发线程里重新读取凭据，用 runner 传入的加载函数从加密存储读取，用完即清零，然后重放配置、建立新会话，再转发这条连接。
  - 重连失败时，按“连接丢失”交给 runner，由正常启动流程映射错误，并套用 §4.2 的档位和休眠。
  - 状态显示为 `Ready(standby = true)`，首页显示“已就绪 · 待机”。
  - 没有单独做依赖树例外：下游隧道的长连接本身就是活动连接，所以上游不会进入待机。
- STCP 部分仍按原计划推迟，需要库支持按需建立控制连接。

---

### 4.9 【P2】厂商白名单引导（兜底）

继续遵守原设计 §13.7 的“**不默认申请忽略电池优化**”。只在确实需要时引导用户：

1. **触发时机**：API 30 及以上用 `ActivityManager.getHistoricalProcessExitReasons()` 检查上次进程的退出原因。如果不是用户主动结束（例如 `REASON_OTHER`、`REASON_EXCESSIVE_RESOURCE_USAGE`、`REASON_SIGNALED`），且当时存在 desired roots，就在下次打开应用时提示“系统可能限制了后台运行”。各 ROM 杀进程时报告的原因码不统一，需要真机确认。
2. **引导页**：给出常见厂商设置项的文字说明，再提供“打开应用详情”按钮（`Settings.ACTION_APPLICATION_DETAILS_SETTINGS`）作为通用入口。厂商私有的 Activity 组件名经常随版本变化，**不要硬编码**。如果确实要尝试，必须 `try/catch`，失败时退回通用入口。
3. 常见的设置项名称如下，**具体名称和路径以实际系统版本为准**：

   | 系统 | 常见设置项 |
   |---|---|
   | ColorOS / realme UI / OxygenOS | 电池 → 应用耗电管理 → 允许后台行为、允许自启动 |
   | OriginOS / Funtouch OS | 电池 → 后台耗电管理 → 允许后台高耗电 |
   | HyperOS / MIUI | 应用设置 → 省电策略 → 无限制，以及自启动 |
   | MagicOS / HarmonyOS / EMUI | 应用启动管理 → 手动管理 → 允许后台活动 |
   | 通用 | 在多任务界面锁定应用卡片 |

**实现说明（阶段三，已完成）**：

- 见 `app/.../feature/background/BackgroundRestriction.kt`。
- 只看上一次进程的退出原因。当原因是 `REASON_OTHER`、`REASON_SIGNALED`、`REASON_EXCESSIVE_RESOURCE_USAGE` 或 `REASON_LOW_MEMORY`，而且当时存在 desired roots 时，下次打开应用会弹窗提示一次。`REASON_LOW_MEMORY` 是在方案基础上加的，因为不少 ROM 会把后台清理报告成这个原因。
- 已提示过的退出时间戳记在 DataStore（`acknowledged_exit_timestamp`），同一次退出不会重复提示。
- 设置页的“后台运行”部分新增了厂商设置说明，以及“打开应用设置”通用入口（`ACTION_APPLICATION_DETAILS_SETTINGS`），没有硬编码任何厂商组件名。
- Android 11 以下系统没有这个 API，不提示。

---

### 4.10 不建议的做法

| 做法 | 不建议的原因 |
|---|---|
| 持有永久 `PARTIAL_WAKE_LOCK` 或 `WifiLock` | 直接增加耗电，更容易被判定为“持续唤醒” |
| 双进程互拉、无声音频、1 像素 Activity | 违反应用商店政策，ROM 有专门的识别和惩罚 |
| 把前台服务类型伪装成 `mediaPlayback`、`location` 等 | 违反 Android 14 及以上版本的 FGS 类型规则和商店政策。当前的 `specialUse` 是正确选择 |
| 缩短心跳来“保证连接” | 提高唤醒频率，与本次目标相反 |
| 用 AlarmManager 定时拉活或重试 | 新增唤醒源，并且会被 ROM 统计 |

---

## 5. 服务端配置建议（写入用户帮助文档）

> 已写入用户文档《`doc/PortWeave_后台运行与服务端配置指南.md`》，内容包括省电档位说明、服务端配置、厂商设置和诊断计数器。

服务端主动发出的心跳也会唤醒手机的射频和 CPU，客户端单方面降频不够。

| 服务端 | 配置项 | 建议 |
|---|---|---|
| OpenSSH `sshd_config` | `ClientAliveInterval` / `ClientAliveCountMax` | 设为 `0`（关闭，依靠客户端保活），或设为 ≥120s，并把 `CountMax` 设为 3 |
| frps | `transport.tcpMuxKeepaliveInterval`（以所用 frp 版本的文档为准） | 默认 30s，可调到 60–120s，并确认 NAT 不会更早回收 |
| frps | `transport.heartbeatTimeout` | 客户端目前**没有启用 frp 层的 Ping**（`heartbeatIntervalMillis = -1`），只依靠 yamux 保活。如果服务端显式开启了心跳超时，需要确认这种组合可以兼容 |

---

## 6. 验证方案

### 6.1 应用内计数器

利用现有的 `RuntimeWorkCounters`，诊断包导出时会附带这些计数，并补充几个新计数：

| 计数器 | 说明 |
|---|---|
| `sshStateChecks` | 断线检测次数（已有） |
| `sshMetricsPublications`、`notificationUpdates`、`recoveries`、`backendStarts` | 已有 |
| `dormantRetryWaits` | **新增（阶段二）**：进入休眠等待的次数 |
| `transportReconnectWaits` | **新增（阶段二）**：STCP 控制连接在后台按档位等待重连的次数 |
| `stcpTransportReconnects` | **新增（阶段二）**：换网时 STCP 就地重连传输层的次数 |
| `networkValidationLost` | **新增（阶段二）**：同一网络失去 VALIDATED、但没有拆隧道的次数 |
| `suppressedEvents` | **新增（阶段二）**：后台被抑制、没有写库的日志条数 |
| `sshKeepAlivesSent`、`sshKeepAliveTimeouts` | **新增（阶段三）**：SSH 驱动发出的保活次数、未收到响应的次数 |
| `stcpKeepAlivesAnswered`、`stcpKeepAliveTimeouts` | **新增（阶段三）**：yamux 保活得到响应、超时的次数 |
| `heartbeatPromotions`、`heartbeatDemotions` | **新增（阶段三）**：自适应保活升档、降档的次数 |
| `sshStandbyEntries`、`sshStandbyResumes` | **新增（阶段三）**：SSH 进入待机、按需重连的次数 |

### 6.2 系统工具

```bash
# 模拟拔电，并清零统计
adb shell dumpsys battery unplug
adb shell dumpsys batterystats --reset

# 按场景运行 N 小时后采集
adb shell dumpsys batterystats dev.portweave.app > batterystats.txt
adb bugreport bugreport.zip           # 导入 Battery Historian，查看 CPU、射频、唤醒时间线

# 模拟 Doze
adb shell dumpsys deviceidle force-idle
adb shell dumpsys deviceidle unforce && adb shell dumpsys battery reset
```

- **Perfetto**：开启 `sched` 和 `power` 数据源，按线程名统计唤醒次数。重点关注 `DefaultDispatcher-worker-*`、`portweave-ssh-forward`、JSch 的 `Connect thread *`、`FrpControlBlockingIo`、`FrpControlBudgetTimer`。
- `batterystats` 里重点看：后台 CPU 时间（user + system）、移动射频激活时长与次数、`Wakeup alarms`（应当为 0）。

### 6.3 场景矩阵与目标

| 场景 | 现状（按代码估算） | 目标 |
|---|---|---|
| S1 亮屏、应用在后台、1 条 SSH 隧道空闲 30 分钟 | 检测唤醒约 7,200 次/小时 | ≤60 次/小时 |
| S2 熄屏、SSH 隧道上保持 1 条长连接 2 小时 | 约 7,200 次/小时 | ≤60 次/小时（事件驱动后接近 0） |
| S3 熄屏、服务端不可达 2 小时（SSH + STCP 各 1 条） | 约 300 次握手/小时 | 首小时 ≤15 次，之后进入休眠，约 0 |
| S4 Wi-Fi 与蜂窝之间反复切换 10 次 | 每次所有隧道整树重建 | 每次每条隧道最多重建 1 次，STCP 本地端口不重绑 |
| S5 Doze（`force-idle`） | 到期定时器集中执行 | Doze 期间没有应用侧的重试 |
| S6 连接每 70s 被 NAT 回收 | 退避一直重置，1–2s 内反复重连 | 退避正常增长，自适应心跳收敛到安全值 |
| S7 **真机长测**：ColorOS、OriginOS、HyperOS、MagicOS、HarmonyOS 各 1 台，后台亮屏 4 小时，熄屏过夜 8 小时 | 出现提示 | 不出现提示，或提示时间明显推迟。记录 ROM 版本号 |

S7 是最终验收标准。S1–S6 是可以在 CI 或本地重复执行的代理指标。

---

## 7. 实施计划

| 阶段 | 内容 | 改动量 | 涉及模块 |
|---|---|---|---|
| 阶段一：止血（**代码已完成，编译与单测通过，待真机验证**） | §4.1-A 检测频率改为按可见性<br>§4.5 熄屏时不刷新通知，后台最短间隔 10s，加 `setOnlyAlertOnce`<br>§4.2-4 稳定判定改为 5 分钟 | 小 | `tunnel-engine`、`app` |
| 阶段二：主体（**代码已完成，编译与单测通过，待真机验证**） | §4.0 运行档位<br>§4.1-B 事件驱动的断线检测<br>§4.2 分档重连与休眠<br>§4.3 网络去抖<br>§4.6 日志与指标降频<br>§4.7 端口重试上限 | 中 | `tunnel-engine`、`app`、`frpc-stcp-visitor`（只加注入点） |
| 阶段三：深度优化（**代码已完成，编译与单测通过，待真机验证**；§4.8 的 STCP 部分推迟） | §4.4 自适应心跳和 `HeartbeatCoordinator`<br>§4.8 空闲断开<br>§4.9 白名单引导<br>§5 帮助文档 | 大 | 全部模块 |

每个阶段必须通过的回归测试：

- 现有单元测试和契约测试全部通过：`SshRuntimeRunnerTest`、`TunnelOrchestratorTest`、`SshTunnelBackendTest`、`FrpcStcpBackendTest`、`FrpControlClientTest`、`YamuxConcurrencyTest`。`frpc-stcp-visitor` 不注入策略时，默认行为必须与现在一致，保证 interop 测试不受影响。
- 新增测试：
  - 档位切换会提前结束退避等待
  - 进入休眠后，只有触发器能唤醒
  - 网络去抖：同一 handle 的 VALIDATED 抖动不会触发重建
  - SSH 服务端断开后能即时感知
  - 熄屏期间通知没有更新
- §6.3 的 S1–S6 用计数器加 `batterystats` 对比优化前后的数据，作为发版证据，归档到 `doc/release/evidence/`。

---

## 8. 风险与取舍

| 风险 | 缓解措施 |
|---|---|
| 后台检测降频后，断线发现变慢 | §4.1-B 改为事件驱动后反而更快。阶段一的 30s 兜底只是过渡 |
| 心跳拉长后被 NAT 回收，触发更贵的重连 | 智能心跳逐级上探，断线即回退，按网络记住安全值，上限 4–5 分钟 |
| 休眠导致用户切回应用时发现没有连上 | 回到前台立即重试。通知文案明确写“等待网络或回到前台后重试”，用户也可以随时点“立即重试” |
| 修改 `frpc-stcp-visitor` 可能破坏与 Go 版 frpc 的行为一致性 | 只加可选注入点，默认值保持不变，interop 测试继续用默认配置 |
| 自定义 JSch `SocketFactory` 会改变连接超时和路由行为 | 已自行实现超时，与 `connectTimeoutSeconds` 对齐。建连失败的原始异常仍保留在 cause 中，错误映射不变；已有集成测试全部通过，并新增服务端断开的集成测试 |
| ROM 检测规则不公开，可能仍然触发提示 | §4.9 白名单引导兜底，并在 S7 真机长测中持续回归 |

---

## 9. 后续文档同步

方案落地后，同步更新《开发设计文档》：

- §13.6：补充去抖、按 networkHandle 区分换网，以及 STCP 走轻量的传输层替换。
- §13.7：补充运行档位、休眠等待、通知节流、自适应心跳和“不做的事”清单。
- §21（测试）：补充 §6.3 的场景矩阵和量化目标。
