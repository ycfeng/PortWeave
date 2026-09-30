# 阶段 F 构建记录

- 候选版本：0.1.0（versionCode 1）
- 记录日期：2026-08-28（Asia/Shanghai）
- 工作区：`E:\project\PortWeave_Android`
- 工具链：JDK 21 / Gradle 9.6.1 / AGP 9.2.1 / compileSdk 36
- 发布后端：纯 Kotlin STCP，无 GoMobile/NDK

## 主机侧结果

执行结果：

- `releaseQualityCheck`：通过（含单测、release lint、instrumentation APK 编译和三个 ABI 自动包装检查）
- JVM 测试：37 个 suite、457 项测试，0 failure、0 error、0 skipped
- release lint：0 fatal、0 error；18 个非阻断 warning（版本更新提示及 frp 可选未固定证书兼容模式）
- 两个不同且初始为空的隔离 build root，均使用 `--no-build-cache --no-parallel --no-daemon`：三个 ABI 哈希分别一致
- arm64-v8a：SHA-256 `E7F815BEEEC7C30E2B302949363B8392FC5121CF2F415AA134B5B5BDDC3CB477`
- armeabi-v7a：SHA-256 `FECBEDA7F63C4312C99777B98FC781BE3AB9A569F911EE9EB69418DA2175E538`
- x86_64：SHA-256 `50C6573671B67741489D4D8BB398A51ED899B9DFBF77A4A0A98F2E9CE07A2A98`
- 包装检查：每个 ABI APK 均含 `assets/licenses.txt`（离线法律包）；不含 GoMobile、`libgojni.so` 或应用自有 NDK 制品
- 正式 `assembleRelease`：签名门禁通过，三 ABI 构建成功；`apksigner verify` 对三个 APK 均通过（APK Signature Scheme v2）
- 正式 arm64-v8a APK：SHA-256 `F607D434D9B8137717B83761DD158876D163EF853F457F94F43AC6ED1F7F2089`
- 正式 armeabi-v7a APK：SHA-256 `5A82D26842241C805F085ECC40224519FCDCB523CC4DE5B791164C4FA01FC782`
- 正式 x86_64 APK：SHA-256 `0890AB49ED6C1563C693D60DBF5B6504D15416D1FDF5965113E5DBA319090870`
- 发布证书 SHA-256：`967E41C61956FDF11DE991706474A5DCB8989022E367BC80C5019B0DBDDD67F3`；证书身份及离线备份仍须由发布负责人确认
- 依赖链 1,000 次启动/停止：每轮结束均无活动会话
- SSH：100 个并发真实本地 Socket 经嵌入式 SSH 服务完成 Echo；停止后本地端口可立即重绑
- STCP：100 个并发 relay 跨两个 session 运行并清零，第 101 个按容量保护拒绝；另有 100 个真实本地 Socket 同时接入监听器、关闭后端口可立即重绑
- API 36 / x86_64 / 16 KB page-size 模拟器：3 项 app instrumentation 通过，覆盖 200% 字体核心操作、失败启动后的前台通知清理，以及实际首页/详情/日志/设置/编辑器导航
- API 36 TalkBack 自动语义证据：配置卡的配置名、后端类型、“未运行”状态描述、“查看详情”和“启动映射”自定义操作均通过 Compose semantics 断言；这不替代人工听读
- API 36 UI 维度：原生 1080×2280 / 440 dpi 竖屏在系统字体 2.0 下通过；2560×1600 / 240 dpi（宽度大于 840 dp）横屏分支通过；测试后显示设置已恢复
- API 36：Room schema 1、2、3 到当前 schema 4 的 3 项设备迁移测试通过
- API 36 最终候选 debug：冷启动 `TotalTime=2014 ms`；启动后空闲基线 `PSS=125226 KB`、33 threads、6 FD；未发现 PortWeave 前台通知残留
- API 36 正式 x86_64 APK：全新安装成功，冷启动 `TotalTime=934 ms`，版本 `0.1.0` / versionCode 1 / targetSdk 36；随后同签名覆盖安装成功
- API 36 正式候选空闲基线：`PSS=41565 KB`、29 threads、0 条活动 PortWeave `NotificationRecord`；release 包不可调试，普通 shell 无权读取其 `/proc/<pid>/fd`，因此未记录伪造的 FD 数值
- API 36 正式候选浅色与深色首页完成目视检查，截图见 `evidence/api36-release-light.png` 与 `evidence/api36-release-dark.png`；检查后系统主题恢复为自动
- 按用户限定，本轮只验证 API 36；未安装或运行其他 API 版本

## 未由主机侧验证覆盖

TalkBack 人工听读、含配置和错误状态的完整主题视觉走查、真机后台行为、Doze/厂商耗电策略、8 小时映射 soak、真实 frps 服务端数据面、发布证书备份与商店审核必须由发布负责人按检查清单补充证据。主机侧 100 客户端测试和 API 36 模拟器结果不能表述为真机、真实 frps 或长时间稳定性证据。在这些记录完成前，不应宣称候选版本满足全部设备级发布门禁。
