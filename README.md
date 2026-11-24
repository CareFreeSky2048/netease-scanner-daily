# 网易云扫描器服务器版

## GitHub Actions 部署与运维指南（替代 VPS）

本仓库支持直接通过 **GitHub Actions** 运行每日自动扫描，并将最新缓存、去重 CSV 与精简后的 SQLite 历史持久化保存在私有 GitHub Release（tag: `scanner-state`）中；同时在每次扫描后构建静态网页并推送到单提交 orphan 分支 `site`，且生成可离线打开的单文件报告 `latest-report.html`。

### 1. 创建私有 GitHub 仓库并推送代码

1. 在 GitHub 创建一个 **Private（私有）** 仓库（例如 `netease-scanner`），保证扫描记录、缓存和配置不会对外公开。
2. 在本地 `netease_server` 目录关联远程仓库并推送 `main` 分支：

```powershell
cd C:\Users\32461\Music\netease_server
git remote add origin https://github.com/<你的用户名>/<你的仓库名>.git
git push -u origin main
```

### 2. 配置 Secrets 与 Variables

打开 GitHub 仓库的 **Settings → Secrets and variables → Actions**：

- **Repository secrets（必填）**：
  - `MUSIC_COOKIE`：填入网易云音乐大号 Cookie（支持纯 `MUSIC_U` 值或完整 `MUSIC_U=...;`；切勿写入仓库文件或日志）。
- **Repository variables（可选）**：
  - `SCAN_THREADS`：默认并发线程数，建议设为 `8`（未设置时工作流默认 `8`）。
  - `ARTIST_CACHE_KEY`：若已有大号关注列表缓存指纹，可填入 16 位十六进制指纹（例如 `92c430701d65a19f`），在实时拉取失败时作为回退缓存。

### 3. 导入已有缓存与历史到 Release `scanner-state`

迁移前请先通过本地离线打包好的目录（例如 `vps_backup_YYYYMMDD/github_state/` 下的 `scanner-state-*.tar.gz`、`*.sha256`、`state-manifest.json`）初始化持久状态，确保延续现有专辑缓存和 CSV 去重记录：

**方式 A：命令行一键上传（推荐）**

1. 在 GitHub 生成一个仅勾选该私有仓库 `Contents: Read and write` 权限的 Fine-grained Personal Access Token。
2. 在 PowerShell 执行：

```powershell
cd C:\Users\32461\Music\netease_server
$env:GITHUB_TOKEN = "<你的_PAT>"
python state_store.py bootstrap-upload --repo <你的用户名>/<你的仓库名> --pack-dir .\vps_backup_20261124\github_state
Remove-Item Env:GITHUB_TOKEN
```

**方式 B：GitHub 网页手动创建 Release**

1. 打开仓库右侧 **Releases → Draft a new release**。
2. Tag 填写 `scanner-state`（Target 选 `main`），标题可填 `Scanner Persistent State`，勾选 `Set as a pre-release`。
3. 将 `github_state/` 内的 `scanner-state-*.tar.gz`、`scanner-state-*.tar.gz.sha256` 和 `state-manifest.json` 三个文件拖入附件区并点击 **Publish release**。

> 安全保护：`restore` 步骤默认严格校验 Release 是否存在有效归档；若首次从零启动且无历史缓存，需手动在工作流触发界面勾选 `allow_empty_state`。

### 4. 接口探测、试运行与停用旧 VPS

1. 打开 GitHub 仓库 **Actions → Netease Music Scanner → Run workflow**。
2. 先选择 `action = probe` 运行一次接口探测，确认 GitHub Actions 节点连通网易云接口且 `MUSIC_COOKIE` 有效。
3. 再选择 `action = scan`、`scan_mode = auto` 手动试跑一次全量扫描，核对 **Job Summary**、下载 Artifacts 中的 `latest-report.html` / `today.csv` / `records-export.zip`。
   - **隔离试跑模式（`action = test_fresh_day`）**：如需模拟“假设今天一次都没扫描过”的状态（仅回滚临时工作区中今日的任务、命中、CSV 当日去重行与今日专辑缓存，完整保留昨日及更早历史与艺人缓存），可在 **Run workflow** 选择 `action = test_fresh_day`。结果仅输出到 **Job Summary** 与 Artifacts，**绝不会**写回生产 Release（`scanner-state`）或覆盖 `site` 分支。
4. 确认新链路连续正常后，登录旧 VPS 停止原扫描服务（避免双端同时写入或重复触发）：

```bash
cd /opt/netease-scanner
docker compose -f compose.tunnel.yaml stop scanner
```

### 5. 私有查看结果与同步本地电脑

- **Actions Job Summary**：每次运行结束后直接在工作流页面查看状态、耗时、错误告警与全部新增专辑（按热度排序，展示命中艺人，点击专辑标题直达网易云）。
- **离线单文件报告 & 记录包**：每次运行在 Artifacts 保留 30 天，包含 `latest-report.html`（双端自适应，双击即可离线搜索/筛热度/导出 CSV）、`today.csv`、`run.csv`、`m3-report.csv`（M3 模式）及桌面兼容回传包 `records-export.zip`。
- **私有托管静态网页（推荐 Cloudflare Pages + Access）**：工作流每次扫描后会将精简静态页面强制单提交推送到 `site` 分支（无历史膨胀）。在 Cloudflare Pages 绑定该私有仓库的 `site` 分支（构建命令留空，输出目录填 `.`），并开启 **Cloudflare Access** 登录保护，即可用手机和电脑随时安全访问全部按日结果与任务历史。

### 6. 调度策略与 GitHub Actions 免费额度说明

- 默认工作流 `.github/workflows/scan.yml` 每天于北京时间 **00:03**（UTC `3 16 * * *`）与 **04:00**（UTC `0 20 * * *`）自动执行，并发组锁定保证同一时刻只有一个扫描读写持久状态；支持通过 `workflow_dispatch` 手动选择 `auto` / `regular` / `m3` / `m3_intersection`、日期窗口、并发线程或针对异常任务的 `retry_of`。
- **免费额度提示**：GitHub Free 账户对**私有仓库**每月提供 **2,000 分钟** Linux runner 免费额度。按 `SCAN_THREADS=8`、单次全量扫描约 30 分钟估算，每天 2 次约消耗 **1,800 分钟/月**（接近上限）；如需留出更大余量，可将变量 `SCAN_THREADS` 调高至 `10–12`，或在 `.github/workflows/scan.yml` 中暂时注释掉 `04:00` 定时条目改为每天 1 次。

---

## 原 VPS 部署与功能说明（历史参考）

每天北京时间 **00:03 和 04:00** 各扫描一次；截至 **2026-11-25（含）**，白天 **12:00–16:30 每隔 15 分钟**使用“仅 M3 交集”模式扫描一次，共 19 个时段。凌晨任务继续遵循原自动模式，2026-11-26 起恢复常规扫描，白天临时时段停止创建任务。保留手动扫描、暂停、继续、停止和并发调整。网页默认显示今日扫描新增，另可查看每次任务历史及原 CSV 的累计结果。今日视图区分尚未扫描、等待、扫描中、完成无新增，以及失败／部分完成／中断；按任务所属日期汇总，不按歌曲发行日期筛选。原始桌面脚本不被修改。

## 已部署 VPS：更新本轮策略

首批白天 M3 交集任务于北京时间 2026-10-26 17:22 应用到现有 VPS；2026-10-27 14:11 已将白天时段扩展为每 15 分钟一次，14:15:00 首次新增任务已自然触发。今天已过去的新增时点从次日生效，明天起完整覆盖 19 个白天时段。当前默认 4 线程、上限 8，自动扫描已开启；部署记录见 VALIDATION.md。

服务器引擎静默排除已确认不存在的专辑 `141201113`，它不会进入详情请求、低速补扫或最终失败统计，也不显示专门跳过提示。历史任务的失败记录不回写。白天所有临时时段固定使用 `m3_intersection`，不执行其余关注艺人的第二轮；外部合辑与新歌的默认起始日期均为 2026-10-24，仅适用于交集模式，其他模式的日期窗口不受影响。手动交集模式在 11 月 26 日以后仍可使用。白天每 15 分钟会创建一个独立任务；若前一轮尚未结束，任务按现有单进程队列等待，结果按当日归并。

在电脑 PowerShell 上传以下三个文件：

```powershell
cd C:\Users\32461\Music\netease_server
scp -P <SSH_PORT> netease-server-code.zip netease-server-code.zip.sha256 update_m3_schedule.py root@<VPS_IP>:/opt/netease-scanner/
```

等待当前扫描及排队任务结束，然后在 VPS 登录终端执行；任一步报错时停止，不继续下一步：

```bash
cd /opt/netease-scanner
sha256sum -c netease-server-code.zip.sha256
python3 update_m3_schedule.py --check
python3 update_m3_schedule.py --apply
```

`--check` 只检查并显示拟采用的时段；`--apply` 自动备份源码、配置、数据库及旧镜像，校验并构建代码包中的三个运行文件，再更新配置和启动服务，最后输出 `DEPLOYMENT_OK`。无需手动解压整包，也不要用示例配置覆盖现有配置。Cookie、线程、扫描深度、调度启用状态、数据和资源限制继续使用当前设置。首次增加时，如果当天已到或已过某个新增时段，该时段从次日生效，避免部署后立即补跑；重复执行不会继续推迟。原凌晨时段仍遵循原有当天缺失任务补跑规则。构建失败在服务激活前会恢复旧源码和镜像；新服务启动后的校验异常不会自动打断可能已运行的任务，应按输出的备份位置检查。

## 目前没有域名：使用 SSH 网页入口

无需购买域名也能让 VPS 独立完成每日扫描。使用 `compose.tunnel.yaml` 时网页仅监听服务器 `127.0.0.1:18080`，通过 SSH 加密连接查看，网页仍需账号密码。这个阶段的访问地址是本机 `http://127.0.0.1:15432/`，不是对公网开放的网址；退出 SSH 后网页不可访问，但 VPS 扫描和调度继续运行。

按下一节上传并解压代码后，在 VPS 的 `/opt/netease-scanner` 内执行 `cp .env.tunnel.example .env`，按下文生成网页密码哈希并填写 `.env`，创建 `secrets/music_cookie.txt`。然后执行：

```bash
bash deploy.sh --tunnel netease-migration-20261007.tar.gz
docker compose -f compose.tunnel.yaml exec -T scanner python probe.py
```

在 Windows 另开一个终端，用原连接命令增加一个转发：

```powershell
ssh -p <SSH_PORT> -o ServerAliveInterval=30 -L 54321:127.0.0.1:54321 -L 15432:127.0.0.1:18080 root@<VPS_IP>
```

如果原来的 SSH 窗口仍占用本机 54321，新窗口只保留 `-L 15432:127.0.0.1:18080` 即可。连接后在浏览器打开 `http://127.0.0.1:15432/`，用 `.env` 中的网页用户名和你生成哈希时输入的明文密码登录。不要填写 SSH 密码。此方式不需要开放服务器 80、443 或 18080 入站端口。

完成 API 探测和网页全量手动扫描后，再启用 `config/config.json` 的 `schedule_enabled`。隧道方式下，查看、停止和重启等所有 `docker compose` 命令都要带 `-f compose.tunnel.yaml`。以后有域名时，停止隧道入口并按下面的域名方式配置 `.env` 和 `compose.yaml`，数据与任务历史可以继续使用。

### 手机查看

Android 可以使用 Termux，建议从其官方推荐渠道安装。在 Termux 中先执行 `pkg install openssh`，再执行以下连接命令，按提示输入 SSH 密码：

```bash
ssh -N -p <SSH_PORT> -o ServerAliveInterval=30 -o ExitOnForwardFailure=yes -L 15432:127.0.0.1:18080 root@<VPS_IP>
```

`-N` 只建立网页转发，连接后没有命令提示符是正常现象。保持这个会话运行，在同一手机浏览器打开 `http://127.0.0.1:15432/`，输入网页账号密码。若要在手机上执行部署命令，另开 Termux 会话执行 `ssh -p <SSH_PORT> root@<VPS_IP>`，不加 `-N`。SSH 密码与网页密码分别使用；关闭 Termux 或手机断网只会中断查看，不会停止 VPS 扫描。Android 的省电管理可能终止后台会话，必要时重新连接。

iPhone 可以使用支持本地端口转发的 SSH 客户端，例如 Termius，Android 也可使用这一类客户端。设置主机 `<VPS_IP>`、端口 `<SSH_PORT>`、用户 `root`，再设置本地端口 `15432` 转发到远端 `127.0.0.1:18080`，连接后同样通过手机浏览器访问。仅有普通 SSH 登录功能而没有本地端口转发的客户端，不能用这一方式打开网页。

## 先准备服务器

目前目标为美国 VPS `<VPS_IP>`，SSH 端口 `<SSH_PORT>`，用户 `root`。继续使用密码登录即可；原有 `54321` 转发与本项目没有关联，可以保留。需要 Linux、Docker Engine 与 Compose v2；使用公网 HTTPS 入口时另需一个指向 VPS 的域名。先检查系统、内存、磁盘，以及 80、443 是否已有服务占用；如果已有网站，需接入现有反向代理，不能直接替换其端口。隧道方式不占用这两个端口。

网页入口需要入站 80、443；保留 SSH 的 <SSH_PORT> 端口。扫描器的 5000 和网易云 API 的 3000 不对公网开放。先检查该美国 IP 上的接口可用性，不因地理位置直接判定可用或不可用。

## 上传代码与记录

本次交付的 `netease-migration-*.tar.gz` 是从音乐文件夹复制的快照，保留本地原件。`data/` 内保存当前 CSV、专辑缓存、艺人缓存和当前报告，旧记录与约 1.1 GB 的 API 日志压缩进入 `data/migration_archive/`。旁边的 `.sha256` 是包校验，`.manifest.json` 是各文件清单及艺人缓存数量。归档和 Cookie 都不通过网页提供下载。

在 Windows PowerShell 上传（下列文件名以实际生成的包为准）：

```powershell
ssh -p <SSH_PORT> root@<VPS_IP> "mkdir -p /opt/netease-scanner"
scp -P <SSH_PORT> .\netease-server-code.zip .\netease-server-code.zip.sha256 .\netease-migration-*.tar.gz .\netease-migration-*.tar.gz.sha256 root@<VPS_IP>:/opt/netease-scanner/
```

在服务器先进入 `/opt/netease-scanner/`，执行 `sha256sum -c netease-server-code.zip.sha256`，然后解压代码 ZIP 到此目录。可使用已安装的 unzip，或 Python 的 `python3 -m zipfile -e netease-server-code.zip .`。在目录中执行 `sha256sum -c netease-migration-实际日期.tar.gz.sha256`；数据包先不要手动解压，部署脚本会校验包内各文件并安全导入。

若需要重新生成快照，先停止原扫描器，再在本地执行：

```powershell
powershell -ExecutionPolicy Bypass -File .\check_local_idle.ps1
python .\prepare_migration.py --source C:\Users\32461\Music --output .\netease-migration-新日期.tar.gz --confirm-idle
```

生成时再次核对源文件是否改变；遇到变化则停止并拒绝交付这个快照。`verify_migration.py` 会验证压缩内容和落地文件的 SHA-256，并拒绝覆盖已有数据。

## 设置域名、网页密码与 Cookie

复制 `.env.example` 为 `.env`，填入域名与网页用户名。执行 `docker run --rm -it caddy:2.10.2-alpine caddy hash-password`，按提示输入网页密码，将得到的 bcrypt 哈希写入 `WEB_PASSWORD_HASH`，保留单引号以防 `$` 被解释。不要将明文网页密码填入该字段。

创建 `secrets/music_cookie.txt`，只写入当前大号 MUSIC_U 值或完整 `MUSIC_U=...;`，不要把 Cookie 写入 `.env`、代码、聊天记录或命令参数。之后更新这个文件即可，下一次扫描会重新读取；建议直接编辑该文件，不替换整个 secrets 目录。

复制 `config.example.json` 为 `config/config.json`（首次也可让部署脚本复制），保持 `schedule_enabled: false`，待接口验证与完整扫描通过后再启用。日期窗口不写死在配置中，每次由后端根据北京时间重新计算。

如果更换 Cookie 后指纹变化，服务器会尝试实时拉取关注列表。若需要在拉取失败时沿用同一大号的迁移缓存，可以从迁移清单中选择正确的 16 位缓存指纹填入 `artist_cache_key`；服务器仍优先刷新实时列表，只有刷新失败才使用指定缓存，并将该次扫描标记为降级。不能仅按列表最大数量选择，因为缓存可能属于不同账号。

## 启动与验证

在服务器目录执行：

```bash
bash deploy.sh netease-migration-实际日期.tar.gz
docker compose exec -T scanner python probe.py
```

探测只检查登录、少量关注艺人、歌曲列表、艺人专辑列表与专辑详情，不执行全量扫描，也不写入历史 CSV。通过后访问 `https://你的域名/`，使用网页账号密码登录，检查历史累计结果和迁移数据；随后进行一次完整手动扫描，查看耗时、失败统计与日志，确认旧链接不重复写入。首次服务启动会将导入的 CSV 原样保留为“迁移前累计结果”，之后的扫描不会修改这份快照；“全部累计结果”则包含服务器后续新增。

通过验证后，编辑 `config/config.json`，将 `schedule_enabled` 改为 `true`。本轮临时策略在 `schedule_times` 中加入 12:00 至 16:30（含）的每个 15 分钟时点，并通过 `schedule_modes` 固定为 `m3_intersection`，由 `schedule_end_dates` 限定至 `2026-11-25`（含）。已有配置未设置时间表时保留原 00:03 行为，更新代码本身不擅自增加任务。网页将连续高频时段压缩为时间范围显示。重启或开启后按时间段分别补跑当天生效范围内缺失的已到期任务，不补跑早于当天的任务；`schedule_start_dates` 可为新时间指定首次生效日期，避免在当天已过指定时间时立即补跑。各时段 Cron 在启动时注册，变更时间表后重启服务；30 秒检查仍会读取最新配置并负责恢复。

原扫描算法和 recent/归属验证/新专探测数量保持默认值 50/50/20，并发 5。外部合辑从前一自然月同日到当天（月末按实际天数截断），新专／新歌从昨天到未来三天。关注列表按北京时间每天刷新，失败时回退缓存并明确记录。

### M3 特供双轮模式与艺人配对

手动模式新增“仅 M3 交集（手动）”：复用下述配对规则，只扫描 M3 数据库与 MUSIC_U 关注艺人中唯一明确匹配项及用户已确认的合并组，不执行其余关注艺人第二轮。大小写、Unicode、有限标点与已有别名继续参与匹配；其他歧义项及未匹配项均排除，并在配对报告中分别标为“未纳入交集·歧义”和“未纳入交集·无匹配”。空交集明确记为失败，不回退为全部关注艺人扫描。外部合辑与新歌默认起始日均为 2026-10-24，结束日分别沿用当天与新歌默认结束日；调用方显式提供日期时仍按自选窗口扫描。此日期范围仅作用于 `m3_intersection`，常规和 M3 双轮日期窗口不变，11-26 后仍可手动选择。该模式不设为全局 `scan_mode` 默认值，但可通过 `schedule_modes` 单独指定给临时白天时段，不改变凌晨任务的自动切换与并发配置。交集模式中的热度仅统计本次交集扫描覆盖的逻辑艺人。

自动模式在北京时间 2026-11-25（含）前使用 M3 双轮，2026-11-26 起恢复常规扫描；手动选择“M3 特供双轮”不受截止日期限制。M3 模式的外部合辑起始日期默认是 2026-10-24，结束日仍按扫描任务日期计算；新专／新歌日期窗口和其余扫描参数继续使用服务器端默认值。

服务器使用随代码包提供的 1,440 条 M3 艺人记录，记录保留 M3 数据库内部艺人 ID、名称和已整理别名，与 MUSIC_U 返回的关注艺人列表逐项比对。名称比较会做 Unicode NFKC 归一化、大小写折叠及空白归一化，因此如纯大小写差异会匹配；兜底比较仅忽略空格、连接号、句点和下划线，不删除 `+`、`#`、`!`、`/` 等可能有辨识意义的符号。已有别名也参与匹配，但不会用模糊相似度自动猜测。

通常只有当规范化结果唯一对应一个 M3 数据库艺人和一个网易云关注艺人 ID 时，才进入第一轮优先扫描。2026-10-26 经用户确认的 13 个歧义组按同一逻辑艺人处理：ariiol、Eleanor Forte、Eno、Kou、Mono.、moyu、rejection、Sonance、Xeno、XH、Zekk、Maqμin 和前澤／maezawa；组内所有列出的网易云账号都会进入第一轮或交集扫描，报告仍逐个保留网易云 ID，并标明 `user_confirmed_group`。扫描计数区分逻辑艺人数与覆盖账号数，专辑热度也把同组账号合并为一个艺人。此确认仅作用于预审表列出的账号及相应 M3 名称，不会泛化到其他同名账户。`WATER` ↔ `Wa!ter` 与 `Kou!` ↔ `kou` 是不同艺人，不做标点合并。

其余同名网易云账号、多 M3 候选，或一个 M3 主名和别名命中多个未确认网易云 ID 的情况仍列为歧义，不会自动提升到第一轮。每次 M3 扫描结束后，网页提供“下载 M3 配对报告”；对应 JSON 和 CSV 接口为 `GET /api/runs/<run_id>/m3` 与 `GET /api/runs/<run_id>/m3.csv`。CSV 包含每个账号的配对及第二轮歧义和未匹配关注艺人。不要把“未匹配”理解为确认不属于 M3 数据库：它表示名称和已整理别名中没有可靠对应项。

## 调度、备份与搬迁

### 将服务器记录回传电脑

页面新增“记录回传本地”。电脑端 Chrome／Edge 通过当前 SSH 本地网址或 HTTPS 打开网页后，先关闭本地桌面扫描器，再点击“回传并覆盖本地音乐文件夹”，选择 `C:\Users\32461\Music` 并允许浏览器读写。目录必须包含原 `VA_Tracks_Output.csv`，避免误选目录。浏览器无法绕过目录授权直接写入固定路径；目录选择器通常会记住上次位置，但可能仍需要重新选择和授权。

回传包含服务器当前 `VA_Tracks_Output.csv`、`VA_Album_Date_Cache.json`、`VA_Artist_Caches/artists_*.json`、`VA_Scan_Metrics.json` 和 `VA_Scan_Tuning_Report.txt`（存在的文件）。只覆盖这些同名文件，本地其他文件和服务器未包含的旧艺人缓存保留；Cookie、程序源码、调度配置、旧版本状态、API 日志、迁移档案以及服务器 SQLite 任务历史均不回传。

生成快照时使用扫描共用锁；服务器正在扫描或有等待任务时拒绝导出，需等任务结束。快照复制完成后，服务器可以继续后续扫描，这次下载保持快照时的内容。每个文件附带 SHA-256，保持原文件名和字节编码。浏览器先下载并校验全部文件，再将将要覆盖的本地原件备份到 `Music/netease_sync_backups/时间-编号/`，验证备份后覆盖，并验证写入结果。发现本地记录发生变化时中止；覆盖中失败会尝试恢复原件，失败信息会说明恢复情况和备份位置。多文件覆盖不是整个目录的一次原子事务，因此覆盖期间请保持页面打开，并确保本地扫描器关闭。

手机、不支持目录写入的浏览器也可使用“下载记录包 ZIP”。包内有文件清单与 SHA-256；此按钮只下载，不自动覆盖。直接覆盖和下载均排除 Cookie。回传是以服务器当前记录为准的覆盖，不合并本地独立扫描的新结果；这些本地记录会留在覆盖前备份中。

### 已部署服务器如何更新按钮

先等待服务器扫描结束。在 Windows PowerShell 上传更新后的代码 ZIP 和校验文件：

```powershell
cd C:\Users\32461\Music\netease_server
scp -P <SSH_PORT> netease-server-code.zip netease-server-code.zip.sha256 root@<VPS_IP>:/opt/netease-scanner/
```

在 VPS 执行以下命令，更新代码并重建扫描器。此次更新不要重新导入迁移包，也不要重新复制 `.env`、Cookie 或配置示例到实际配置。

```bash
cd /opt/netease-scanner
sha256sum -c netease-server-code.zip.sha256
python3 -m zipfile -e netease-server-code.zip .
docker compose -f compose.tunnel.yaml build scanner
docker compose -f compose.tunnel.yaml up -d --no-deps scanner
```

刷新网页后即可看到回传按钮。此操作保留现有数据、Cookie 与调度配置；服务启动时仍遵循当天未启动任务的补跑规则。

只有一个进程运行扫描引擎，进程锁和统一任务锁避免同一数据目录重复启动。任何生效时段到期时若已有扫描，自动任务进入队列，结束后执行，保留创建时的模式与日期窗口；如果等到次日仍未执行，则标记中断，不补跑旧日期。重启时运行中的任务标为中断，当天各时间段尚未创建且在生效期内的定时任务可以补跑；失败和中断任务不会无限自动重试。四个时段共用配置的默认线程和历史 CSV 去重，每次只追加累计 CSV 中尚未记录的结果，并汇入当日列表。数据库以日期加定时时间作为唯一键，升级将旧自动任务归入 00:03，同时保留原任务编号、结果和日志。

成功、部分完成、失败、停止和中断分别记录。Cookie 过期先在登录检查阶段失败；个别艺人或专辑失败保留已找到的结果，并标明可能遗漏。扫描时无需网页保持开启；多个页面独立接收实时更新，慢连接可重新读取持久化记录。

网页单独显示定时默认并发、服务器上限，以及运行中任务的实际并发和创建时并发。手动表单与“调整运行并发”只影响本次任务，不保存为定时默认配置；定时任务在创建时保存参数快照，之后修改 config/config.json 的 threads 只影响之后创建的任务。任务历史显示创建时并发，详情显示最后记录并发，新任务日志记录启动来源和运行中手动调整。失败摘要保存最多 20 项阶段、艺人／专辑 ID 和经过凭据脱敏的错误详情，并写入任务日志；旧任务未保存的错误详情无法事后恢复。

单次扫描在每个阶段完成后，会对失败对象进行最多三轮延后低速补扫：每轮分别等待 30、120、300 秒，单线程、对象之间至少间隔 1 秒，整个任务的低速补扫累计时间预算为 30 分钟。只重试未恢复对象，不重扫成功艺人或专辑；补扫沿用原日期窗口、筛选与 CSV 去重，成功恢复后删除该对象的最终失败记录，仍失败时保持部分完成／失败。等待期间支持暂停、继续与停止，不改变任务配置的并发。任务详情显示低速补扫尝试次数和恢复数量。登录检查失败不进入此循环。

部分完成、失败、停止或中断的已结束任务，可以点击“按原窗口与模式补扫”：旧任务没有可恢复的完整候选关系快照，因此这个入口重新扫描原模式覆盖的艺人，保留原窗口与扫描深度，使用当前配置的默认并发，只追加累计 CSV 中尚未记录的结果。常规和 M3 双轮模式覆盖全部关注艺人；交集模式只重新配对并扫描当前可确认的 M3 交集，不扩大为全部关注艺人。补扫产生新任务，参数 retry_of 指向原任务；原任务及其失败状态作为历史保留。与手动／定时任务共用运行锁，不并行扫描，也不会替代或重复创建当天定时任务。

每天北京时间 03:00 后的空闲时段自动备份活动数据与 SQLite 数据库到 `data/backups/`，默认保留约 14 天。不可变的 `migration_archive/` 不重复打入每日备份，原始迁移包应另外保存。所有备份在同一 VPS，建议定期下载到另一台设备；同机备份无法覆盖整台 VPS 丢失的情况。容器日志按 10 MB × 3 文件轮转。

更换服务器前停止旧扫描器：`docker compose stop scanner`，确认旧定时任务不再触发，再复制整个 `data/`、`config/`、`secrets/` 和 `.env`（用受保护的 SSH 传输），以及代码部署文件；在新 VPS 重设权限、验证接口、启动并切换域名。只有一台 VPS 可以启用调度。

查看服务：`docker compose ps`；查看扫描器日志：`docker compose logs --tail=100 scanner`。日志可能包含历史接口响应信息，不要公开上传完整 API 日志。暂停或停止扫描使用网页按钮即可。

## 接口与本地验证

热度沿用桌面版的 album_count：同一专辑在本次扫描候选中命中的不同艺人数，3 人及以上显示火焰标记；今日汇总和单次任务均先按热度、再按发行日期排序，分页与 CSV 下载使用同一顺序。已经保存热度的服务器任务在更新后直接显示，不需要重新扫描。每日和单次任务 CSV 末尾追加热度列；原累计 CSV 仍保留六列，不回写热度。迁移前或累计 CSV 没有该字段的记录显示“—”，不以历史行数推测原扫描热度。

`POST /api/start` 返回 `run_id`；`POST /api/control` 保留暂停、继续、停止及并发调整。写请求须带 `X-Scanner-Request: 1` 与 JSON；Cookie 由服务器文件读取，请求内的 Cookie 不会被保存或采用。

`POST /api/runs/<run_id>/retry` 按原窗口与模式创建上述补扫任务并返回新 run_id；仅异常结束任务可使用，已有任务运行／排队时返回 409。客户端不能借此改写原日期窗口或移除历史去重。`POST /api/start` 的 scanMode 可使用 `m3_intersection` 手动启动交集扫描。

`POST /api/records/export` 生成闲时记录快照并返回文件清单；`GET /api/records/<export_id>/file/<path>` 读取清单内单个文件，`GET /api/records/<export_id>/zip` 下载 ZIP，`POST /api/records/<export_id>/release` 释放临时快照。临时包最多 3 个、一小时有效，限制仅作用于回传快照，不限制持久化历史。接口使用固定文件白名单，受同一网页登录保护。

`GET /api/status` 查询当前扫描和调度配置；`/api/today` 与 `/api/today/csv` 查看、下载今日任务的新增结果，默认日期由服务器按北京时间计算，也支持 `date=YYYY-MM-DD`；`GET /api/runs` 和 `/api/runs/<run_id>` 查看任务、分页结果及日志；`/api/runs/<run_id>/csv` 下载单次 CSV；`/api/legacy` 与 `/api/legacy/csv` 查看、下载全部累计 CSV；`/api/pre-migration` 与 `/api/pre-migration/csv` 查看、下载迁移时的只读 CSV。今日任务中的自动扫描按原定扫描日归属，手动扫描按任务创建的北京时间日期归属；页面跨日时自动切换今日日期。`/api/stream` 是实时广播，并不承担结果保存。页面与这些接口全部受 Caddy 登录保护。

M3 双轮任务另提供 `/api/runs/<run_id>/m3` 配对 JSON 和 `/api/runs/<run_id>/m3.csv` 配对 CSV；普通常规扫描没有该报告。报告按任务保存，后续 M3 名单变更不会改写已经完成任务的配对结果。

本地测试：安装 `requirements.txt` 与 pytest，执行 `python -m pytest tests -q`。测试使用隔离数据目录和模拟接口，不扫描真实账号，也不改写原音乐文件夹的记录。

回传文件写入逻辑另用 `node --test tests/test_record_sync.cjs` 验证，模拟浏览器目录句柄，覆盖备份、字节校验、失败恢复与本地记录变化检测；Node 仅用于开发测试，扫描服务不需要额外安装。

已完成的本地验证、迁移包核对结果、尚未完成的 VPS 验收和 API 依赖限制详见 `VALIDATION.md`。

针对 1 核 / 1 GB VPS 的资源压力恢复、swap 准备、独立运行限制和并发上限说明见 `LOW_RESOURCE.md`。低资源配置仅在明确启用 `compose.low-resource.yaml` 时生效，不会自动改变现有部署，也不能保证真实完整扫描一定适合 1 GB 内存。
