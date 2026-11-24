# 1 核 / 1 GB VPS 恢复与试运行

用户提供的监控点显示内存 891.88 / 960.99 MB（约 93%），CPU 同时突升。与 SSH 握手超时、控制台无法输入一起看，资源压力是重要嫌疑，但不能仅凭曲线确认 OOM，也不能据此完全排除链路问题。供应商 CPU 百分比可能采用不同统计口径，151.95% 不直接解释成实际拥有或使用了 1.52 核。

## 先恢复管理能力

如果 SSH 和 VNC 都无法输入，只能通过供应商管理面板恢复。可先尝试面板的正常／软重启；若无法完成再考虑强制重启。强制重启可能丢失尚未落盘的最近记录，需要在恢复后核对 CSV、缓存与任务状态。原始本地记录、迁移包和服务器已有备份可用于恢复，不能保证涵盖最后一刻的新增。

重新连接后先暂停扫描器，避免又被后台任务拖慢：

```bash
docker stop -t 40 netease-scanner-scanner-1
```

这是当前 Compose 默认容器名；若提示没有此容器，用 `docker ps -a --format '{{.Names}}'` 查看名称，不停止其他无关服务。通过 Docker 手动停止后，`unless-stopped` 不会自行再次启动这个容器。停止后网页扫描入口暂时不可用，API 和其他服务保留。

检查当前内存、交换空间、磁盘和内核日志：

```bash
free -h
swapon --show
df -h /
journalctl -k -b -1 --no-pager -n 100
docker stats --no-stream
```

上一轮内核日志若存在 `Out of memory`、`Killed process` 或 `oom-kill`，可支持内存耗尽判断；如果上一轮日志未持久化，命令可能无法提供证据。检查其他已有服务是否也占用较多内存。

## 为低内存机器准备交换空间

也可使用交付的 `setup_swap.sh`。`bash setup_swap.sh --check` 只显示内存、已启用 swap 和磁盘；停止扫描器后执行 `bash setup_swap.sh --apply` 才创建专用 1 GiB swap。脚本在已有至少 1 GiB 活动 swap 时不重复创建，拒绝覆盖同名文件，仅支持 ext4 / XFS，要求根目录至少有 2 GiB 空闲，使用互斥锁并备份 fstab 后配置开机启用。未激活前失败会清理本次新建文件，激活后配置持久化失败则保留活动 swap 并明确报错。它不修改 Cookie、扫描数据或自动启动扫描器。

如果已存在至少 1 GB 的 swap，可沿用，无需重复创建。没有 swap 时，先确认根目录有至少约 2 GB 可用空间，再执行下面的整体代码块创建专用 1 GB 交换文件。代码拒绝覆盖同名已有文件；交换空间降低瞬时内存耗尽概率，但可能增加扫描耗时和磁盘压力。

```bash
(
set -eu
test ! -e /swapfile-netease
fallocate -l 1G /swapfile-netease
chmod 600 /swapfile-netease
mkswap /swapfile-netease
swapon /swapfile-netease
printf '\n/swapfile-netease none swap sw 0 0\n' >> /etc/fstab
)
free -h
```

如果文件系统不支持这种交换文件，`swapon` 会失败且不会添加启动配置；请依据错误处理，不重试覆盖文件。无需调整整个服务器的其他内存策略。

## 更新代码并启用低资源配置

如已有 1 GiB swap，且完整扫描仍有明显内存压力，可上传新版 setup_swap.sh，先结束扫描并停止扫描器，再执行 `bash setup_swap.sh --extend`。该模式新建 `/swapfile-netease-extra`，不关闭或重写原 swap；要求至少 3 GiB 空闲磁盘，新增 1 GiB 后保留至少约 2 GiB。总 swap 已有 2 GiB 时不重复创建。同名额外文件已存在但容量不足时拒绝覆盖，需检查实际激活情况。

宿主增加 swap 不会自动放宽容器限制：新版扫描器 RAM+swap 总上限为 1536 MiB（物理 RAM 448 MiB，swap 最多 1088 MiB）、API 为 320 MiB；要求宿主已有 2 GiB swap。额外 swap 可给宿主或其他服务提供缓冲，但不会加快扫描。不要把剩余磁盘全部分配给 swap，仍需容纳缓存增长、备份、日志和镜像更新。

在本机 PowerShell 上传新代码包及校验文件，保留现有数据和 Cookie：

```powershell
cd C:\Users\32461\Music\netease_server
scp -P <SSH_PORT> netease-server-code.zip netease-server-code.zip.sha256 root@<VPS_IP>:/opt/netease-scanner/
```

在 VPS 中执行：

```bash
cd /opt/netease-scanner
sha256sum -c netease-server-code.zip.sha256
python3 -m zipfile -e netease-server-code.zip .
```

先备份实际配置并临时关闭自动扫描，避免启动后马上补跑。在当前部署目录执行下面的代码；只修改调度开关和并发，不修改 Cookie 或数据。

```bash
python3 - <<'PY'
import json, shutil
from datetime import datetime
from pathlib import Path
p = Path('config/config.json')
backup = p.with_name('config.before-small-' + datetime.now().strftime('%Y%m%d-%H%M%S-%f') + '.json')
shutil.copy2(p, backup)
data = json.loads(p.read_text(encoding='utf-8-sig'))
data.update(schedule_enabled=False, threads=2)
p.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding='utf-8')
print('调度已暂时关闭，并发改为 2；原配置已备份')
PY
chown root:10001 config/config.json
chmod 440 config/config.json
bash compose-small.sh config --quiet
bash compose-small.sh build scanner
bash compose-small.sh up -d --no-build
```

`compose-small.sh` 固定使用 SSH 隧道部署和低资源覆盖配置，以后管理此部署时也使用该脚本，例如 `bash compose-small.sh ps`、`bash compose-small.sh logs --tail=100 scanner`。它对扫描器设置 0.60 核 CPU 配额、448 MiB RAM 上限及最多 1536 MiB 的 RAM+swap 总量；API 为 0.25 核、160 MiB RAM 和 320 MiB RAM+swap；网页入口为 0.50 核、64 MiB RAM 且不使用 swap。CPU 配额限制单个服务，并非预留核心；网页入口允许短暂的密码校验突发。RAM 上限合计 672 MiB，给宿主系统和其他服务留出部分余量；如果已有服务占用较多，仍可能不足。

低资源配置现在允许后端并发上限为 8，手动和定时任务共用上限，网页运行时调整也不能突破。初始试运行配置仍设为 2；已经确认较低并发稳定且有资源余量后，可以先手动用 5 线程，再通过网页“调整运行并发”逐步切到 6，最后评估是否需要 8。放宽上限不会自动修改实际配置中的默认并发。recent 50、归属验证 50、新专探测 20、日期窗口和扫描算法保持原行为。并发降低主要减少请求压力，不保证降低与关注艺人总数、全局候选池有关的基础内存。

已部署旧的上限 2 或 4 时，在 VPS 终端执行下面的命令即可更新，无需重新上传代码包或构建镜像：

```bash
cd /opt/netease-scanner
sed -i -E 's/(NETEASE_MAX_THREADS: *)"[0-9]+"/\1"8"/' compose.low-resource.yaml
bash compose-small.sh config --quiet
bash compose-small.sh up -d --no-deps --no-build scanner
```

后端从容器环境读取上限，必须重新创建容器才能生效；普通 restart 不会更新容器环境。重新创建会中断正在运行的任务，先等任务结束，或在网页停止后确认已结束，再应用。CPU／RAM 限制和 Cookie 都保留。网页刷新后先选择 5；定时任务默认并发取自 `config/config.json`，只有后续将其 threads 改为 5 才会默认使用 5。

如果 docker stats 中扫描器 CPU 大部分时间约 5%，偶尔升至约 60%，可能有较多网络或接口等待；单次突升不能认定 CPU 是持续瓶颈。扫描器的 0.60 核配额对应单核口径约 60%，API 的 0.25 核配额对应约 25%；持续接近各自配额时，增加并发的收益可能受限。更高并发也可能增加接口限流、重试和内存占用，8 是允许尝试的上限，尚未验证其完整扫描稳定性。

参考观察点是给上限保留余量，而不是等到触顶：例如扫描器接近 350 MiB、API 接近 120 MiB、swap 快速增长、SSH 明显变慢或接口失败比例上升时，先降回 2 或 3。单次低占用不代表所有扫描阶段都低占用。如果 API CPU 长期靠近其 25% 配额，继续增加并发可能没有明显收益；应先区分 API 处理、网络等待与扫描阶段瓶颈。

容器限制只约束运行中的容器，不约束 Docker 构建阶段；重建前应保持扫描器停止，尽量复用已有构建缓存。回传与每日备份也会产生压缩和读写开销，应在扫描空闲时使用。

## 再验收，最后恢复调度

首次 4 线程全量实测耗时约 75.2 分钟，61780 个专辑候选中 60423 个命中缓存（97.8%），实际 /album 请求 1412 次。/artist/album 的 5551 次成功对应 4596 位艺人新专兜底加 955 位归属验证，是主要请求耗时来源；并发 4 的理想粗估约 33.9 分钟，recent 约 22.7 分钟，专辑详情约 5.3 分钟。这不是实测阶段耗时，API 统计也不包含所有解析、写盘、等待和重试间隔。尾部 34／53 秒慢请求需要扫描运行期间的资源和阶段数据解释，不能仅凭结束后 vmstat 判定 swap 瓶颈。

用户在 4 线程第二阶段观察到扫描器约 376 MiB／448 MiB，后续 cgroup 检查为 memory.current 420.8 MiB、swap.current 17.3 MiB、swap.max 576 MiB、max 12、oom 和 oom_kill 均为 0；宿主已启用 2 GiB swap，可用物理内存约 110 MiB。max 表示曾触及内存边界并尝试回收，不表示已经发生 OOM。Docker stats 的内存值还可能扣除部分可回收文件缓存，与 memory.current 不完全相同。不能由这次采样预测完整扫描峰值；先降至 2 线程观察，不继续升并发，也不直接扩大 RAM 上限。

服务器引擎已增加内存优化：艺人专辑页只保留 code 与专辑 id/name/publishTime，线程池逐项交付已完成结果，日期筛选后释放不再需要的采样集合，扫描结束或失败后清理本轮专辑页缓存。保留全部艺人的 recent 50、新专目录 20 和归属验证 50，不改变历史缓存文件、CSV 格式或筛选规则。全局候选池和持久化专辑日期缓存仍需要内存，不能据此保证完整扫描一定适合 1 GB VPS。

应用内存优化需要更新 engine.py 所在的扫描器镜像；只修改并发环境变量不会获得这些代码改动。先等任务结束，或在网页停止并确认任务已结束。按前述 PowerShell 命令上传代码 ZIP 和 SHA-256 文件后，在 VPS 执行：

```bash
cd /opt/netease-scanner
sha256sum -c netease-server-code.zip.sha256
python3 -m zipfile -e netease-server-code.zip .
bash compose-small.sh stop -t 40 scanner
bash compose-small.sh build scanner
```

上面两项构建前操作只在校验成功后继续执行；若构建失败，先处理错误，不将旧镜像当作已更新镜像启动。构建成功后执行：

```bash
bash compose-small.sh up -d --no-deps --no-build scanner
bash compose-small.sh ps
```

代码 ZIP 不包含实际 config、secrets、data 或 .env，不覆盖 Cookie 与运行记录；本次新版低资源覆盖文件的并发上限为 8，但当前推荐仍先用 2。已启用的定时任务可能在启动后补跑当天缺失任务，默认线程取自现有 config/config.json。随后通过网页完整扫描并对比各阶段内存、swap 和耗时；当前运行中的旧代码不会自动获得优化。

先执行 `bash compose-small.sh exec -T scanner python probe.py`，随后通过网页做一次手动完整扫描，并同时观察 `docker stats`、`free -h`、SSH 和控制台是否保持可用。如果被 RAM 上限终止或任务中断，检查 `docker inspect --format '{{.State.OOMKilled}}' netease-scanner-scanner-1` 和内核日志，评估升级内存，而不是只提高限制挤占宿主系统。

低资源参数是供此机器试运行的起点，尚未在用户 VPS 的真实全量扫描中验证。swap 不等同于额外物理内存，1 GB 的稳定运行不能由此保证；如果仍频繁卡顿或 OOM，建议升级至至少 2 GB 后再次实测。

完整扫描和管理连接均稳定后再将 `config/config.json` 的 `schedule_enabled` 改回 `true`。当天 00:03 已过且尚无当天定时任务时，会立即补跑；已中断的任务不会无限重试。避免使用缺少低资源覆盖文件的 Compose 命令重建容器，否则限制将不再生效。
