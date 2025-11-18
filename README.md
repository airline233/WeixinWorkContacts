# WeixinWorkContacts / 企业微信通讯录采集工具

这是一个用于采集企业微信通讯录的并发爬虫脚本，专为大规模数据抓取设计，具备断点续爬、空组织跳过、失败重试及本地缓存等高级功能。

## 主要特性

- **并发抓取**: 使用多线程 (`ThreadPoolExecutor`) 并发处理多个组织，显著提升采集效率。
- **断点续爬**: 抓取状态实时保存 (`dept_state.json`)，程序意外中断后可无缝恢复，避免重复工作。
- **自动重试**: 内置请求重试机制 (`urllib3.Retry`)，应对网络波动，提高抓取成功率。
- **空组织跳过**: 自动识别并记录空组织 (`empty_departments.json`)，在后续运行中直接跳过，节省时间。
- **本地数据库缓存**: 联系人信息存储在 SQLite 数据库 (`contacts.db`) 中，方便后续查询与分析。
- **增量更新**: 只抓取数据库中不存在的新成员，避免重复写入。
- **详细日志**: 提供控制台关键信息输出和详细的文件日志 (`logs/`)，便于监控与排错。
- **失败成员记录**: 针对无法获取详情的成员，会将其 ID 记录在 `logs/failed_members.log` 中，方便后续手动排查或补采。

## 文件结构

```
.
├── main.py                 # 主程序
├── contacts.db             # 联系人数据库 (SQLite)
├── dept_state.json         # 组织抓取状态记录
├── empty_departments.json  # 空组织缓存
├── requirements.txt        # Python 依赖
├── .gitignore              # Git 忽略文件配置
└── logs/
    ├── crawler.log           # 详细运行日志
    ├── empty_departments.log # 空组织记录日志
    └── failed_members.log    # 失败成员记录
```

## 环境准备

- Python 3.9+

1.  克隆项目
    ```bash
    git clone https://github.com/airline233/WeixinWorkContacts
    cd WeixinWorkContacts
    ```

2.  安装依赖
    ```bash
    pip install -r requirements.txt
    ```

## 配置说明

脚本的主要配置项位于 `main.py` 顶部，可根据实际需求调整：

- `DB_FILE`: SQLite 数据库文件名。
- `PAGE_LIMIT`: 获取组织成员列表时的分页大小。
- `MAX_WORKERS`: 并发抓取的线程数，可根据网络环境和机器性能调整。
- `CORP_ID`: 目标企业 ID。
- `REQUEST_TIMEOUT`: 请求超时时间（秒）。
- `MAX_REQUEST_RETRIES`: 请求失败后的最大重试次数。
- `MEMBER_MAX_RETRIES`: 获取单个成员详情失败后的最大重试次数。

## 使用方法

1.  获取 `h5_contacts_sid`：
    - 登录企业微信网页版或客户端。
    - 打开浏览器开发者工具 (F12)，切换到 "Application" (或 "存储") 标签页。
    - 在左侧的 "Cookies" 下找到 `work.weixin.qq.com`。
    - 复制 `h5_contacts_sid` 的值。

2.  运行脚本：
    在项目目录下打开终端，运行以下命令：
    ```powershell
    python main.py
    ```

3.  输入 SID：
    根据提示，粘贴你复制的 `h5_contacts_sid` 并按回车。

脚本将自动开始执行，并显示进度。

## 状态管理与断点续爬

- **状态文件**: `dept_state.json` 记录了每个组织的状态 (`pending`, `running`, `done`, `empty`, `failed`)。
- **恢复执行**: 如果脚本中断，重新运行即可。它会自动跳过 `done` 和 `empty` 状态的组织，并重试 `failed` 或处理 `pending` 的组织。
- **强制重抓**: 若要强制重新抓取某个组织，只需从 `dept_state.json` 文件中删除对应的条目即可。

## 故障排查

- **SID 失效**: 如果提示 `无法获取组织架构`，通常是 `h5_contacts_sid` 已过期，请重新获取。
- **频繁超时**: 如果日志中出现大量请求超时，可以尝试在 `main.py` 中调低 `MAX_WORKERS` (并发数) 或调高 `REQUEST_TIMEOUT` (超时时间)。
- **成员详情获取失败**: 失败的成员 ID 会被记录在 `logs/failed_members.log` 中。你可以检查这些日志来分析失败原因，或编写辅助脚本进行补采.
