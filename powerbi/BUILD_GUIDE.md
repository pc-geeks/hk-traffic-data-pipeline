# Power BI 搭建指南（约 3–4 小时）

> 已在 Power BI Desktop（Microsoft Store，2.159）上用 5 个工作日的 CSV 走通。
> 同目录的 `HKTraffic.pbip` 就是那份成品，用 Desktop 直接打开。它用参数 `DataPath` 指向 CSV 目录，当前是
> `G:/MyGithub/self-learn-autumn/hk-traffic/hk-traffic-pipeline/hk-traffic-pipeline/powerbi/bi_data`。
> 打开后如果顶部写着表没有数据，点 **刷新**（主页 → 刷新，或横幅上的 Refresh now）。
> 下面仍是手工搭建步骤。标题里的数字属于这一批数据（2026-10-26 至 10-30，只有工作日）；换数据后要重算。

## 0. 前置条件

- **Windows 电脑 + Power BI Desktop（免费）。** Mac 没有 Desktop 版。Mac 用户的备选：用 Power BI 网页版（功能受限），
  或者改用免费的 Metabase / Apache Superset，同样的 CSV 和 SQL 视图都能用，页面设计思路不变。
- 计算已经跑完：`hktraffic run-range --start 2026-10-26 --end 2026-10-30`（或逐日 `run-day`，至少 5 个工作日，否则没有异常检测结果）。
- 装库和导出要先有 PostgreSQL。打开 Docker Desktop，在管道目录执行 `docker compose up -d warehouse`，再执行 `hktraffic serve`。

## 1. 导出数据

`export-bi` 从 PostgreSQL 的 `bi_*` 视图写出 CSV（UTF-8 带 BOM）。`--out` 用报告参数 `DataPath` 的目录。仓库默认的 `data/bi` 是另一个文件夹。

```powershell
hktraffic export-bi --out "G:/MyGithub/self-learn-autumn/hk-traffic/hk-traffic-pipeline/hk-traffic-pipeline/powerbi/bi_data"
```

写完后用 Desktop 打开 `HKTraffic.pbip`，点主页 **刷新**。这一批 7 个 CSV：

| 文件 | 这一批的行数 | 内容 | 用在哪一页 |
|---|---|---|---|
| `bi_detector_hour.csv` | 91,549 | 每个探测器每小时一行：速度、流量、拥堵占比、覆盖率、坏数据行数 | 第 1、5 页 |
| `bi_corridor_hour.csv` | 18,145 | 按走廊 × 行车方向 × 小时聚合 | 第 3 页（潮汐） |
| `bi_hotspot.csv` | 779 | 拥堵热点排名，含经纬度，一行一个探测器 | 第 2 页 |
| `bi_anomaly.csv` | 66,650 | 每个可比较的探测器-小时都在；`is_anomaly = 1` 才是异常（这一批 212 行） | 第 4 页 |
| `bi_coverage_hour.csv` | 120 | 每天每小时的数据覆盖情况 | 第 5 页 |
| `bi_detector_quality.csv` | 779 | 每个探测器的坏数据占比 | 第 5 页 |
| `bi_pipeline_run.csv` | 5 | 每天处理了多少原始文件、各类标记行数 | 第 5 页 |

也可以不用 CSV，直接用 Power BI 的 PostgreSQL 连接器连到 `hkt` 这个 schema 里的同名视图（需要先装 Npgsql 驱动，所以 CSV 更省事）。

## 2. 导入和建模

1. `获取数据 → 文本/CSV`，逐个导入 7 个文件。逐个文件比「文件夹一次导入」好核对。
2. 导入后若提示表是空的或不完整，点主页的 **刷新**。进 Power Query 再「关闭并应用」不一定会把数据装进来。
3. 检查数据类型：
   - `obs_date` 设为日期（不要留成日期/时间，否则和 `Date` 表连不上）。
   - `hour`、`iso_dow`、`is_anomaly`、`low_coverage` 设为整数。`hour` 若被识别成文本，后面的小时关系会建失败。
   - `is_anomaly` 和 `low_coverage` 是 0/1，不是布尔值。筛选器用数字 `1`。
   - 占比列（`congested_share`、`drop_share`、`bad_share`、`avg_coverage` 等）在 CSV 里是小数，`0.011` 表示 1.1%。格式设为 **百分比**。度量值已经返回小数，不要再除以 100。
   - 速度保留 1 位小数。
4. 经纬度列：选中 `latitude`，`列工具 → 数据类别 → 纬度`；`longitude` 设为 `经度`。`bi_detector_hour`、`bi_hotspot`、`bi_anomaly`、`bi_detector_quality` 里有这两列的都设。
5. `weekday` 列：`列工具 → 按列排序 → iso_dow`（`bi_detector_hour` 和 `bi_corridor_hour` 各设一次），
   这样星期一到星期日才会按顺序显示，而不是按字母排序。
6. `Hour` 表建好后，把 `Hour Label` 设为按 `Hour` 排序。
7. 新建两个辅助表（`建模 → 新建表`），代码在 `measures.dax` 最前面：`Date` 和 `Hour`。
8. 建关系（`模型视图`），多对一、筛选方向从日期/小时指向事实表：
   - `Date[Date]` → `bi_detector_hour`、`bi_corridor_hour`、`bi_anomaly`、`bi_coverage_hour` 的 `obs_date`
   - `Hour[Hour]` → 这四个表的 `hour`
   - `bi_hotspot`、`bi_detector_quality`、`bi_pipeline_run` 不连。热点和质量是汇总表，管道运行的日期列叫 `day`，对不上 `obs_date`。
9. `主页 → 输入数据`，建一个空表叫 `_Measures`，然后把 `measures.dax` 里的度量值逐个粘贴进去（在 `_Measures` 上 `新建度量值`）。
   这一批数据上，度量值原样可用，卡片数字见第 5 节。
10. `视图 → 主题 → 浏览主题`，导入 `theme.json`（主题名 HK Traffic）。

> 度量值里「速度」一律按**流量加权**，并且只算覆盖率达标的小时（`low_coverage = 0`）。这是刻意的，原因在 `docs/data_findings.md`。

页签用短名，页面上方的标题用一句话结论。这一批数据用的页签名是：一天的节奏、拥堵热点、潮汐车流、异常事件、数据可信度。

## 3. 页面设计（5 页，每页一个问题）

**每页标题写成「一句话结论」，而不是「速度分析」这样的标签。**
标题比图表先被读到，所以写成这批数据实际算出的结论。下面括号里的句子写在页面上的文本框或卡片里。

### 第 1 页：香港道路的一天（节奏）

- 标题：`工作日晚高峰比午间慢 6.4%（18:00 最慢，约 65 km/h）`
- 卡片 × 4：`Avg Speed (km/h)`、`Congested Share`、`Evening Peak Slowdown %`、`Detectors Reporting`
- 再用一张卡片放 `Title Speed`（`measures.dax` 末尾）。不加筛选时应显示：Average speed 72 km/h, 1.1% of lane readings congested。
- **折线图**：X = `Hour[Hour Label]`，Y = `Avg Speed (km/h)`，图例 = `bi_detector_hour[day_type]`
  - 这一批只有工作日，图例只会有一条线。周末线要等数据里真有 `day_type = weekend`。
- **热力矩阵**：行 = `weekday`，列 = `Hour[Hour Label]`，值 = `Congested Share`
  - 条件格式用绿 → 黄 → 红，自己定色阶，不要用自动最小/最大。这一批大部分格子在 1% 附近，自动色阶会看起来几乎一样。可用的停靠点：0 绿、3% 黄、8% 红。
- 切片器：`Date[Date]`、`Hour[Hour Label]`、`district`

### 第 2 页：拥堵热点地图

- 标题：`前 5 条走廊占了 31% 的拥堵读数，前 10 占 51%`
  - 这个 31% / 51% 是拥堵读数的合计占比（见 `docs/FINDINGS_5_WEEKDAYS.md`），不是下面条形图的横轴。条形图是探测器的平均拥堵占比。
- **散点图**（不要用 Azure 地图）：X = `longitude`（平均值），Y = `latitude`（平均值），大小 = `mean_congested_share`（平均值），图例 = `peak_hour`，明细 = `detector_id`。明细放在「详细信息」，不要放进「值」，否则会提示删掉值才能画 X/Y。
  - Azure 地图视觉对象会要求登录（To display Azure Maps visuals, sign in）。散点图不登录也能画出香港路网的形状。
  - 悬停提示：`corridor`、`location`、`district`、`mean_speed_kmh`。
- **条形图**：Y = `corridor`，X = 平均 `mean_congested_share`。在**这个视觉对象**的筛选器里设「前 N 个 = 10」，按这个平均值排序。不要做成页面级筛选，否则表格也被砍成 10 行。
- **表格**：`rank`、`corridor`、`location`、`district`、`peak_hour`、`mean_speed_kmh`
- 切片器：`district`

### 第 3 页：潮汐车流（方向不对称）

- 标题：`Tuen Mun Road 早上东行更慢，晚上西行更慢`
- 切片器：`corridor`，设成单选，默认 **Tuen Mun Road**
- 页面筛选器：`day_type = weekday`（文本 `weekday`，不是布尔值）
- **折线图**：X = `Hour[Hour Label]`，Y = `Corridor Speed (km/h)`，图例 = `travel_direction`
- **折线图**：同上，Y = `Corridor Volume`（看流量是否也随方向翻转）
- 这一批里两个方向都有足够流量、并且早晚相反的走廊：**Tuen Mun Road**（早 Eastbound 慢 7.9%，晚 Westbound 慢 6.2%）、**Yuen Long Highway**。North Lantau Highway 两个方向都接近畅通路速。其余走廊先看是不是只有一个方向的探测器。
- 三条过海隧道（红隧、东隧、西隧）**不在**这个数据集里，不要做「过海隧道对比」。

### 第 4 页：异常事件

- 标题：`10月30日 18:00，Island Eastern Corridor 近 ICAC Headquarters 比平时慢 89%`
- 卡片：`Anomaly Count`、`Avg Anomaly Drop %`（这一批是 212 和 38.7%）
- **页面筛选器：`is_anomaly = 1`（整数 1）。** 不筛的话表里是全部 66,650 行，卡片也会把正常小时算进去。
- **表格**（这一页以表为准）：`obs_date`、`hour`、`corridor`、`location`、`wavg_speed`、`baseline_mean`、`drop_share`，按 `drop_share` 降序。第一行应是 2026-10-30 18:00，速度约 8.0，基线约 75.9，降幅 89.48%。
- **柱状图**：横轴 = `Hour[Hour Label]`，图例 = `obs_date`，值 = `Anomaly Count`。原来的散点图把日期和未汇总的小时放在两条轴上，Desktop 会报轴类型不匹配，所以改成按天、按小时计数。具体降幅仍看左边的表。
- 把表格里最严重的 3–5 条**拿去对照新闻和天气记录**（交通意外、暴雨、台风信号），写进 README。这一步还没做。同一小时、同一条走廊上多个探测器一起掉速，更像真实事件而不是单个坏探测器；周五的异常条数偏多，因为基线把周一到周五混在一起了。

### 第 5 页：数据可信度

- 标题：`原始归档每天大约只到 68%–74%，TDS90018 的坏数据占 68%`
- 卡片：`Raw File Completeness`（71.1%）、`Bad Row Share`（0.88%）、`Avg Hour Coverage`（68.3%）
- **热力矩阵**：行 = 日期，列 = `Hour[Hour Label]`，值 = `Avg Hour Coverage`
  - 色阶自己定：40% 红、70% 黄、95% 绿。自动色阶会把 70% 附近的格子涂成同一种颜色。这一批最差的是 10 月 29 日 17:00 和 10 月 30 日 13:00，大约 44%–45%。
- **条形图**：Y = `detector_id`，X = `bad_share`，前 10，提示里加 `corridor` 和 `location`。按走廊聚合会把 TDS90018 平均掉。第一名是 TDS90018（Tuen Mun Road，68.3%），第二是 AID09403（45.1%）。
- **表格**：`bi_pipeline_run` 的每日行（files、rows、各类标记行数）
- 这一页回答原始文件缺了多少、哪些探测器的坏行多到不能直接拿来算速度。

## 4. 交互和细节

- 每页最多 3–4 种颜色，沿用主题色：绿（顺畅）、黄、红（拥堵）。
- 给图表加提示框（Tooltip）：悬停时显示走廊、位置、速度、流量。
- 用底部的页面选项卡切换。页面导航视觉对象会盖住图表，这一版没有放。
- 所有标题和坐标轴写清楚单位（km/h、%）。
- 走廊切片器放在切片器自己的筛选里（单选 + 默认值）。页面级筛选器只放全页都要的条件：第 3 页的 `day_type`，第 4 页的 `is_anomaly`。

## 5. 核对数字（必做）

不加任何筛选时，卡片应和下面一致（这一批 CSV；定义与 SQL 相同：流量加权，且 `low_coverage` 为假）：

| 度量值 | 期望值 |
|---|---|
| `Avg Speed (km/h)` | 72.3 |
| `Congested Share` | 1.1% |
| `Evening Peak Slowdown %` | 6.4% |
| `Detectors Reporting` | 779 |
| `Anomaly Count`（页面上还要筛 `is_anomaly = 1`） | 212 |
| `Raw File Completeness` | 71.1% |

有数据库时，用这条 SQL 对速度：

```sql
SELECT ROUND((SUM(wavg_speed * volume) / SUM(volume))::numeric, 1)
FROM hkt.fact_detector_hour WHERE NOT low_coverage AND wavg_speed IS NOT NULL;
```

和 `Avg Speed (km/h)` 卡片（不加任何筛选时）应该一致。对不上时先看这四件事：数据有没有刷新、`hour` / `obs_date` 的数据类型、关系是不是只连了那四个表、占比是不是被格式或再除 100 弄错了。

## 6. 报告还没放进仓库的材料

- 在仓库里放 3–4 张截图（`docs/images/`）和一段 20 秒的 GIF，并嵌进 README，这样不打开 Desktop 也能看到图。这一步还没做。
- 导出 PDF（`文件 → 导出 → PDF`）也放进仓库。
- **不要用「发布到 Web」**，除非你确认数据使用条款允许公开再分发；它会生成任何人都能看的公开链接。
- 可编辑的项目是 `HKTraffic.pbip`（文本格式，读取 `DataPath` 指向的 CSV）。需要单个文件时，在 Desktop 里另存为 `.pbix`。换 CSV 目录时改 `DataPath` 再刷新。
