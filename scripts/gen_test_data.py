#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Stratum 大规模测试数据生成脚本。

通过 stratum-gateway 的 REST API（默认 http://127.0.0.1:8081）向运行中的
Stratum 灌入大量测试数据：创建多个知识库（不同索引类型 / 相似度度量 / 分块
参数 / embed 模型），每个知识库写入大批文档并构建多条版本链（ADD / UPDATE /
DELETE / 混合变更），随后覆盖三大外部服务的全部主要功能：

  KnowledgeBaseService  CreateKnowledgeBase / GetKnowledgeBase /
                        ListKnowledgeBases / CreateVersion / ListVersions /
                        RollbackVersion / DeleteKnowledgeBase
  QueryService          Query（active 版本、指定版本、top_k、threshold、
                        aggregation=MEDIAN/MAX/MEAN、错误路径）
  AdminService          HealthCheck / GetSystemStatus / RebuildIndex /
                        WarmupVersion

数据量默认较大（10 个库 × 2000 篇文档 × 多条版本，约 6~8 万向量），可用
--kbs / --docs / --versions / --delta 按需缩放；--dry-run 只打印计划不连服务。

前置条件：
  1. 已启动 ./start.sh（内含 vecstore、mock embed、stratum、gateway）。
  2. mock embed 是默认实现：它对 chunk ID 做确定性哈希生成向量。本脚本在
     验证阶段用同样的算法复算查询向量（见 mock_embed_vector），因此"用文档
     原文片段查询"能精确命中该文档，从而自校验检索正确性。
     若你的 embed 服务不是 mock（或改了 VEC_DIM），请用 --no-verify 跳过
     校验，或用 --dim 对齐向量维度。

用法示例：
  python3 scripts/gen_test_data.py --dry-run
  python3 scripts/gen_test_data.py
  python3 scripts/gen_test_data.py --kbs 3 --docs 500 --versions 3
  python3 scripts/gen_test_data.py --no-verify --quiet

重跑提示：本脚本不幂等，重复执行会追加新的知识库；想清空重来请先停服务再
运行 scripts/delete_test_db.py，然后重新 ./start.sh。

只依赖 Python 标准库。
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import random
import struct
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

# ---------------------------------------------------------------------------
# 常量
# ---------------------------------------------------------------------------
DEFAULT_GATEWAY = "http://127.0.0.1:8081"
DEFAULT_EMBED_ADDR = "http://localhost:8080"
DEFAULT_DIM = 768  # 与 mock embed 默认 VEC_DIM 对齐

VERSION_PENDING = "INDEX_STATUS_PENDING"
VERSION_READY = "INDEX_STATUS_READY"
VERSION_FAILED = "INDEX_STATUS_FAILED"
KB_ACTIVE = "KB_STATUS_ACTIVE"

OP_ADD = "CHANGE_OP_ADD"
OP_UPDATE = "CHANGE_OP_UPDATE"
OP_DELETE = "CHANGE_OP_DELETE"

AGG_MEDIAN = "AGGREGATION_METHOD_MEDIAN"
AGG_MAX = "AGGREGATION_METHOD_MAX"
AGG_MEAN = "AGGREGATION_METHOD_MEAN"

HEALTHY = "HEALTH_STATUS_HEALTHY"
DEGRADED = "HEALTH_STATUS_DEGRADED"

# ---------------------------------------------------------------------------
# 测试语料：多个领域的模板句子，用于生成"看起来真实"的文档
# ---------------------------------------------------------------------------
DOMAINS = [
    ("ai", "AI 与机器学习", [
        "本季度向量检索服务的平均查询延迟为 38ms，P99 延迟 112ms，均低于 SLO 阈值。",
        "HNSW 索引在 10 万级向量规模下保持 96% 以上的召回率，且构建时间随数据量近似线性增长。",
        "评估集由 500 条人工标注的查询-文档对组成，覆盖中英文混合与长尾专业术语场景。",
        "RAG 流水线将召回阶段的 top-20 结果送入重排模型，最终输出 top-5 答案并附带引用出处。",
        "embedding 模型基于对比学习训练，支持 768 维向量输出，与下游 Faiss 索引直接兼容。",
        "增量训练每两周执行一次，使用上一轮难例挖掘得到的负样本进行困难样本增强。",
        "向量数据库的 MVCC 版本机制允许同一知识库并行维护多个索引版本，用于 A/B 对比实验。",
        "离线评测显示，分块窗口 512、重叠 64 的参数组合在问答任务上得分最高。",
        "模型蒸馏将教师模型的 768 维表征压缩为 384 维，推理吞吐提升约 40% 而精度损失小于 1%。",
        "检索结果按相似度降序返回，并对重复文档进行去重聚合，保证答案来源的唯一性。",
        "数据标注流程采用双人独立标注加仲裁机制，标注一致性系数达到 0.91。",
        "线上监控面板实时展示索引构建状态、内存占用与查询热点分布，异常时自动告警。",
    ]),
    ("ecom", "电商与供应链", [
        "双十一大促期间订单峰值达到每秒 12 万笔，交易系统通过分库分表与异步削峰保障稳定。",
        "商品详情页的库存数量由分布式缓存与数据库双写，缓存击穿时自动回源并加锁重建。",
        "供应链系统按区域与时效分为 28 个履约中心，超时订单自动触发赔付流程。",
        "推荐系统综合用户历史行为、实时点击与季节因子，为每个用户生成个性化商品池。",
        "物流轨迹数据每小时同步一次，异常停滞包裹超过 48 小时自动升级人工介入。",
        "售后工单按紧急程度分为三级，24 小时内处理率目标为 95%，实际达成 97.3%。",
        "价格策略引擎支持满减、秒杀、会员价三种优惠叠加，叠加规则经多轮灰度验证。",
        "供应商准入审核包含资质核验、样品检测与产能评估三道工序，平均审核周期 5 个工作日。",
        "仓储机器人集群按订单波次调度，拣选效率较人工提升 3 倍，错发率低于十万分之一。",
        "会员体系积分按消费金额与活跃度双维度累积，积分有效期两年，过期前短信提醒。",
        "大促预案在系统压测通过后生效，核心链路预留 30% 冗余容量应对流量突增。",
        "客服知识库覆盖常见咨询场景 1200 余条，智能机器人首答解决率稳定在 82% 左右。",
    ]),
    ("fin", "金融与风控", [
        "风控模型对每笔交易实时评分，欺诈拦截率 99.2%，误伤率控制在 0.05% 以内。",
        "贷款审批流程采用规则引擎加机器学习双通道，小额贷款实现秒级自动放款。",
        "反洗钱监测系统对可疑交易进行多维度关联分析，生成的可疑交易报告须在 48 小时内提交。",
        "客户风险等级分为五档，高风险客户每季度重新评估，评估结果留痕可追溯。",
        "交易系统每日对账在凌晨两点前完成，长短款差异自动生成工单并由专人处理。",
        "理财产品信息披露遵循净值化转型要求，持仓净值每日公布，重大事项实时公告。",
        "支付通道按商户类别分配限额，跨境交易额外执行外汇申报与额度校验。",
        "征信数据调取需双人复核授权，调用日志保留五年，供监管检查随时调阅。",
        "流动性压力测试覆盖极端市场情景，测试结果按月报送风险管理委员会。",
        "反欺诈规则库包含 300 余条规则，新增规则先影子运行两周再正式上线。",
        "客户投诉处理时效按监管要求执行，一般投诉 5 个工作日办结并回访确认。",
        "系统灾备采用同城双活架构，核心系统 RPO 小于 1 分钟，RTO 小于 30 分钟。",
    ]),
    ("med", "医疗健康", [
        "电子病历系统遵循 HL7 标准，检验结果自动归档并支持跨院区共享调阅。",
        "临床决策支持系统对用药方案进行相互作用检查，高危警示必须由医生二次确认。",
        "影像诊断平台支持 CT、MRI、超声多模态数据，AI 辅助筛查肺结节灵敏度达到 94%。",
        "药品库存按效期先进先出管理，近效期药品提前 90 天预警，过期药品自动下架。",
        "预约挂号系统支持分时段精准预约，爽约三次以上的用户限制预约特权。",
        "住院医嘱执行采用条码扫码双人核对，给药差错率下降至万分之零点三。",
        "慢病管理平台对糖尿病、高血压患者提供随访提醒与用药指导，随访完成率 88%。",
        "手术排程综合评估医生资质、器械准备与床位资源，超时手术自动顺延并通知家属。",
        "医疗废弃物分类收集、专车转运、定点处置，全程电子联单可追溯。",
        "远程会诊平台支持多学科专家协同，会诊意见 24 小时内出具并归档至病历。",
        "医院信息平台每年通过等保三级测评，核心数据库访问均记录审计日志。",
        "患者隐私数据脱敏展示，敏感字段加密存储，授权访问范围按岗位最小化配置。",
    ]),
    ("legal", "法律合规", [
        "合同审核流程覆盖法务初审、业务复审与高管终审三级，重大合同必须出具书面意见。",
        "个人信息保护合规要求数据处理活动逐项登记，删除权与携带权请求 15 日内响应。",
        "知识产权申请由代理机构统一管理，发明专利从申请到授权平均周期 22 个月。",
        "反垄断合规培训每半年组织一次，高风险业务上线前须通过合规评审清单。",
        "审计证据保存期限按档案管理规定执行，电子证据双副本异地存储。",
        "供应商合同到期前 90 天自动预警，续签谈判由采购与法务联合参与。",
        "出口管制清单每季度更新一次，涉及受限物项的业务须逐单申请许可。",
        "数据出境安全评估通过国家网信部门审批后方可执行，评估材料归档备查。",
        "劳动用工合规检查覆盖考勤、加班、社保缴纳与竞业限制条款执行情况。",
        "商业贿赂高风险岗位实行轮岗制度，举报渠道全年开放并由独立部门受理。",
        "合规事件报告要求两小时内上报风险管理部门，重大事件同步报送董事会。",
        "外部律师函件统一由法务部归口处理，回复时限按紧急程度分级管控。",
    ]),
    ("infra", "云原生基础设施", [
        "Kubernetes 集群按业务域划分命名空间，资源配额与优先级由平台团队统一管理。",
        "服务网格基于 Istio 实现灰度发布与熔断限流，金丝雀比例可精确到 1% 粒度。",
        "CI 流水线包含静态检查、单元测试、镜像扫描与集成测试四个阶段，全绿才允许发版。",
        "日志系统采用 Loki 集中采集，保留 30 天，检索索引按租户隔离防止越权。",
        "监控告警基于 Prometheus 指标，告警分级路由，P0 告警 5 分钟内必达值班手机。",
        "对象存储采用纠删码冗余，跨可用区三副本，数据持久性承诺达到十一个九。",
        "配置中心支持多环境隔离与变更审计，回滚操作在 30 秒内完成且保留快照。",
        "数据库连接池按服务实例数动态伸缩，慢查询超过 500ms 自动记录并推送优化建议。",
        "容器镜像仓库开启漏洞扫描，高危漏洞镜像禁止部署至生产环境。",
        "混沌工程演练每季度执行一次，演练场景包括节点宕机、网络分区与时钟漂移。",
        "弹性伸缩策略结合 CPU 利用率与 QPS 双指标，扩容触发阈值 70%，冷却时间 5 分钟。",
        "密钥管理采用集中式 KMS，应用通过 SDK 动态获取密钥，杜绝明文落盘。",
    ]),
    ("hr", "人力资源与制度", [
        "年度绩效考核采用 360 度评估，结合目标达成、协作贡献与价值观三个维度。",
        "招聘流程包含简历筛选、笔试、两轮面试与背景调查，offer 审批时限为五个工作日。",
        "新员工入职培训为期两周，涵盖企业文化、信息安全与岗位技能三部分内容。",
        "请假制度按事假、病假、年假分类管理，连续请假超过五天需部门负责人审批。",
        "薪酬调整每年一次，依据绩效等级与市场分位数据综合确定，调整结果保密发放。",
        "员工股权激励分四年归属，每年归属四分之一，离职时未归属部分自动回收。",
        "晋升通道分管理序列与专业序列，专业序列最高可达与副总裁平级的技术专家。",
        "培训预算按部门年度计划分配，人均培训时长目标不低于四十小时。",
        "员工满意度调查每年两次，匿名收集，结果由工会代表共同解读并跟进改善。",
        "竞业限制协议覆盖核心技术与管理岗位，补偿金按月支付，禁业期最长两年。",
        "弹性工作制度允许核心时段外自由安排，每月远程办公上限为十天。",
        "员工援助计划提供心理咨询与法律咨询支持，服务记录严格保密。",
    ]),
    ("sec", "网络安全", [
        "安全漏洞管理遵循发现、评估、修复、复测四步流程，高危漏洞 72 小时内完成修复。",
        "防火墙策略变更需双人审批，每周自动巡检无效策略并生成清理建议。",
        "终端安全软件覆盖全量办公设备，违规外联与敏感文件外发行为实时阻断。",
        "渗透测试每半年执行一次，测试报告中的高风险项须在下一迭代内整改闭环。",
        "账号权限遵循最小化原则，离职员工账号在离岗当天禁用，特权账号季度复核。",
        "敏感数据加密传输使用 TLS 1.3，密钥轮换周期 180 天，历史密钥保留两年。",
        "Web 应用防火墙拦截 SQL 注入与跨站脚本攻击，规则库每周自动更新。",
        "安全事件响应按应急预案分级处置，重大事件成立应急小组并每两小时通报进展。",
        "堡垒机对所有运维操作录像留痕，高危命令执行需二次审批。",
        "备份数据加密后异地存储，恢复演练每季度一次，演练结果形成报告存档。",
        "钓鱼邮件演练不定期开展，点击率高于 5% 的部门需追加安全意识培训。",
        "安全基线扫描覆盖操作系统、中间件与数据库，偏离基线的配置自动生成整改工单。",
    ]),
]


# ---------------------------------------------------------------------------
# 网关 HTTP 客户端（REST / JSON，对应 cmd/stratum-gateway 的路由）
# ---------------------------------------------------------------------------
class ApiError(Exception):
    """HTTP 层错误：携带状态码与网关返回的 JSON（error / grpc_code）。"""

    def __init__(self, http_code: int, body, grpc_code=None):
        self.http_code = http_code
        self.body = body
        self.grpc_code = grpc_code
        super().__init__(f"HTTP {http_code} grpc={grpc_code} {body}")


class Gateway:
    def __init__(self, base: str, timeout: int = 300):
        self.base = base.rstrip("/")
        self.timeout = timeout

    def _call(self, method: str, path: str, body=None):
        data = None
        headers = {}
        if body is not None:
            data = json.dumps(body, ensure_ascii=False).encode("utf-8")
            headers["Content-Type"] = "application/json"
        last_err = None
        for attempt in range(10):
            url = self.base + path
            req = urllib.request.Request(url, data=data, headers=headers, method=method)
            try:
                with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                    raw = resp.read().decode("utf-8")
                    return resp.status, (json.loads(raw) if raw else {})
            except urllib.error.HTTPError as e:
                raw = e.read().decode("utf-8", "replace")
                try:
                    parsed = json.loads(raw)
                except ValueError:
                    parsed = {"error": raw}
                if self._is_transient(e.code, parsed) and attempt < 9:
                    last_err = ApiError(e.code, parsed, parsed.get("grpc_code"))
                    time.sleep(0.5 * (attempt + 1))
                    continue
                raise ApiError(e.code, parsed, parsed.get("grpc_code")) from None
            except urllib.error.URLError as e:
                raise ApiError(-1, {"error": f"无法连接 {self.base}: {e.reason}"}) from None
        raise last_err

    @staticmethod
    def _is_transient(code: int, parsed) -> bool:
        """判断是否为可重试的瞬态错误（raft 选举中 / 服务暂时不可达）。"""
        msg = (parsed.get("error", "") or "").lower()
        grpc = parsed.get("grpc_code") or ""
        if code == 503:
            return True
        if code == 500 and ("not leader" in msg or "no leader" in msg or grpc == "Unavailable"):
            return True
        return False

    # ---- KnowledgeBaseService ----
    def create_kb(self, name, window, overlap, index_type, similarity, embed_addr, model_id):
        return self._call("POST", "/api/knowledge-bases", {
            "name": name,
            "chunk_window_size": window,
            "chunk_overlap_size": overlap,
            "index_type": index_type,
            "similarity": similarity,
            "embed_config": {"service_addr": embed_addr, "model_id": model_id},
        })[1]

    def get_kb(self, kb_id):
        return self._call("GET", "/api/knowledge-bases/" + urllib.parse.quote(kb_id, safe=""))[1]

    def list_kbs(self):
        return self._call("GET", "/api/knowledge-bases")[1]

    def delete_kb(self, kb_id):
        return self._call("POST", "/api/knowledge-bases/delete", {"knowledge_base_id": kb_id})[1]

    def create_version(self, kb_id, parent_version_id, changes):
        return self._call("POST", "/api/knowledge-bases/" + urllib.parse.quote(kb_id, safe="") + "/versions", {
            "parent_version_id": parent_version_id,
            "changes": changes,
        })[1]

    def list_versions(self, kb_id):
        return self._call("GET", "/api/knowledge-bases/" + urllib.parse.quote(kb_id, safe="") + "/versions")[1]

    def rollback(self, kb_id, version_id):
        return self._call("POST", "/api/knowledge-bases/" + urllib.parse.quote(kb_id, safe="") + "/rollback",
                          {"target_version_id": version_id})[1]

    # ---- QueryService ----
    def query(self, kb_id, vector, top_k, threshold=None, version_id=None, aggregation=None):
        body = {"knowledge_base_id": kb_id, "vector": vector, "top_k": top_k}
        if threshold is not None:
            body["threshold"] = threshold
        if version_id is not None:
            body["version_id"] = version_id
        if aggregation is not None:
            body["aggregation"] = aggregation
        return self._call("POST", "/api/query", body)[1]

    # ---- AdminService ----
    def health(self):
        return self._call("GET", "/api/health")[1]

    def system_status(self):
        return self._call("GET", "/api/system-status")[1]

    def rebuild(self, kb_id, version_id):
        return self._call("POST", "/api/knowledge-bases/" + urllib.parse.quote(kb_id, safe="") + "/rebuild",
                          {"version_id": version_id})[1]

    def warmup(self, kb_id, version_id):
        return self._call("POST", "/api/knowledge-bases/" + urllib.parse.quote(kb_id, safe="") + "/warmup",
                          {"version_id": version_id})[1]


# ---------------------------------------------------------------------------
# mock embed 算法复刻：ChunkID = SHA-256(文本 + model_id)，向量 = 对 ChunkID
# 做确定性哈希后归一化（与 integration/docker/mock_embed_server.go 一致）。
# 校验阶段用它复算查询向量，实现"文档原文片段 → 精确命中该文档"。
# ---------------------------------------------------------------------------
def mock_embed_vector(chunk_id: str, dim: int):
    """复刻 mock_embed_server.go 的 deterministicVector：float32 语义。"""
    h = hashlib.sha256(chunk_id.encode("ascii")).digest()
    vec = []
    for i in range(dim):
        b = h[(i + i // 32) % 32] / 255.0
        vec.append(struct.unpack("f", struct.pack("f", b))[0])
    norm = math.sqrt(sum(x * x for x in vec)) or 1.0
    return [struct.unpack("f", struct.pack("f", x / norm))[0] for x in vec]


def chunk_id_for(text: str, model_id: str) -> str:
    return hashlib.sha256((text + model_id).encode("utf-8")).hexdigest()


def query_vector_for(text: str, model_id: str, dim: int):
    """文本 → mock embed 的查询向量（等价于 embed 服务对该文本的嵌入结果）。"""
    return mock_embed_vector(chunk_id_for(text, model_id), dim)


def first_chunk_text(content: str, window: int) -> str:
    """复刻 splitter 的滑窗分块：返回文档的第一个 chunk 文本。"""
    if window <= 0 or len(content) <= window:
        return content
    return content[:window]


def num_chunks(content: str, window: int, overlap: int) -> int:
    """按 splitter 的滑窗规则估算一篇文档的 chunk 数（用于汇总展示）。"""
    n = len(content)
    if n == 0 or window <= 0:
        return 1 if n else 0
    if n <= window:
        return 1
    eff = overlap
    if eff < 0:
        eff = 0
    if eff >= window:
        eff = window - 1
    step = window - eff
    count, start = 0, 0
    while start < n:
        count += 1
        end = start + window
        if end >= n:
            break
        start += step
    return count


# ---------------------------------------------------------------------------
# 文档生成（确定性：同一种子输出相同内容）
# ---------------------------------------------------------------------------
def make_doc(rng: random.Random, domain_key: str, doc_index: int, revision: int = 0) -> str:
    for key, _label, sentences in DOMAINS:
        if key == domain_key:
            domain_sentences = sentences
            break
    else:
        domain_sentences = DOMAINS[0][2]
    title = f"{domain_key.upper()} 测试文档 {doc_index:04d}"
    n = rng.randint(12, 18)
    picked = [rng.choice(domain_sentences) for _ in range(n)]
    paragraphs = []
    for i in range(0, len(picked), 4):
        paragraphs.append("".join(picked[i:i + 4]))
    content = title + "\n\n" + "\n\n".join(paragraphs)
    if revision:
        content += f"\n\n——修订标记 v{revision}——"
    return content


# ---------------------------------------------------------------------------
# 进度与结果统计
# ---------------------------------------------------------------------------
class Stats:
    def __init__(self):
        self.passed = 0
        self.failed = 0
        self.warned = 0
        self.failures = []

    def check(self, label, ok, detail=""):
        if ok:
            self.passed += 1
            print(f"      [PASS] {label}")
        else:
            self.failed += 1
            self.failures.append((label, detail))
            print(f"      [FAIL] {label}  {detail}")

    def warn(self, label, detail=""):
        self.warned += 1
        print(f"      [WARN] {label}  {detail}")


def section(title: str):
    print(f"\n========== {title} ==========")


def plan_versions(args) -> list[dict]:
    """构造每个知识库的版本变更计划（按序执行，ADD/UPDATE/DELETE/混合全覆盖）。"""
    plan = []
    remaining = args.docs
    while remaining > 0:
        n = min(args.batch_size, remaining)
        plan.append({"kind": "load", "add": n, "update": 0, "delete": 0, "label": f"初始批量灌入({n}篇)"})
        remaining -= n
    if len(plan) < args.versions:
        plan.append({"kind": "add", "add": args.delta, "update": 0, "delete": 0, "label": "新增文档"})
    if len(plan) < args.versions:
        plan.append({"kind": "update", "add": 0, "update": max(1, args.delta // 2), "delete": 0, "label": "更新文档"})
    if len(plan) < args.versions:
        plan.append({"kind": "delete", "add": 0, "update": 0, "delete": max(1, args.delta // 4), "label": "删除文档"})
    if len(plan) < args.versions:
        plan.append({
            "kind": "mixed",
            "add": max(1, args.delta // 2),
            "update": max(1, args.delta // 3),
            "delete": max(1, args.delta // 4),
            "label": "混合变更(增/改/删)",
        })
    i = 0
    while len(plan) < args.versions:
        if i % 2 == 0:
            plan.append({"kind": "add", "add": args.delta, "update": 0, "delete": 0, "label": "新增文档"})
        else:
            plan.append({
                "kind": "mixed",
                "add": max(1, args.delta // 2),
                "update": max(1, args.delta // 3),
                "delete": max(1, args.delta // 4),
                "label": "混合变更(增/改/删)",
            })
        i += 1
    return plan[:args.versions]


# ---------------------------------------------------------------------------
# 等待与轮询
# ---------------------------------------------------------------------------
def wait_healthy(gw: Gateway, timeout: float):
    """等待网关可达且 raft 选出 leader（HealthCheck 返回 HEALTHY）。

    返回健康检查结果；若超时且从未成功连接，返回 None。
    """
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        try:
            h = gw.health()
        except ApiError as e:
            if e.http_code == -1:  # 网关尚未就绪，继续等
                time.sleep(2)
                continue
            raise
        last = h
        if h.get("status") == HEALTHY:
            return h
        time.sleep(2)
    return last


def wait_version_ready(gw: Gateway, kb_id: str, version_id: int, timeout: float, quiet: bool = False) -> str:
    """轮询 ListVersions 直到该版本 READY / FAILED / 超时，返回状态字符串。"""
    deadline = time.time() + timeout
    last = VERSION_PENDING
    while time.time() < deadline:
        try:
            versions = gw.list_versions(kb_id).get("versions", [])
        except ApiError as e:
            if not quiet:
                print(f"      (查询版本状态失败: {e}，重试中)")
            time.sleep(2)
            continue
        for v in versions:
            if v.get("version_id") == version_id:
                last = v.get("index_status", VERSION_PENDING)
                if last in (VERSION_READY, VERSION_FAILED):
                    return last
                break
        time.sleep(2)
    return f"timeout:{last}"


def wait_kb_deleted(gw: Gateway, kb_id: str, timeout: float = 120) -> str:
    """轮询 GetKnowledgeBase，返回 REMOVED / DELETING / DELETE_FAILED / timeout。"""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            kb = gw.get_kb(kb_id).get("knowledge_base", {})
            status = kb.get("status", KB_ACTIVE)
            if status != KB_ACTIVE:
                return status
        except ApiError as e:
            if e.http_code == 404:
                return "REMOVED"
        time.sleep(2)
    return "timeout"


# ---------------------------------------------------------------------------
# 知识库处理：创建 → 版本链（含逐版本校验）→ 当前状态校验
# ---------------------------------------------------------------------------
def build_changes(kb, plan_phase, rng, args):
    """按计划阶段构造 DocChange 列表并更新脚本侧的文档台账。

    返回 (changes, 元数据)，其中元数据含 update_records / delete_records
    供后续逐版本校验使用。
    """
    changes = []
    meta = {"updated": [], "deleted": []}
    docs = kb["docs"]
    before = set(docs.keys())  # 阶段开始前的文档集，用于判断"父版本中是否存在"

    def new_doc():
        kb["seq"] += 1
        doc_id = f"{kb['name']}-d{kb['seq']:06d}"
        content = make_doc(rng, kb["domain"], kb["seq"], revision=kb["revision"])
        docs[doc_id] = content
        return doc_id, content

    added = set()
    for _ in range(plan_phase["add"]):
        doc_id, content = new_doc()
        added.add(doc_id)
        changes.append({"op": OP_ADD, "doc_id": doc_id, "content": content})

    updated = set()
    update_k = min(plan_phase["update"], len(docs))
    if update_k > 0:
        for doc_id in rng.sample(sorted(docs.keys()), update_k):
            old = docs[doc_id]
            kb["revision"] += 1
            new_content = make_doc(rng, kb["domain"], int(doc_id.split("-d")[1]), revision=kb["revision"])
            docs[doc_id] = new_content
            updated.add(doc_id)
            changes.append({"op": OP_UPDATE, "doc_id": doc_id, "content": new_content})
            meta["updated"].append({
                "doc_id": doc_id, "old_content": old, "new_content": new_content,
                "existed_in_parent": doc_id in before,
            })

    # 删除只针对"父版本已存在、且本阶段未新增/未更新"的文档：这样被删文档的
    # content 就是它在父版本中的真实内容，父版本回查（MVCC）才能可靠命中；
    # 也避免"同版本先 ADD/UPDATE 再 DELETE"导致父版本校验逻辑失效。
    deletable = [d for d in docs if d not in added and d not in updated]
    delete_k = min(plan_phase["delete"], len(deletable))
    if delete_k > 0:
        for doc_id in rng.sample(sorted(deletable), delete_k):
            content = docs.pop(doc_id)
            changes.append({"op": OP_DELETE, "doc_id": doc_id, "content": ""})
            meta["deleted"].append({"doc_id": doc_id, "content": content})

    return changes, meta


def create_and_seed_kb(gw: Gateway, kb, args, stats: Stats, rng: random.Random):
    """创建知识库并灌入全部版本。返回是否成功（False 表示版本构建失败中断）。"""
    name, profile = kb["name"], kb["profile"]
    print(f"\n--- 知识库 {name}（{profile['label']}）---")
    resp = gw.create_kb(
        name, profile["window"], profile["overlap"],
        profile["index_type"], profile["similarity"],
        args.embed_addr, profile["model_id"],
    )
    kb_id = resp.get("knowledge_base_id")
    initial = resp.get("initial_version_id")
    kb["kb_id"] = kb_id
    kb["initial_version_id"] = initial

    got = gw.get_kb(kb_id).get("knowledge_base", {})
    stats.check(f"创建并回读 {name}",
                got.get("knowledge_base_id") == kb_id and got.get("status") == KB_ACTIVE,
                f"id={kb_id} got={got.get('knowledge_base_id')}")

    parent = initial
    versions = []
    for phase in plan_versions(args):
        try:
            changes, meta = build_changes(kb, phase, rng, args)
            resp = gw.create_version(kb_id, parent, changes)
        except ApiError as e:
            stats.check(f"{name} 创建版本({phase['label']})", False, f"{e}")
            return False
        vid = resp.get("version_id")
        t0 = time.time()
        status = wait_version_ready(gw, kb_id, vid, args.index_timeout, args.quiet)
        build_s = time.time() - t0
        if status != VERSION_READY:
            stats.check(f"{name} 版本{vid}({phase['label']})索引构建完成", False,
                        f"status={status}（可用 POST /rebuild 重试构建）")
            return False
        stats.check(f"{name} 版本{vid}({phase['label']})索引构建完成", True, f"{build_s:.1f}s")
        rr = gw.rollback(kb_id, vid)  # 发布：让新版本成为 active
        stats.check(f"{name} 发布(回滚至)版本{vid}", rr.get("success") is True, f"resp={rr}")
        versions.append({"version_id": vid, "phase": phase, "meta": meta, "build_s": build_s})

        # ---- 逐版本校验（此时父子版本索引都在内存，避免 LRU 淘汰影响）----
        for rec in meta["updated"]:
            verify_update(gw, kb, rec, vid, parent, args, stats)
        for rec in meta["deleted"]:
            verify_delete(gw, kb, rec, vid, parent, args, stats)
        parent = vid

    kb["versions"] = versions
    kb["active_version_id"] = parent
    verify_current_state(gw, kb, rng, args, stats)
    return True


def verify_update(gw: Gateway, kb, rec, vid, parent_vid, args, stats: Stats):
    name = kb["name"]
    label = f"{name} 更新文档 {rec['doc_id']}"
    vec = query_vector_for(first_chunk_text(rec["new_content"], kb["profile"]["window"]),
                           kb["profile"]["model_id"], args.dim)
    r = gw.query(kb["kb_id"], vec, 5, version_id=vid, aggregation=AGG_MAX)
    hit = next((x for x in r["results"] if x["doc_id"] == rec["doc_id"]), None)
    stats.check(f"{label} 新版本可检索到", hit is not None,
                f"results={[x['doc_id'] for x in r['results']]}")
    stats.check(f"{label} 内容为新版",
                hit is not None and hit["content"] == rec["new_content"],
                f"content={(hit['content'][:30] if hit else '无')!r}")
    if rec["existed_in_parent"]:
        vec_old = query_vector_for(first_chunk_text(rec["old_content"], kb["profile"]["window"]),
                                   kb["profile"]["model_id"], args.dim)
        rp = gw.query(kb["kb_id"], vec_old, 5, version_id=parent_vid, aggregation=AGG_MAX)
        hit_p = next((x for x in rp["results"] if x["doc_id"] == rec["doc_id"]), None)
        stats.check(f"{label} 父版本{parent_vid}仍可检索到旧文档", hit_p is not None,
                    f"results={[x['doc_id'] for x in rp['results']]}")
        stats.check(f"{label} 父版本内容仍为旧版",
                    hit_p is not None and hit_p["content"] == rec["old_content"],
                    f"content={(hit_p['content'][:30] if hit_p else '无')!r}")
    else:
        stats.check(f"{label} 父版本{parent_vid}无该文档(同版本新增+更新)", True, "跳过父版本回查")


def verify_delete(gw: Gateway, kb, rec, vid, parent_vid, args, stats: Stats):
    name = kb["name"]
    vec = query_vector_for(first_chunk_text(rec["content"], kb["profile"]["window"]),
                           kb["profile"]["model_id"], args.dim)
    rc = gw.query(kb["kb_id"], vec, 5, version_id=vid, aggregation=AGG_MAX)
    absent = all(x["doc_id"] != rec["doc_id"] for x in rc["results"])
    stats.check(f"{name} 删除文档 {rec['doc_id']} 在版本{vid}不可见", absent,
                f"results={[x['doc_id'] for x in rc['results']]}")
    rp = gw.query(kb["kb_id"], vec, 5, version_id=parent_vid, aggregation=AGG_MAX)
    top = rp["results"][0] if rp["results"] else None
    stats.check(f"{name} 删除文档在父版本{parent_vid}仍可见",
                top is not None and top["doc_id"] == rec["doc_id"],
                f"top={top['doc_id'] if top else None}")


def verify_current_state(gw: Gateway, kb, rng, args, stats: Stats):
    """对最新（active）版本做一轮查询校验：精确命中、阈值、聚合、错误路径。

    注意：mock embed 的确定性向量并非零均值（32 字节哈希拉伸到 768 维），
    任意向量间的余弦基线约 0.8，因此聚合后的分数普遍偏高。精确命中（1.0）
    仍是最高分，但 MEDIAN/MEAN 聚合会把差距拉小，所以：
      - 严格 top1+分数校验只用 MAX 聚合（精确 chunk 1.0 vs 随机 ~0.85，差距稳定）；
      - 其余聚合只断言"目标文档出现在 top-5 结果中"。
    """
    name, profile, kb_id = kb["name"], kb["profile"], kb["kb_id"]
    active = kb["active_version_id"]
    live = list(kb["docs"].keys())
    if not live:
        return
    samples = rng.sample(live, min(args.queries, len(live)))
    for doc_id in samples:
        content = kb["docs"][doc_id]
        vec = query_vector_for(first_chunk_text(content, profile["window"]), profile["model_id"], args.dim)
        # 严格校验：MAX 聚合下精确 chunk 分数 = 1.0，且必须是 top1
        rs = gw.query(kb_id, vec, 5, aggregation=AGG_MAX)
        top = rs["results"][0] if rs["results"] else None
        stats.check(f"{name} 精确查询 {doc_id} 命中 top1(MAX)",
                    top is not None and top["doc_id"] == doc_id,
                    f"top={top['doc_id'] if top else None}")
        stats.check(f"{name} 精确查询分数≈1.0(MAX)",
                    top is not None and top["score"] > 0.95,
                    f"score={top['score'] if top else None}")
        # 默认（MEDIAN）聚合：文档出现在 top-5 即可（分数基线偏高，见函数注释）
        r = gw.query(kb_id, vec, 5)
        stats.check(f"{name} 精确查询 {doc_id} 出现在结果中",
                    any(x["doc_id"] == doc_id for x in r["results"]),
                    f"results={[x['doc_id'] for x in r['results']]}")
        stats.check(f"{name} 默认查询使用 active 版本",
                    r.get("version_id") == active,
                    f"version_id={r.get('version_id')} active={active}")
        rt = gw.query(kb_id, vec, 5, threshold=0.95)
        stats.check(f"{name} threshold=0.95 仍能命中",
                    any(x["doc_id"] == doc_id for x in rt["results"]),
                    f"results={[x['doc_id'] for x in rt['results']]}")
        rh = gw.query(kb_id, vec, 5, threshold=1.5)
        stats.check(f"{name} threshold=1.5(超上限)返回空",
                    len(rh["results"]) == 0,
                    f"results={[x['doc_id'] for x in rh['results']]}")
        for agg in (AGG_MEAN, AGG_MEDIAN):
            ra = gw.query(kb_id, vec, 5, aggregation=agg)
            present = any(x["doc_id"] == doc_id for x in ra["results"])
            stats.check(f"{name} aggregation={agg} 结果包含目标文档", present,
                        f"results={[x['doc_id'] for x in ra['results']]}")
    try:
        gw.query(kb_id, query_vector_for("不存在的查询文本", profile["model_id"], args.dim),
                 5, version_id=99999999)
        stats.check(f"{name} 查询不存在的版本应报错", False, "未报错")
    except ApiError as e:
        stats.check(f"{name} 查询不存在的版本应报错",
                    e.http_code in (404, 412),
                    f"http={e.http_code} grpc={e.grpc_code}")


# ---------------------------------------------------------------------------
# 管理功能演示
# ---------------------------------------------------------------------------
def admin_checks(gw: Gateway, args, stats: Stats, kbs):
    section("管理功能：HealthCheck / GetSystemStatus")
    h = gw.health()
    status = h.get("status")
    stats.check("HealthCheck 状态", status in (HEALTHY, DEGRADED), f"status={status} details={h.get('details')}")
    if status == DEGRADED:
        stats.warn("HealthCheck 为 DEGRADED", h.get("details", ""))

    ss = gw.system_status()
    ss_health = ss.get("health", {})
    stuck = ss.get("stuck_versions", [])
    del_failed = ss.get("delete_failed_kbs", [])
    wal_alerts = ss.get("wal_alerts", [])
    ru = ss.get("resource_usage", {})
    print(f"      GetSystemStatus: health={ss_health.get('status')} "
          f"stuck_versions={len(stuck)} delete_failed={len(del_failed)} "
          f"wal_alerts={len(wal_alerts)} "
          f"loaded_indexes={ru.get('loaded_index_count')} "
          f"doc_store={_fmt_bytes(ru.get('doc_store_bytes', 0))} "
          f"chunk_store={_fmt_bytes(ru.get('chunk_store_bytes', 0))}")
    stats.check("GetSystemStatus 无卡死版本", len(stuck) == 0,
                f"stuck={stuck}")
    stats.check("GetSystemStatus 无删除失败库", len(del_failed) == 0, f"delete_failed={del_failed}")
    stats.check("GetSystemStatus 无 WAL 告警", len(wal_alerts) == 0, f"wal_alerts={wal_alerts}")


def rebuild_demo(gw: Gateway, args, stats: Stats, kbs):
    section("管理功能：RebuildIndex")
    kb = kbs[0]
    vid = kb["versions"][-1]["version_id"]
    resp = gw.rebuild(kb["kb_id"], vid)
    stats.check(f"触发 RebuildIndex({kb['name']} v{vid})", resp.get("success") is True, f"resp={resp}")
    status = wait_version_ready(gw, kb["kb_id"], vid, args.index_timeout, args.quiet)
    stats.check(f"重建后版本恢复 READY", status == VERSION_READY, f"status={status}")


def warmup_demo(gw: Gateway, args, stats: Stats, kbs):
    section("管理功能：WarmupVersion（把最早版本的索引重新加载进内存）")
    kb = kbs[0]
    vid = kb["versions"][0]["version_id"]  # 最早版本，大概率已被 LRU 淘汰
    resp = gw.warmup(kb["kb_id"], vid)
    stats.check(f"触发 WarmupVersion({kb['name']} v{vid})", resp.get("success") is True, f"resp={resp}")
    # 重建是异步的且状态保持 READY，无法轮询状态；直接重试查询直到可用。
    vec = query_vector_for("warmup 探测查询", kb["profile"]["model_id"], args.dim)
    deadline = time.time() + 120
    ok = False
    last_err = ""
    while time.time() < deadline:
        try:
            r = gw.query(kb["kb_id"], vec, 3)
            if "results" in r:
                ok = True
                break
        except ApiError as e:
            last_err = str(e)
            if "index" not in str(e).lower() and "ready" not in str(e).lower() and e.http_code != 500:
                break  # 非索引加载类错误，停止重试
        time.sleep(3)
    stats.check(f"Warmup 后 v{vid} 可查询", ok, f"last_err={last_err or 'ok'}")


def rollback_demo(gw: Gateway, args, stats: Stats, kbs):
    """回滚演示：选择最后一个知识库（其全部版本索引仍在 LRU 内）。

    回滚到最早的版本 → 已被删除的文档重新可见；回滚回最新 → 再次不可见。
    """
    section("版本回滚演示（RollbackVersion 的 MVCC 语义）")
    kb = kbs[-1]
    deleted_rec = None
    for v in kb["versions"]:
        if v["meta"]["deleted"]:
            deleted_rec = v["meta"]["deleted"][0]
            break
    if deleted_rec is None:
        stats.warn("回滚演示跳过", "该知识库没有删除过文档")
        return
    target = kb["versions"][0]["version_id"]
    gw.rollback(kb["kb_id"], target)
    got = gw.get_kb(kb["kb_id"]).get("knowledge_base", {})
    stats.check(f"回滚到最早版本 v{target} 后 active 切换",
                got.get("active_version_id") == target,
                f"active={got.get('active_version_id')}")
    vec = query_vector_for(first_chunk_text(deleted_rec["content"], kb["profile"]["window"]),
                           kb["profile"]["model_id"], args.dim)
    r = gw.query(kb["kb_id"], vec, 5)
    stats.check(f"回滚后已删除文档 {deleted_rec['doc_id']} 重新可见",
                any(x["doc_id"] == deleted_rec["doc_id"] for x in r["results"]),
                f"results={[x['doc_id'] for x in r['results']]}")
    latest = kb["active_version_id"]
    gw.rollback(kb["kb_id"], latest)
    got = gw.get_kb(kb["kb_id"]).get("knowledge_base", {})
    stats.check(f"回滚回最新版本 v{latest}", got.get("active_version_id") == latest,
                f"active={got.get('active_version_id')}")
    r2 = gw.query(kb["kb_id"], vec, 5)
    stats.check(f"回滚回最新后文档再次不可见",
                all(x["doc_id"] != deleted_rec["doc_id"] for x in r2["results"]),
                f"results={[x['doc_id'] for x in r2['results']]}")


def delete_demo(gw: Gateway, args, stats: Stats):
    section("删除知识库演示（DeleteKnowledgeBase）")
    resp = gw.create_kb(
        "gen-delete-demo", 256, 32, "INDEX_TYPE_HNSW", "SIMILARITY_COSINE",
        args.embed_addr, "mock-embed-del",
    )
    kb_id = resp.get("knowledge_base_id")
    rng = random.Random(args.seed + 999)
    changes = []
    for i in range(20):
        doc_id = f"gen-delete-demo-d{i:06d}"
        changes.append({"op": OP_ADD, "doc_id": doc_id,
                        "content": make_doc(rng, "infra", i)})
    resp = gw.create_version(kb_id, resp.get("initial_version_id"), changes)
    vid = resp.get("version_id")
    status = wait_version_ready(gw, kb_id, vid, args.index_timeout, args.quiet)
    stats.check("删除演示库版本就绪", status == VERSION_READY, f"status={status}")
    d = gw.delete_kb(kb_id)
    stats.check("DeleteKnowledgeBase 返回 success", d.get("success") is True, f"resp={d}")
    final = wait_kb_deleted(gw, kb_id)
    stats.check("删除异步清理完成（KB 被移除或进入 DELETING）",
                final in ("REMOVED", "KB_STATUS_DELETING"),
                f"final={final}")
    if final == "KB_STATUS_DELETE_FAILED":
        stats.warn("删除进入 DELETE_FAILED", "清理失败，请查看服务日志")


# ---------------------------------------------------------------------------
# 主流程
# ---------------------------------------------------------------------------
def _fmt_bytes(n) -> str:
    try:
        n = int(n)
    except (TypeError, ValueError):
        return str(n)
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024:
            return f"{n:.1f}{unit}"
        n /= 1024
    return f"{n:.1f}TB"


def build_profiles(args) -> list[dict]:
    """构造 --kbs 个知识库的配置（轮换索引类型 / 相似度 / 分块参数 / 模型）。"""
    base = [
        {"index_type": "INDEX_TYPE_HNSW", "similarity": "SIMILARITY_COSINE",
         "window": 512, "overlap": 64, "model_id": "mock-embed-v1", "label": "HNSW+COSINE"},
        {"index_type": "INDEX_TYPE_HNSW", "similarity": "SIMILARITY_EUCLIDEAN",
         "window": 256, "overlap": 32, "model_id": "mock-embed-v2", "label": "HNSW+EUCLIDEAN"},
        {"index_type": "INDEX_TYPE_HNSW", "similarity": "SIMILARITY_INNER_PRODUCT",
         "window": 1024, "overlap": 128, "model_id": "mock-embed-v3", "label": "HNSW+INNER_PRODUCT"},
        {"index_type": "INDEX_TYPE_IVF", "similarity": "SIMILARITY_COSINE",
         "window": 512, "overlap": 64, "model_id": "mock-embed-v4", "label": "IVF(元数据)+COSINE"},
        {"index_type": "INDEX_TYPE_FLAT", "similarity": "SIMILARITY_EUCLIDEAN",
         "window": 384, "overlap": 48, "model_id": "mock-embed-v5", "label": "FLAT(元数据)+EUCLIDEAN"},
    ]
    profiles = []
    for i in range(args.kbs):
        p = dict(base[i % len(base)])
        if args.kbs > len(base):
            p["model_id"] = f"mock-embed-v{i + 1}"
        profiles.append(p)
    return profiles


def print_plan(args):
    profiles = build_profiles(args)
    plan = plan_versions(args)
    total_docs = 0
    total_chunks = 0
    rng = random.Random(args.seed)
    for i, p in enumerate(profiles):
        kb_docs = args.docs
        kb_chunks = 0
        for _ in range(kb_docs):
            content = make_doc(rng, DOMAINS[i % len(DOMAINS)][0], 0)
            kb_chunks += num_chunks(content, p["window"], p["overlap"])
        total_docs += kb_docs
        total_chunks += kb_chunks
    versions_total = args.kbs * len(plan)
    est_s = versions_total * 6 + total_docs * 0.01
    print("========== 计划（--dry-run）==========")
    print(f"网关: {args.gateway}   embed: {args.embed_addr}   向量维度: {args.dim}")
    print(f"知识库: {args.kbs} 个    每个初始文档: {args.docs} 篇    每库版本数: {len(plan)}")
    print(f"预计文档总量: {total_docs} 篇    预计 chunk 总量: ~{total_chunks} 个向量")
    print(f"预计耗时: ~{est_s / 60:.0f} 分钟（视机器性能浮动）")
    print("\n知识库配置轮换:")
    for i, p in enumerate(profiles):
        print(f"  {i + 1:>2}. {p['label']:<22} window={p['window']:<4} overlap={p['overlap']:<3} "
              f"model={p['model_id']}")
    print("\n每个知识库的版本变更计划:")
    for i, ph in enumerate(plan):
        print(f"  v{i + 1:>2}: {ph['label']}（ADD {ph['add']} / UPDATE {ph['update']} / DELETE {ph['delete']}）")
    print("\n功能覆盖: 创建/查询/删除知识库、版本链(ADD/UPDATE/DELETE/混合)、发布回滚、")
    print("          查询(top_k/threshold/聚合/指定版本/错误路径)、健康检查、系统状态、")
    print("          RebuildIndex、WarmupVersion、知识库删除清理")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="生成 Stratum 大规模测试数据（多知识库、大文档量、覆盖主要测试功能）",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--gateway", default=DEFAULT_GATEWAY, help="stratum-gateway 地址")
    parser.add_argument("--embed-addr", default=DEFAULT_EMBED_ADDR, help="写入知识库元数据的 embed 服务地址")
    parser.add_argument("--dim", type=int, default=DEFAULT_DIM, help="向量维度（须与 mock embed 的 VEC_DIM 一致）")
    parser.add_argument("--kbs", type=int, default=10, help="创建的知识库数量")
    parser.add_argument("--docs", type=int, default=2000, help="每个知识库初始文档数（大数据量）")
    parser.add_argument("--batch-size", type=int, default=1000,
                        help="单次 CreateVersion 的文档数上限（避免超过 gRPC 消息大小限制）")
    parser.add_argument("--delta", type=int, default=100, help="后续每个版本的增量文档数")
    parser.add_argument("--versions", type=int, default=6, help="每个知识库的版本总数（含初始批量）")
    parser.add_argument("--queries", type=int, default=3, help="每个知识库当前状态精确查询的文档数")
    parser.add_argument("--seed", type=int, default=42, help="随机种子（保证内容可复现）")
    parser.add_argument("--index-timeout", type=float, default=900, help="等待单个版本索引构建完成的最长秒数")
    parser.add_argument("--leader-timeout", type=float, default=60, help="等待 raft 选出 leader 的最长秒数")
    parser.add_argument("--no-verify", action="store_true", help="跳过查询校验（非 mock embed 时使用）")
    parser.add_argument("--no-delete", action="store_true", help="跳过删除知识库演示")
    parser.add_argument("--dry-run", action="store_true", help="只打印计划，不连接服务")
    parser.add_argument("--quiet", action="store_true", help="减少冗余输出")
    args = parser.parse_args()

    if args.kbs < 1 or args.docs < 1 or args.versions < 1:
        raise SystemExit("--kbs / --docs / --versions 必须 >= 1")
    if args.batch_size < 1:
        raise SystemExit("--batch-size 必须 >= 1")

    if args.dry_run:
        return print_plan(args)

    gw = Gateway(args.gateway)
    stats = Stats()

    section("前置检查")
    try:
        h = wait_healthy(gw, args.leader_timeout)
        if h is None:
            print(f"      在 {args.leader_timeout:.0f} 秒内无法连接网关 {args.gateway}")
            print("      请先运行 ./start.sh 启动服务后再执行本脚本。")
            return 1
        print(f"      HealthCheck: {h.get('status')} ({h.get('details')})")
        if h.get("status") != HEALTHY:
            print(f"      ⚠ raft 仍未就绪（{h.get('status')}），后续写操作会触发自动重试。")
    except ApiError as e:
        print(f"      无法连接网关 {args.gateway}: {e}")
        print("      请先运行 ./start.sh 启动服务后再执行本脚本。")
        return 1
    if not args.no_verify:
        try:
            probe = urllib.request.urlopen(args.embed_addr.rstrip("/") + "/health", timeout=5).read().decode()
            print(f"      embed 服务探测: {args.embed_addr} -> {probe.strip() or 'ok'}")
        except Exception as e:
            stats.warn("embed 服务不可达", f"{args.embed_addr}: {e}（查询校验可能失败）")

    profiles = build_profiles(args)
    kbs = []
    rng = random.Random(args.seed)
    section(f"创建 {args.kbs} 个知识库")
    for i, p in enumerate(profiles):
        domain = DOMAINS[i % len(DOMAINS)][0]
        kb = {
            "name": f"gen-{domain}-{i + 1:02d}",
            "domain": domain,
            "profile": p,
            "kb_id": None,
            "seq": 0,
            "revision": 0,
            "docs": {},
            "versions": [],
            "active_version_id": None,
        }
        if not create_and_seed_kb(gw, kb, args, stats, rng):
            stats.warn(f"知识库 {kb['name']} 处理中断", "后续阶段跳过该库")
        else:
            kbs.append(kb)

    if not kbs:
        print("\n没有任何知识库成功创建，中止。")
        return 1

    section("校验 ListKnowledgeBases")
    listed = gw.list_kbs().get("knowledge_bases", [])
    stats.check(f"ListKnowledgeBases 至少包含 {len(kbs)} 个测试库",
                len(listed) >= len(kbs), f"实际 {len(listed)} 个")

    if not args.no_verify:
        rollback_demo(gw, args, stats, kbs)
    else:
        section("跳过版本回滚演示（--no-verify）")

    admin_checks(gw, args, stats, kbs)
    rebuild_demo(gw, args, stats, kbs)
    warmup_demo(gw, args, stats, kbs)

    if not args.no_delete:
        delete_demo(gw, args, stats)
    else:
        section("跳过删除知识库演示（--no-delete）")

    # ---------- 汇总 ----------
    section("汇总")
    total_docs = sum(len(kb["docs"]) for kb in kbs)
    total_versions = sum(len(kb["versions"]) for kb in kbs)
    print(f"{'知识库':<24}{'配置':<22}{'存活文档':>8}{'版本数':>6}")
    for kb in kbs:
        print(f"{kb['name']:<24}{kb['profile']['label']:<22}{len(kb['docs']):>8}{len(kb['versions']):>6}")
    print(f"\n成功创建知识库: {len(kbs)}    版本总数: {total_versions}    当前存活文档: {total_docs}")
    print(f"校验结果: PASS {stats.passed} / FAIL {stats.failed} / WARN {stats.warned}")
    if stats.failures:
        print("失败明细:")
        for label, detail in stats.failures:
            print(f"  - {label}: {detail}")
    print("\n下一步:")
    print("  1) 打开 Web 控制台 http://localhost:8081/ 查看知识库与版本链")
    print("  2) 查看服务日志 run/log/stratum.log、run/log/vecstore.log")
    print("  3) 想清空数据重来：先停服务，再 python3 scripts/delete_test_db.py，然后 ./start.sh")
    return 1 if stats.failed else 0


if __name__ == "__main__":
    sys.exit(main())
