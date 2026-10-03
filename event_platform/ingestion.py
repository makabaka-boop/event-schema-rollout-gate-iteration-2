"""接入模块：按生产者声明的版本验证事件，验证通过后入库并生成投影。

不变量：
- 事件先按声明的原版本验证（不是当前可见版本），过渡期内旧版本仍可写；
- 无效事件进入隔离区，事件库与所有消费者投影都不留痕迹；
- 投影先为全部活跃消费者构建完毕，再与入库在同一临界区内一次提交，
  任何中途失败都不会留下部分投影。

隔离事件更正重放（运营修正生产者填错的字段）：
- 提交：隔离记录标识、预期记录修订、声明的模式版本、更正后的字段值；
- 先按声明版本重新校验，再对同一时刻快照出的仍活跃消费者集合检查映射
  能否满足并构建全部投影；
- 全部步骤成功后，才在一次提交中发布正式事件、各消费者投影，并把隔离
  记录翻转为已处理（修订号 +1），原失败内容原样保留供审计；
- 校验失败、修订冲突、消费者映射无法满足或投影构建异常 -> 拒绝且不留
  任何正式数据；
- 相同更正请求（四元组完全一致）的重试返回原结果（含原回执），不同内容
  的请求不能借用旧成功回执。

整个重放与发布/回退/弃用/消费者升级共用平台门面的同一把锁，校验所用
版本状态与投影所用消费者视图不可能来自两个时刻。
"""
from __future__ import annotations

import enum
import hashlib
import threading
from typing import Any, Dict, List, Optional, Tuple

from .consumers import ConsumerManager
from .model import CorrectionError, SchemaVersion, ValidationError
from .registry import SchemaRegistry, find_compatibility_problems


class IngestStatus(enum.Enum):
    ACCEPTED = "accepted"
    QUARANTINED = "quarantined"


class IngestResult:
    def __init__(self, status: IngestStatus, event_id: str, reason: Optional[str] = None):
        self.status = status
        self.event_id = event_id
        self.reason = reason

    @property
    def accepted(self) -> bool:
        return self.status is IngestStatus.ACCEPTED

    def __repr__(self) -> str:  # pragma: no cover
        return f"IngestResult({self.status.value}, {self.event_id!r}, {self.reason!r})"


class QuarantineStatus(enum.Enum):
    """隔离记录的处理状态。未更正的记录永远是 PENDING（旧行为不变）。"""

    PENDING = "pending"
    CORRECTED = "corrected"


class CorrectionStatus(enum.Enum):
    ACCEPTED = "accepted"
    REJECTED = "rejected"


class StoredEvent:
    def __init__(self, event_id: str, producer_id: str, schema_version: int,
                 payload: Dict[int, Any], corrected: bool = False):
        self.event_id = event_id
        self.producer_id = producer_id
        self.schema_version = schema_version
        self.payload = payload  # 已套用默认值的规范化负载
        self.corrected = corrected  # 是否由隔离更正重放接入

    def __repr__(self) -> str:  # pragma: no cover
        return f"StoredEvent({self.event_id!r}, v{self.schema_version}, {self.payload!r})"


class QuarantineRecord:
    """一条隔离记录。

    除最初的失败内容外，还带有供乐观裁决的修订号与处理状态；更正成功后
    记录本身保留在隔离区（审计），``payload``/``reason`` 原样不变，更正
    信息写在单独的属性上。
    """

    def __init__(self, event_id: str, producer_id: str, schema_version: int,
                 payload: Dict[int, Any], reason: str):
        self.event_id = event_id
        self.producer_id = producer_id
        self.schema_version = schema_version
        self.payload = payload  # 原始失败负载，更正后也不覆盖
        self.reason = reason  # 原始失败原因，更正后也不覆盖
        self.revision = 0
        self.status = QuarantineStatus.PENDING
        self.corrected_event_id: Optional[str] = None
        self.corrected_schema_version: Optional[int] = None
        self.corrected_payload: Optional[Dict[int, Any]] = None
        self.receipt: Optional["CorrectionReceipt"] = None

    @property
    def processed(self) -> bool:
        return self.status is QuarantineStatus.CORRECTED

    def __repr__(self) -> str:  # pragma: no cover
        return f"QuarantineRecord({self.event_id!r}, v{self.schema_version}, {self.status.value})"


class CorrectionReceipt:
    """更正成功的正式回执；与请求指纹绑定，不同内容拿不到它。"""

    def __init__(self, receipt_id: str, quarantine_id: str, event_id: str,
                 revision: int, schema_version: int, payload: Dict[int, Any],
                 request_fingerprint: str):
        self.receipt_id = receipt_id
        self.quarantine_id = quarantine_id
        self.event_id = event_id
        self.revision = revision  # 更正后记录的修订号
        self.schema_version = schema_version  # 更正时声明并校验的版本
        self.payload = payload  # 套用默认值后的更正负载
        self.request_fingerprint = request_fingerprint

    def __repr__(self) -> str:  # pragma: no cover
        return f"CorrectionReceipt({self.receipt_id!r}, {self.quarantine_id!r}, rev{self.revision})"


class CorrectionResult:
    def __init__(self, status: CorrectionStatus, quarantine_id: str,
                 receipt: Optional[CorrectionReceipt] = None, reason: Optional[str] = None):
        self.status = status
        self.quarantine_id = quarantine_id
        self.receipt = receipt
        self.reason = reason

    @property
    def accepted(self) -> bool:
        return self.status is CorrectionStatus.ACCEPTED

    def __repr__(self) -> str:  # pragma: no cover
        return f"CorrectionResult({self.status.value}, {self.quarantine_id!r}, {self.reason!r})"


def build_projection(requirements: Dict[int, Any], normalized: Dict[int, Any]) -> Dict[int, Any]:
    """按消费者固定的字段 ID 抽取投影（以 ID 为键，重命名透明）。"""
    return {fid: normalized.get(fid) for fid in requirements}


def _canon_value(value: Any) -> Tuple[Any, ...]:
    """给值打类型标签后再参与指纹，避免 Python 中 True == 1、1 == 1.0 混淆。"""
    if isinstance(value, bool):
        return ("bool", value)
    if isinstance(value, int):
        return ("int", value)
    if isinstance(value, float):
        return ("float", value)
    if value is None:
        return ("null",)
    if isinstance(value, str):
        return ("string", value)
    return ("other", repr(value))


def _canon_key(key: Any) -> Tuple[Any, ...]:
    if isinstance(key, bool):
        return ("bool", key)
    if isinstance(key, int):
        return ("int", key)
    if isinstance(key, str):
        return ("string", key)
    return ("other", repr(key))


def correction_fingerprint(quarantine_id: str, expected_revision: int,
                           schema_version: int, corrected_payload: Dict[int, Any]) -> str:
    """更正请求四元组的稳定指纹：四者完全一致才算同一个请求。"""
    body = (
        quarantine_id,
        expected_revision,
        schema_version,
        tuple(sorted((_canon_key(k), _canon_value(v)) for k, v in corrected_payload.items())),
    )
    digest = hashlib.sha256(repr(body).encode("utf-8")).hexdigest()
    return f"corr-{digest[:16]}"


class IngestionEngine:
    def __init__(self, lock: threading.RLock, registry: SchemaRegistry, consumers: ConsumerManager):
        self._lock = lock
        self._registry = registry
        self._consumers = consumers
        self._store: List[StoredEvent] = []
        self._quarantined: List[QuarantineRecord] = []
        self._quarantine_index: Dict[str, QuarantineRecord] = {}
        self._seen_ids: set[str] = set()
        # 请求指纹 -> 首次请求的裁决结果（成功或拒绝都登记，重试原样返回）
        self._correction_outcomes: Dict[str, CorrectionResult] = {}
        self._receipt_seq = 0

    # ---------- 查询 ----------

    @property
    def stored_events(self) -> Tuple[StoredEvent, ...]:
        with self._lock:
            return tuple(self._store)

    @property
    def quarantined_events(self) -> Tuple[QuarantineRecord, ...]:
        with self._lock:
            return tuple(self._quarantined)

    def get_quarantine(self, quarantine_id: str) -> Optional[QuarantineRecord]:
        with self._lock:
            return self._quarantine_index.get(quarantine_id)

    # ---------- 验证 ----------

    @staticmethod
    def validate_event(sv: SchemaVersion, payload: Dict[int, Any]) -> Tuple[List[str], Dict[int, Any]]:
        """按声明版本验证负载。返回 (错误列表, 套用默认值后的规范化负载)。"""
        errors: List[str] = []
        normalized: Dict[int, Any] = {}
        for key in payload:
            if isinstance(key, bool) or not isinstance(key, int) or key not in sv.fields:
                errors.append(f"版本 v{sv.version} 中不存在字段 {key!r}")
        for fid, f in sv.fields.items():
            if fid in payload:
                value = payload[fid]
                if not f.type.accepts(value):
                    errors.append(
                        f"字段 {fid}({f.name}) 期望 {f.type.value}，实际值 {value!r}"
                    )
                else:
                    normalized[fid] = value
            elif f.required:
                errors.append(f"缺少必需字段 {fid}({f.name})")
            elif f.default is not None:
                normalized[fid] = f.default
        return errors, normalized

    # ---------- 接入 ----------

    def ingest(self, event_id: str, producer_id: str, schema_version: int,
               payload: Dict[int, Any]) -> IngestResult:
        with self._lock:
            if event_id in self._seen_ids:
                raise ValueError(f"重复事件 ID: {event_id!r}")
            self._seen_ids.add(event_id)  # 事件 ID 只消费一次，无论接受还是隔离

            sv = self._registry.get_version(schema_version)
            if sv is None or not self._registry.is_accepting(schema_version):
                return self._quarantine(
                    event_id, producer_id, schema_version, payload,
                    f"模式版本 v{schema_version} 不存在或已弃用",
                )

            errors, normalized = self.validate_event(sv, payload)
            if errors:
                return self._quarantine(
                    event_id, producer_id, schema_version, payload, "；".join(errors)
                )

            # 先为全部活跃消费者构建投影，全部成功后才提交 —— 不留部分投影
            try:
                projections = {
                    view.consumer_id: build_projection(view.requirements, normalized)
                    for view in self._consumers.active_views()
                }
            except Exception as exc:
                return self._quarantine(
                    event_id, producer_id, schema_version, payload,
                    f"投影构建失败: {exc}",
                )

            # 原子提交：入库与所有消费者投影在同一临界区内完成
            self._store.append(StoredEvent(event_id, producer_id, schema_version, normalized))
            self._consumers.commit_projections(event_id, schema_version, projections)
            return IngestResult(IngestStatus.ACCEPTED, event_id)

    # ---------- 隔离事件更正重放 ----------

    def replay_correction(
        self,
        quarantine_id: str,
        expected_revision: int,
        schema_version: int,
        corrected_payload: Dict[int, Any],
    ) -> CorrectionResult:
        """重放一条隔离记录的更正请求。

        所有裁决与发布在共享锁的同一临界区内完成；任何一步失败都不写入
        正式事件/投影，也不推进隔离记录修订。相同四元组的重试返回首次
        请求的原结果。
        """
        if isinstance(expected_revision, bool) or not isinstance(expected_revision, int) \
                or expected_revision < 0:
            raise ValidationError(f"预期记录修订必须是非负整数: {expected_revision!r}")
        if isinstance(schema_version, bool) or not isinstance(schema_version, int):
            raise ValidationError(f"声明的模式版本必须是整数: {schema_version!r}")
        if not isinstance(corrected_payload, dict):
            raise ValidationError("更正后的字段值必须是字段 ID 到值的映射")

        with self._lock:
            fingerprint = correction_fingerprint(
                quarantine_id, expected_revision, schema_version, corrected_payload
            )
            # 相同请求的重试：无论首次是成功还是拒绝，原样返回首次裁决
            cached = self._correction_outcomes.get(fingerprint)
            if cached is not None:
                return cached

            record = self._quarantine_index.get(quarantine_id)
            if record is None:
                raise CorrectionError(f"隔离记录 {quarantine_id!r} 不存在")

            # 修订裁决：已处理记录拒绝任何新内容；修订号不符即冲突
            if record.status is QuarantineStatus.CORRECTED:
                return self._reject_correction(
                    fingerprint, quarantine_id,
                    f"隔离记录 {quarantine_id!r} 已更正（修订 {record.revision}），不能再次处理",
                )
            if record.revision != expected_revision:
                return self._reject_correction(
                    fingerprint, quarantine_id,
                    f"修订冲突：预期修订 {expected_revision}，"
                    f"记录 {quarantine_id!r} 当前修订 {record.revision}",
                )

            # 按声明版本（不是当前可见版本）重新校验
            sv = self._registry.get_version(schema_version)
            if sv is None or not self._registry.is_accepting(schema_version):
                return self._reject_correction(
                    fingerprint, quarantine_id,
                    f"模式版本 v{schema_version} 不存在或已弃用",
                )

            errors, normalized = self.validate_event(sv, corrected_payload)
            if errors:
                return self._reject_correction(
                    fingerprint, quarantine_id, "；".join(errors)
                )

            # 同一时刻仍活跃消费者的快照：映射检查与投影构建都基于这同一集合
            views = self._consumers.active_views()
            problems = find_compatibility_problems(sv.fields, views)
            if problems:
                return self._reject_correction(
                    fingerprint, quarantine_id,
                    "消费者映射无法满足：" + "；".join(problems),
                )

            try:
                projections = {
                    view.consumer_id: build_projection(view.requirements, normalized)
                    for view in views
                }
            except Exception as exc:
                return self._reject_correction(
                    fingerprint, quarantine_id, f"投影构建失败: {exc}"
                )

            # ---- 全部步骤成功，一次发布：正式事件 + 全部投影 + 已处理状态 ----
            self._receipt_seq += 1
            new_revision = record.revision + 1
            receipt = CorrectionReceipt(
                receipt_id=f"receipt-{self._receipt_seq}",
                quarantine_id=quarantine_id,
                event_id=record.event_id,
                revision=new_revision,
                schema_version=schema_version,
                payload=dict(normalized),
                request_fingerprint=fingerprint,
            )
            self._store.append(
                StoredEvent(record.event_id, record.producer_id, schema_version,
                            normalized, corrected=True)
            )
            self._consumers.commit_projections(record.event_id, schema_version, projections)
            record.status = QuarantineStatus.CORRECTED
            record.revision = new_revision
            record.corrected_event_id = record.event_id
            record.corrected_schema_version = schema_version
            record.corrected_payload = dict(normalized)
            record.receipt = receipt

            result = CorrectionResult(CorrectionStatus.ACCEPTED, quarantine_id, receipt)
            self._correction_outcomes[fingerprint] = result
            return result

    # ---------- 内部 ----------

    def _quarantine(self, event_id: str, producer_id: str, schema_version: int,
                    payload: Dict[int, Any], reason: str) -> IngestResult:
        record = QuarantineRecord(event_id, producer_id, schema_version, dict(payload), reason)
        self._quarantined.append(record)
        self._quarantine_index[event_id] = record
        return IngestResult(IngestStatus.QUARANTINED, event_id, reason)

    def _reject_correction(self, fingerprint: str, quarantine_id: str,
                           reason: str) -> CorrectionResult:
        """登记一次失败裁决：不动任何正式数据，供相同请求重试时原样返回。"""
        result = CorrectionResult(CorrectionStatus.REJECTED, quarantine_id, reason=reason)
        self._correction_outcomes[fingerprint] = result
        return result
