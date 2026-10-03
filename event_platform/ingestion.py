"""接入模块：按生产者声明的版本验证事件，验证通过后入库并生成投影。

不变量：
- 事件先按声明的原版本验证（不是当前可见版本），过渡期内旧版本仍可写；
- 无效事件进入隔离区，事件库与所有消费者投影都不留痕迹；
- 投影先为全部活跃消费者构建完毕，再与入库在同一临界区内一次提交，
  任何中途失败都不会留下部分投影；
- 隔离记录可由运营更正重放：按声明版本重新校验、基于同一时刻仍活跃的
  消费者集合构建全部投影，全部成功后一次提交正式事件、全部投影与记录的
  已处理状态，原失败内容保留供审计；更正请求幂等 —— 相同请求重试返回
  原结果，同一标识携带不同内容一律拒绝。
"""
from __future__ import annotations

import enum
import threading
from typing import Any, Dict, List, Optional, Tuple

from .consumers import ConsumerManager
from .model import SchemaError, SchemaVersion
from .registry import SchemaRegistry


class IngestStatus(enum.Enum):
    ACCEPTED = "accepted"
    QUARANTINED = "quarantined"


class QuarantineStatus(enum.Enum):
    PENDING = "pending"    # 待处理：仍在隔离区等待更正
    RESOLVED = "resolved"  # 已处理：更正重放成功，事件已重新接入


class CorrectionStatus(enum.Enum):
    RESOLVED = "resolved"  # 更正成功：正式事件与各消费者投影已一次发布
    REJECTED = "rejected"  # 更正失败：未留任何正式数据，记录保持待处理


class RevisionConflictError(SchemaError):
    """隔离记录的预期修订与实际修订冲突（记录已被其他更正处理过）。"""


class StoredEvent:
    def __init__(self, event_id: str, producer_id: str, schema_version: int, payload: Dict[int, Any]):
        self.event_id = event_id
        self.producer_id = producer_id
        self.schema_version = schema_version
        self.payload = payload  # 已套用默认值的规范化负载

    def __repr__(self) -> str:  # pragma: no cover
        return f"StoredEvent({self.event_id!r}, v{self.schema_version}, {self.payload!r})"


class CorrectionResolution:
    """隔离记录的处理信息：更正重放成功后写入记录，供审计。"""

    def __init__(self, correction_id: str, schema_version: int, payload: Dict[int, Any]):
        self.correction_id = correction_id
        self.schema_version = schema_version  # 更正时声明并验证的版本
        self.payload = payload  # 更正后已套用默认值的规范化负载

    def __repr__(self) -> str:  # pragma: no cover
        return f"CorrectionResolution({self.correction_id!r}, v{self.schema_version})"


class QuarantineRecord:
    """一条隔离记录。原失败内容（payload/reason）写入后不再修改，供审计。"""

    def __init__(self, event_id: str, producer_id: str, schema_version: int,
                 payload: Dict[int, Any], reason: str):
        self.event_id = event_id
        self.producer_id = producer_id
        self.schema_version = schema_version
        self.payload = payload
        self.reason = reason
        self.revision = 0  # 成功处理的次数；更正重放以此做乐观并发控制
        self.status = QuarantineStatus.PENDING
        self.resolution: Optional[CorrectionResolution] = None

    def __repr__(self) -> str:  # pragma: no cover
        return f"QuarantineRecord({self.event_id!r}, v{self.schema_version}, {self.reason!r})"


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


class CorrectionResult:
    """一次更正重放的结果。成功即正式事件已入库；失败则未留任何正式数据。"""

    def __init__(self, status: CorrectionStatus, correction_id: str, event_id: str,
                 reason: Optional[str] = None):
        self.status = status
        self.correction_id = correction_id
        self.event_id = event_id
        self.reason = reason

    @property
    def resolved(self) -> bool:
        return self.status is CorrectionStatus.RESOLVED

    def __repr__(self) -> str:  # pragma: no cover
        return (
            f"CorrectionResult({self.status.value}, {self.correction_id!r}, "
            f"{self.event_id!r}, {self.reason!r})"
        )


def build_projection(requirements: Dict[int, Any], normalized: Dict[int, Any]) -> Dict[int, Any]:
    """按消费者固定的字段 ID 抽取投影（以 ID 为键，重命名透明）。"""
    return {fid: normalized.get(fid) for fid in requirements}


class IngestionEngine:
    def __init__(self, lock: threading.RLock, registry: SchemaRegistry, consumers: ConsumerManager):
        self._lock = lock
        self._registry = registry
        self._consumers = consumers
        self._store: List[StoredEvent] = []
        self._quarantined: List[QuarantineRecord] = []
        self._quarantine_index: Dict[str, QuarantineRecord] = {}
        self._seen_ids: set[str] = set()
        # 更正请求幂等表：correction_id -> (请求内容指纹, 原结果)
        self._corrections: Dict[str, Tuple[tuple, CorrectionResult]] = {}

    # ---------- 查询 ----------

    @property
    def stored_events(self) -> Tuple[StoredEvent, ...]:
        with self._lock:
            return tuple(self._store)

    @property
    def quarantined_events(self) -> Tuple[QuarantineRecord, ...]:
        with self._lock:
            return tuple(self._quarantined)

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
                projections = self._build_all_projections(normalized)
            except Exception as exc:
                return self._quarantine(
                    event_id, producer_id, schema_version, payload,
                    f"投影构建失败: {exc}",
                )

            # 原子提交：入库与所有消费者投影在同一临界区内完成
            self._store.append(StoredEvent(event_id, producer_id, schema_version, normalized))
            self._consumers.commit_projections(event_id, schema_version, projections)
            return IngestResult(IngestStatus.ACCEPTED, event_id)

    # ---------- 隔离记录更正重放 ----------

    def correct(self, correction_id: str, event_id: str, expected_revision: int,
                schema_version: int, payload: Dict[int, Any]) -> CorrectionResult:
        """更正重放一条隔离记录：按声明版本重新校验后把事件重新接入。

        全部步骤（重新校验 -> 为同一时刻仍活跃的消费者集合构建全部投影 ->
        提交）都在本临界区内完成，与发布/回退/弃用/消费者升级互斥，
        校验与投影不会看到不同版本：任何一步失败都不留部分正式数据，
        隔离记录保持待处理；全部成功才一次发布正式事件、各消费者投影
        与记录的已处理状态。

        更正请求按 ``correction_id`` 幂等：相同请求重试返回原结果；
        同一标识携带不同内容则拒绝，不得借用旧回执。
        """
        with self._lock:
            fingerprint = (event_id, expected_revision, schema_version, dict(payload))
            prior = self._corrections.get(correction_id)
            if prior is not None:
                prior_fingerprint, prior_result = prior
                if prior_fingerprint != fingerprint:
                    raise ValueError(
                        f"更正请求 {correction_id!r} 已以不同内容提交，不能复用其回执"
                    )
                return prior_result  # 相同更正请求的重试：返回原结果

            record = self._quarantine_index.get(event_id)
            if record is None:
                raise SchemaError(f"隔离记录 {event_id!r} 不存在")
            if record.revision != expected_revision:
                raise RevisionConflictError(
                    f"隔离记录 {event_id!r} 的修订为 {record.revision}，"
                    f"与预期修订 {expected_revision} 冲突"
                )
            if record.status is QuarantineStatus.RESOLVED:
                raise SchemaError(f"隔离记录 {event_id!r} 已处理，不能重复更正")

            # 先按声明版本重新校验（与接入同一套规则：版本须仍接受写入）
            sv = self._registry.get_version(schema_version)
            if sv is None or not self._registry.is_accepting(schema_version):
                return self._reject_correction(
                    correction_id, fingerprint, event_id,
                    f"模式版本 v{schema_version} 不存在或已弃用",
                )
            errors, normalized = self.validate_event(sv, payload)
            if errors:
                return self._reject_correction(
                    correction_id, fingerprint, event_id, "；".join(errors)
                )

            # 再基于同一时刻仍活跃的消费者集合构建全部投影
            try:
                projections = self._build_all_projections(normalized)
            except Exception as exc:
                return self._reject_correction(
                    correction_id, fingerprint, event_id, f"投影构建失败: {exc}"
                )

            # 原子提交：正式事件 + 全部消费者投影 + 隔离记录已处理状态；
            # 原失败内容（record.payload/reason）保留供审计
            self._store.append(
                StoredEvent(event_id, record.producer_id, schema_version, normalized)
            )
            self._consumers.commit_projections(event_id, schema_version, projections)
            record.status = QuarantineStatus.RESOLVED
            record.revision += 1
            record.resolution = CorrectionResolution(correction_id, schema_version, normalized)
            result = CorrectionResult(CorrectionStatus.RESOLVED, correction_id, event_id)
            self._corrections[correction_id] = (fingerprint, result)
            return result

    # ---------- 内部 ----------

    def _build_all_projections(self, normalized: Dict[int, Any]) -> Dict[str, Dict[int, Any]]:
        """为同一时刻仍活跃的全部消费者构建投影（调用方必须持有锁）。"""
        return {
            view.consumer_id: build_projection(view.requirements, normalized)
            for view in self._consumers.active_views()
        }

    def _reject_correction(self, correction_id: str, fingerprint: tuple,
                           event_id: str, reason: str) -> CorrectionResult:
        """更正失败：不留任何正式数据，仅登记回执 —— 相同请求重试返回原结果。"""
        result = CorrectionResult(CorrectionStatus.REJECTED, correction_id, event_id, reason)
        self._corrections[correction_id] = (fingerprint, result)
        return result

    def _quarantine(self, event_id: str, producer_id: str, schema_version: int,
                    payload: Dict[int, Any], reason: str) -> IngestResult:
        record = QuarantineRecord(event_id, producer_id, schema_version, dict(payload), reason)
        self._quarantined.append(record)
        self._quarantine_index[event_id] = record
        return IngestResult(IngestStatus.QUARANTINED, event_id, reason)
