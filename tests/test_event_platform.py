"""事件模式平台测试。

核心场景 test_interleaved_publish_ingest_upgrade_rollback 交错执行：
发布新版 -> 旧版写入 -> 消费者升级 -> 失败的发布/回退 -> 成功回退，
并在每一步核对各方（注册中心、生产者、各消费者）看到的版本与数据。
"""
import threading
import unittest
from unittest import mock

from event_platform import (
    CompatibilityError,
    CorrectionError,
    CorrectionStatus,
    EventPlatform,
    Field,
    FieldType,
    IngestStatus,
    QuarantineStatus,
    SchemaError,
    ValidationError,
)
from event_platform.model import SchemaVersion
from event_platform.ingestion import build_projection as real_build_projection
from event_platform.registry import SchemaRegistry

INT, FLOAT, STRING, BOOL = (
    FieldType.INT,
    FieldType.FLOAT,
    FieldType.STRING,
    FieldType.BOOL,
)


def build_platform() -> EventPlatform:
    """v1: 字段1 user-id(INT 必需), 字段2 label(STRING 可选, 默认 n/a), 字段3 flag(BOOL 可选)。"""
    p = EventPlatform()
    p.registry.register_initial([
        Field(1, "user-id", INT, required=True),
        Field(2, "label", STRING, default="n/a"),
        Field(3, "flag", BOOL),
    ])
    return p


def projected(platform, consumer_id):
    """[(event_id, schema_version, data), ...]"""
    return [
        (e.event_id, e.schema_version, e.data)
        for e in platform.consumers.projections_of(consumer_id)
    ]


class InterleavedScenarioTest(unittest.TestCase):
    def test_interleaved_publish_ingest_upgrade_rollback(self):
        p = build_platform()
        self.assertEqual(p.registry.current_version, 1)

        # 两个消费者各自固定所需字段 ID 与类型
        p.consumers.register("analytics", {1: INT, 2: STRING})
        p.consumers.register("audit", {1: INT, 3: BOOL})

        # ---- 生产者按 v1 提交；默认值被套用，投影按字段 ID 生成 ----
        r = p.ingest("e1", "producer-a", 1, {1: 42, 3: True})
        self.assertTrue(r.accepted)
        self.assertEqual(projected(p, "analytics"), [("e1", 1, {1: 42, 2: "n/a"})])
        self.assertEqual(projected(p, "audit"), [("e1", 1, {1: 42, 3: True})])

        # ---- 无效事件：隔离，且任何消费者都不留部分投影 ----
        r = p.ingest("e2", "producer-a", 1, {1: "not-an-int", 3: True})
        self.assertEqual(r.status, IngestStatus.QUARANTINED)
        self.assertEqual([e.event_id for e in p.ingestion.stored_events], ["e1"])
        self.assertEqual([e.event_id for e in p.ingestion.quarantined_events], ["e2"])
        self.assertNotIn("e2", [e.event_id for e in p.consumers.projections_of("analytics")])
        self.assertNotIn("e2", [e.event_id for e in p.consumers.projections_of("audit")])

        # ---- 发布 v2：重命名字段 2 的显示名称 + 新增可选字段 4 ----
        draft = p.begin_proposal()
        draft.rename_field(2, "display-label")
        draft.add_field(Field(4, "score", FLOAT, default=0.0))
        self.assertEqual(p.publish(draft), 2)
        self.assertEqual(p.registry.current_version, 2)
        # 重命名不改变字段身份
        f2 = p.registry.current().fields[2]
        self.assertEqual((f2.field_id, f2.name, f2.type, f2.default),
                         (2, "display-label", STRING, "n/a"))

        # ---- 过渡期：旧生产者写 v1、新生产者写 v2，都有效 ----
        self.assertTrue(p.ingest("e3", "old-producer", 1, {1: 7}).accepted)
        self.assertTrue(p.ingest("e4", "new-producer", 2, {1: 8, 4: 2.5}).accepted)
        # 但按 v1 声明的事件不允许携带 v2 才有的字段 4 -> 按原版本验证并隔离
        self.assertFalse(p.ingest("e5", "old-producer", 1, {1: 9, 4: 1.0}).accepted)

        # 消费者无需任何动作即读到新数据（重命名透明，投影键仍是字段 ID）
        self.assertEqual(
            projected(p, "analytics"),
            [("e1", 1, {1: 42, 2: "n/a"}),
             ("e3", 1, {1: 7, 2: "n/a"}),
             ("e4", 2, {1: 8, 2: "n/a"})],
        )
        self.assertEqual(
            projected(p, "audit"),
            [("e1", 1, {1: 42, 3: True}),
             ("e3", 1, {1: 7, 3: None}),
             ("e4", 2, {1: 8, 3: None})],
        )

        # ---- 删除仍被 audit 需要的字段 3 -> 发布被拒，可见版本原子不变 ----
        with self.assertRaises(CompatibilityError):
            p.publish(p.begin_proposal().remove_field(3))
        self.assertEqual(p.registry.current_version, 2)

        # ---- 字段 1 改成 STRING（两个消费者都固定了 INT）-> 发布被拒 ----
        with self.assertRaises(CompatibilityError):
            p.publish(p.begin_proposal().change_type(1, STRING))
        self.assertEqual(p.registry.current_version, 2)

        # ---- 消费者 audit 升级：放弃字段 3；此后删除字段 3 可以发布 ----
        p.consumers.upgrade("audit", {1: INT})
        self.assertEqual(p.publish(p.begin_proposal().remove_field(3)), 3)
        self.assertEqual(p.registry.current_version, 3)

        # v3 下字段 3 已不存在：携带它的事件按 v3 验证被隔离
        self.assertTrue(p.ingest("e6", "new-producer", 3, {1: 10, 4: 1.5}).accepted)
        self.assertFalse(p.ingest("e7", "stale-producer", 3, {1: 11, 3: True}).accepted)
        # 升级后的 audit 只投影字段 1
        self.assertEqual(projected(p, "audit")[-1], ("e6", 3, {1: 10}))

        # ---- analytics 升级到需要字段 4（v1 没有该字段） ----
        p.consumers.upgrade("analytics", {1: INT, 4: FLOAT})

        # ---- 失败回退：v1 缺 analytics 需要的字段 4 -> 拒绝，可见版本不变 ----
        with self.assertRaises(CompatibilityError):
            p.rollback(1)
        self.assertEqual(p.registry.current_version, 3)
        # 回退到不存在的版本同样原子失败
        with self.assertRaises(SchemaError):
            p.rollback(99)
        self.assertEqual(p.registry.current_version, 3)

        # ---- 成功回退到 v2（满足所有活跃消费者），可见版本原子切换 ----
        self.assertEqual(p.rollback(2), 2)
        self.assertEqual(p.registry.current_version, 2)

        # 回退后过渡期依旧双向有效：v2 与 v3 生产者都能写
        self.assertTrue(p.ingest("e8", "producer-b", 2, {1: 12}).accepted)
        self.assertTrue(p.ingest("e9", "producer-b", 3, {1: 13}).accepted)

        # ---- 终态核对：各方看到的数据 ----
        self.assertEqual(
            [e.event_id for e in p.ingestion.stored_events],
            ["e1", "e3", "e4", "e6", "e8", "e9"],
        )
        self.assertEqual(
            [e.event_id for e in p.ingestion.quarantined_events],
            ["e2", "e5", "e7"],
        )
        # analytics 在升级前后的投影视图不同（投影按写入时固定的视图生成）
        self.assertEqual(
            projected(p, "analytics"),
            [("e1", 1, {1: 42, 2: "n/a"}),
             ("e3", 1, {1: 7, 2: "n/a"}),
             ("e4", 2, {1: 8, 2: "n/a"}),
             ("e6", 3, {1: 10, 2: "n/a"}),
             ("e8", 2, {1: 12, 4: 0.0}),
             ("e9", 3, {1: 13, 4: 0.0})],
        )
        self.assertEqual(
            projected(p, "audit"),
            [("e1", 1, {1: 42, 3: True}),
             ("e3", 1, {1: 7, 3: None}),
             ("e4", 2, {1: 8, 3: None}),
             ("e6", 3, {1: 10}),
             ("e8", 2, {1: 12}),
             ("e9", 3, {1: 13})],
        )


class RegistryTest(unittest.TestCase):
    def test_field_validation(self):
        with self.assertRaises(ValidationError):
            Field(1, "x", INT, required=True, default=3)   # 必需字段不应带默认值
        with self.assertRaises(ValidationError):
            Field(1, "x", INT, default="not-an-int")       # 默认值与类型不符
        with self.assertRaises(ValidationError):
            Field(-1, "x", INT)                            # 负 ID
        with self.assertRaises(ValidationError):
            Field(True, "x", INT)                          # bool 不是合法 ID
        with self.assertRaises(ValidationError):
            Field(1, "", INT)                              # 空显示名称

    def test_stale_proposal_rejected(self):
        p = build_platform()
        p.consumers.register("c", {1: INT})
        d1 = p.begin_proposal().add_field(Field(4, "a", INT))
        d2 = p.begin_proposal().add_field(Field(5, "b", INT))
        p.publish(d1)
        # d2 基于 v1 提出，但可见版本已是 v2 -> 拒绝，且不影响可见版本
        with self.assertRaises(SchemaError):
            p.publish(d2)
        self.assertEqual(p.registry.current_version, 2)

    def test_version_numbers_are_monotonic_even_after_rollback(self):
        p = build_platform()
        p.consumers.register("c", {1: INT})
        p.publish(p.begin_proposal().add_field(Field(4, "a", INT)))  # v2
        p.rollback(1)
        # 回退后再提案，版本号不复用 v2
        self.assertEqual(p.publish(p.begin_proposal().add_field(Field(5, "b", INT))), 3)
        self.assertEqual(p.registry.published_versions(), [1, 2, 3])

    def test_deprecate_ends_transition(self):
        p = build_platform()
        p.consumers.register("c", {1: INT})
        p.publish(p.begin_proposal().add_field(Field(4, "x", INT)))
        p.registry.deprecate(1)  # 结束 v1 的过渡期
        self.assertFalse(p.ingest("e1", "p", 1, {1: 1}).accepted)
        self.assertTrue(p.ingest("e2", "p", 2, {1: 1}).accepted)
        with self.assertRaises(SchemaError):
            p.registry.deprecate(2)  # 不能弃用当前可见版本

    def test_rollback_to_current_or_deprecated_rejected(self):
        p = build_platform()
        p.consumers.register("c", {1: INT})
        p.publish(p.begin_proposal().add_field(Field(4, "x", INT)))
        with self.assertRaises(SchemaError):
            p.rollback(2)  # 就是当前版本
        p.registry.deprecate(1)
        with self.assertRaises(SchemaError):
            p.rollback(1)  # 已弃用
        self.assertEqual(p.registry.current_version, 2)


class ConsumerTest(unittest.TestCase):
    def test_registration_checked_against_visible_version(self):
        p = build_platform()
        with self.assertRaises(SchemaError):
            p.consumers.register("ghost", {99: INT})        # 字段不存在
        with self.assertRaises(SchemaError):
            p.consumers.register("wrong-type", {1: STRING})  # 类型不符
        with self.assertRaises(SchemaError):
            p.consumers.register("empty", {})                # 至少固定一个字段
        p.consumers.register("ok", {1: INT})
        with self.assertRaises(SchemaError):
            p.consumers.register("ok", {1: INT})             # 重复注册

    def test_deregistered_consumer_unblocks_publish_and_stops_projections(self):
        p = build_platform()
        p.consumers.register("temp", {3: BOOL})
        # temp 活跃时，删除字段 3 被拒
        with self.assertRaises(CompatibilityError):
            p.publish(p.begin_proposal().remove_field(3))
        # 注销后不再阻塞发布
        p.consumers.deregister("temp")
        self.assertEqual(p.publish(p.begin_proposal().remove_field(3)), 2)
        # 也不再接收投影
        self.assertTrue(p.ingest("e1", "p", 2, {1: 1}).accepted)
        self.assertEqual(p.consumers.projections_of("temp"), [])

    def test_upgrade_validated_against_visible_version(self):
        p = build_platform()
        p.consumers.register("c", {1: INT})
        with self.assertRaises(SchemaError):
            p.consumers.upgrade("c", {1: INT, 4: FLOAT})  # v1 还没有字段 4
        p.publish(p.begin_proposal().add_field(Field(4, "score", FLOAT)))
        p.consumers.upgrade("c", {1: INT, 4: FLOAT})      # v2 有了，升级成功
        self.assertEqual(p.consumers.active_views()[0].requirements, {1: INT, 4: FLOAT})


class IngestionTest(unittest.TestCase):
    def test_validation_details(self):
        p = build_platform()
        p.consumers.register("c", {1: INT})
        self.assertFalse(p.ingest("e1", "p", 1, {}).accepted)                  # 缺必需字段
        self.assertFalse(p.ingest("e2", "p", 1, {1: 1, 99: "x"}).accepted)     # 未知字段
        self.assertFalse(p.ingest("e3", "p", 1, {1: True}).accepted)           # bool 不是 INT
        self.assertFalse(p.ingest("e4", "p", 7, {1: 1}).accepted)              # 版本不存在
        self.assertTrue(p.ingest("e5", "p", 1, {1: 1}).accepted)
        with self.assertRaises(ValueError):
            p.ingest("e5", "p", 1, {1: 2})                                     # 重复事件 ID
        # 前四个无效事件都在隔离区，且带有原因
        self.assertEqual(
            [q.event_id for q in p.ingestion.quarantined_events],
            ["e1", "e2", "e3", "e4"],
        )
        self.assertTrue(all(q.reason for q in p.ingestion.quarantined_events))

    def test_projection_failure_is_atomic(self):
        """投影构建中途失败：事件不入库，任何消费者都不留部分投影。"""
        p = build_platform()
        p.consumers.register("c1", {1: INT})
        p.consumers.register("c2", {1: INT})

        def boom(requirements, normalized):
            raise RuntimeError("模拟投影构建失败")

        with mock.patch("event_platform.ingestion.build_projection", boom):
            r = p.ingest("e1", "p", 1, {1: 5})
        self.assertEqual(r.status, IngestStatus.QUARANTINED)
        self.assertIn("投影构建失败", r.reason)
        self.assertEqual(p.ingestion.stored_events, ())
        self.assertEqual(p.consumers.projections_of("c1"), [])
        self.assertEqual(p.consumers.projections_of("c2"), [])

    def test_defaults_applied_and_normalized_payload_stored(self):
        p = build_platform()
        p.consumers.register("c", {1: INT, 2: STRING})
        p.ingest("e1", "p", 1, {1: 1})
        stored = p.ingestion.stored_events[0]
        self.assertEqual(stored.payload, {1: 1, 2: "n/a"})  # 默认值已套用
        self.assertEqual(stored.schema_version, 1)


class CorrectionReplayTest(unittest.TestCase):
    def test_interleaved_correction_across_version_switch(self):
        """交错：隔离 -> 发布新版/弃用旧版/回退 -> 更正重放，验证按声明版本校验、
        默认值与消费者集合取自同一时刻。"""
        p = build_platform()
        p.consumers.register("analytics", {1: INT, 2: STRING})
        p.consumers.register("audit", {1: INT, 3: BOOL})

        # e-bad 在 v1 下因字段 1 类型错误被隔离
        r = p.ingest("e-bad", "producer-a", 1, {1: "oops", 3: True})
        self.assertEqual(r.status, IngestStatus.QUARANTINED)
        original_reason = p.ingestion.get_quarantine("e-bad").reason

        # ---- 用错误内容更正（仍是坏类型）：拒绝，不留任何正式数据 ----
        bad = p.replay_correction("e-bad", 0, 1, {1: 1.5, 3: True})
        self.assertEqual(bad.status, CorrectionStatus.REJECTED)
        self.assertIsNone(bad.receipt)
        self.assertTrue(bad.reason)
        self.assertEqual([e.event_id for e in p.ingestion.stored_events], [])
        self.assertEqual(p.consumers.projections_of("analytics"), [])
        rec = p.ingestion.get_quarantine("e-bad")
        self.assertEqual(rec.status, QuarantineStatus.PENDING)
        self.assertEqual(rec.revision, 0)

        # 相同的错误请求重试：返回同一个拒绝裁决，仍无副作用
        bad_retry = p.replay_correction("e-bad", 0, 1, {1: 1.5, 3: True})
        self.assertEqual(bad_retry.status, CorrectionStatus.REJECTED)
        self.assertEqual(bad_retry.reason, bad.reason)
        self.assertEqual([e.event_id for e in p.ingestion.stored_events], [])

        # ---- 发布 v2（新增可选字段 4），随后弃用 v1 ----
        self.assertEqual(
            p.publish(p.begin_proposal().add_field(Field(4, "score", FLOAT, default=0.0))),
            2,
        )
        p.registry.deprecate(1)
        # 声明已弃用的 v1 重放 -> 拒绝
        stale = p.replay_correction("e-bad", 0, 1, {1: 5, 3: True})
        self.assertEqual(stale.status, CorrectionStatus.REJECTED)
        self.assertIn("弃用", stale.reason)
        self.assertEqual(p.ingestion.get_quarantine("e-bad").revision, 0)

        # ---- 按当前可见的 v2 更正：默认值套用字段 2/4，快照两个活跃消费者 ----
        ok = p.replay_correction("e-bad", 0, 2, {1: 5, 3: True})
        self.assertEqual(ok.status, CorrectionStatus.ACCEPTED)
        receipt = ok.receipt
        self.assertEqual(receipt.event_id, "e-bad")
        self.assertEqual(receipt.revision, 1)
        self.assertEqual(receipt.schema_version, 2)
        self.assertEqual(receipt.payload, {1: 5, 2: "n/a", 3: True, 4: 0.0})

        # 正式事件与全部消费者投影一次出现（没有消费者先看到）
        self.assertEqual(
            [(e.event_id, e.schema_version, e.corrected) for e in p.ingestion.stored_events],
            [("e-bad", 2, True)],
        )
        self.assertEqual(projected(p, "analytics"),
                         [("e-bad", 2, {1: 5, 2: "n/a"})])
        self.assertEqual(projected(p, "audit"),
                         [("e-bad", 2, {1: 5, 3: True})])

        # 隔离记录：已处理 + 修订 +1，但原失败内容保留供审计
        rec = p.ingestion.get_quarantine("e-bad")
        self.assertEqual(rec.status, QuarantineStatus.CORRECTED)
        self.assertTrue(rec.processed)
        self.assertEqual(rec.revision, 1)
        self.assertEqual(rec.payload, {1: "oops", 3: True})
        self.assertEqual(rec.reason, original_reason)
        self.assertEqual(rec.corrected_payload, {1: 5, 2: "n/a", 3: True, 4: 0.0})
        self.assertIs(rec.receipt, receipt)
        self.assertEqual(len(p.ingestion.quarantined_events), 1)

        # 相同请求重试：返回原结果与原回执，不产生第二条正式事件/投影
        again = p.replay_correction("e-bad", 0, 2, {1: 5, 3: True})
        self.assertEqual(again.status, CorrectionStatus.ACCEPTED)
        self.assertIs(again.receipt, receipt)
        self.assertEqual(len(p.ingestion.stored_events), 1)
        self.assertEqual(len(p.consumers.projections_of("analytics")), 1)

        # 更正后带新内容再来：既不能借用旧回执，也不能二次处理
        tampered = p.replay_correction("e-bad", 0, 2, {1: 6, 3: False})
        self.assertEqual(tampered.status, CorrectionStatus.REJECTED)
        self.assertIsNone(tampered.receipt)
        with_correct_rev = p.replay_correction("e-bad", 1, 2, {1: 6, 3: False})
        self.assertEqual(with_correct_rev.status, CorrectionStatus.REJECTED)
        self.assertIsNone(with_correct_rev.receipt)
        self.assertEqual(len(p.ingestion.stored_events), 1)

        # 未更正隔离记录与旧生产者接入的行为不变：v1 已弃用，旧生产者的
        # 新事件照常隔离；v2 新事件照常接入
        stale_ingest = p.ingest("e-old", "producer-c", 1, {1: 3})
        self.assertFalse(stale_ingest.accepted)
        self.assertEqual(stale_ingest.status, IngestStatus.QUARANTINED)
        self.assertTrue(p.ingest("e-new", "producer-c", 2, {1: 4}).accepted)

    def test_revision_conflict_and_no_receipt_borrowing(self):
        p = build_platform()
        p.consumers.register("c", {1: INT})
        p.ingest("q1", "producer-a", 1, {1: "bad"})

        # 预期修订号不是 0：冲突拒绝
        r = p.replay_correction("q1", 7, 1, {1: 1})
        self.assertEqual(r.status, CorrectionStatus.REJECTED)
        self.assertIn("修订冲突", r.reason)
        # 同四元组重试返回同一冲突结果
        self.assertIs(
            p.replay_correction("q1", 7, 1, {1: 1}).reason, r.reason
        )
        # 正确修订号但与缓存的失败请求内容不同：独立执行，成功且拿到新回执
        ok = p.replay_correction("q1", 0, 1, {1: 1})
        self.assertTrue(ok.accepted)
        self.assertEqual(ok.receipt.revision, 1)
        # 成功后再用冲突修订号发新内容：已更正记录拒绝，不借用任何旧回执
        second = p.replay_correction("q1", 0, 1, {1: 2})
        self.assertFalse(second.accepted)
        self.assertIsNone(second.receipt)
        self.assertEqual(len(p.ingestion.stored_events), 1)
        # 正式事件只有一条，值是首次成功更正的值
        self.assertEqual(p.ingestion.stored_events[0].payload, {1: 1, 2: "n/a"})

    def test_unknown_record_and_bad_arguments(self):
        p = build_platform()
        with self.assertRaises(CorrectionError):
            p.replay_correction("nope", 0, 1, {1: 1})
        p.ingest("q1", "p", 1, {1: "bad"})
        with self.assertRaises(ValidationError):
            p.replay_correction("q1", -1, 1, {1: 1})
        with self.assertRaises(ValidationError):
            p.replay_correction("q1", 0, 1, "not-a-dict")

    def test_partial_projection_failure_leaves_nothing(self):
        """部分消费者投影构建失败：正式事件、任何消费者投影、记录修订全部不变。"""
        p = build_platform()
        p.consumers.register("c1", {1: INT})
        p.consumers.register("c2", {1: INT, 2: STRING})
        p.ingest("q1", "p", 1, {1: "bad"})

        call_count = {"n": 0}

        def flaky(requirements, normalized):
            call_count["n"] += 1
            if call_count["n"] == 2:  # 第二个消费者（c2）构建时失败
                raise RuntimeError("模拟部分投影失败")
            return real_build_projection(requirements, normalized)

        with mock.patch("event_platform.ingestion.build_projection", flaky):
            r = p.replay_correction("q1", 0, 1, {1: 5})
        self.assertEqual(r.status, CorrectionStatus.REJECTED)
        self.assertIn("投影构建失败", r.reason)
        # 无部分正式数据
        self.assertEqual(p.ingestion.stored_events, ())
        self.assertEqual(p.consumers.projections_of("c1"), [])
        self.assertEqual(p.consumers.projections_of("c2"), [])
        rec = p.ingestion.get_quarantine("q1")
        self.assertEqual(rec.status, QuarantineStatus.PENDING)
        self.assertEqual(rec.revision, 0)

        # 相同失败请求重试：返回缓存的拒绝结果（不会因 flaky 状态变化而不同）
        with mock.patch("event_platform.ingestion.build_projection", flaky):
            retry = p.replay_correction("q1", 0, 1, {1: 5})
        self.assertEqual(retry.status, CorrectionStatus.REJECTED)
        self.assertEqual(retry.reason, r.reason)
        self.assertEqual(p.ingestion.stored_events, ())

        # 不同内容的更正请求不得借用旧拒绝回执，正常执行并成功
        ok = p.replay_correction("q1", 0, 1, {1: 9})
        self.assertTrue(ok.accepted)
        self.assertEqual(projected(p, "c1"), [("q1", 1, {1: 9})])
        self.assertEqual(projected(p, "c2"), [("q1", 1, {1: 9, 2: "n/a"})])
        self.assertEqual(p.ingestion.get_quarantine("q1").revision, 1)

    def test_consumer_mapping_unsatisfied_rejected_atomically(self):
        """声明版本满足不了当前活跃消费者的固定映射（类型不兼容/字段缺失）
        时：在投影前拒绝，不留正式数据。"""
        p = build_platform()
        p.consumers.register("c", {1: INT})
        p.ingest("q1", "p", 1, {1: "bad"})

        # v2 把字段 1 改成 STRING：与 c 固定的 INT 不兼容，发布被拒
        with self.assertRaises(CompatibilityError):
            p.publish(p.begin_proposal().change_type(1, STRING))

        # 构造一个能发布但更正时映射不满足的场景：
        # 新消费者固定字段 4（FLOAT），v1 缺该字段 -> 按 v1 重放无法满足它
        p.publish(p.begin_proposal().add_field(Field(4, "score", FLOAT)))  # v2
        p.consumers.register("late", {1: INT, 4: FLOAT})
        r = p.replay_correction("q1", 0, 1, {1: 5})
        self.assertEqual(r.status, CorrectionStatus.REJECTED)
        self.assertIn("消费者映射无法满足", r.reason)
        self.assertIn("late", r.reason)
        self.assertEqual(p.ingestion.stored_events, ())
        self.assertEqual(p.consumers.projections_of("c"), [])
        self.assertEqual(p.consumers.projections_of("late"), [])
        self.assertEqual(p.ingestion.get_quarantine("q1").revision, 0)

        # late 注销后不再参与映射；相同请求仍返回原裁决，不同内容的新请求
        # （另一套声明值）独立执行，按 v1 重放成功
        p.consumers.deregister("late")
        self.assertFalse(p.replay_correction("q1", 0, 1, {1: 5}).accepted)
        ok = p.replay_correction("q1", 0, 1, {1: 6})
        self.assertTrue(ok.accepted)
        self.assertEqual(p.consumers.projections_of("late"), [])
        self.assertEqual(projected(p, "c"), [("q1", 1, {1: 6})])

    def test_different_payloads_do_not_share_receipt(self):
        p = build_platform()
        p.consumers.register("c", {1: INT})
        p.ingest("q1", "p", 1, {1: "bad"})

        a = p.replay_correction("q1", 0, 1, {1: 11})
        self.assertTrue(a.accepted)
        # 另一条隔离记录，即便声明相同版本/修订、相似负载，也拿不到 q1 的回执
        p.ingest("q2", "p", 1, {1: "also-bad"})
        b = p.replay_correction("q2", 0, 1, {1: 11})
        self.assertTrue(b.accepted)
        self.assertIsNotNone(b.receipt)
        self.assertIsNotNone(a.receipt)
        self.assertNotEqual(b.receipt.receipt_id, a.receipt.receipt_id)
        self.assertEqual(b.receipt.quarantine_id, "q2")
        # 不同字段值（即便都是合法更正）在 q1 已处理后也只能被拒绝
        self.assertFalse(p.replay_correction("q1", 0, 1, {1: 12}).accepted)


class ConcurrencyTest(unittest.TestCase):
    def test_concurrent_ingest_publish_rollback_invariants(self):
        """多线程交错写入与发布/回退：每条入库事件都必须出现在所有活跃消费者的投影里。"""
        p = build_platform()
        p.consumers.register("c1", {1: INT})
        p.consumers.register("c2", {1: INT, 2: STRING})
        errors = []
        n_writers, per_writer, n_cycles = 3, 100, 30

        def writer(pid):
            for i in range(per_writer):
                try:
                    p.ingest(f"w{pid}-{i}", f"producer-{pid}", 1, {1: i})
                except Exception as exc:  # noqa: BLE001
                    errors.append(exc)

        def admin():
            for i in range(n_cycles):
                try:
                    draft = p.begin_proposal().add_field(Field(10 + i, f"extra-{i}", INT))
                    p.publish(draft)
                    p.rollback(1)
                except Exception as exc:  # noqa: BLE001
                    errors.append(exc)

        threads = [threading.Thread(target=writer, args=(k,)) for k in range(n_writers)]
        threads.append(threading.Thread(target=admin))
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(errors, [])
        stored = p.ingestion.stored_events
        self.assertEqual(len(stored), n_writers * per_writer)
        stored_ids = [e.event_id for e in stored]
        for cid in ("c1", "c2"):
            self.assertEqual(
                [e.event_id for e in p.consumers.projections_of(cid)], stored_ids
            )
        # 每条入库事件声明的版本都真实存在过
        published = set(p.registry.published_versions())
        self.assertTrue(all(e.schema_version in published for e in stored))

    def test_concurrent_correction_replay_with_publish_rollback(self):
        """更正重放与发布/回退交错：成功的更正只发布一次，且正式事件出现在
        重放时刻所有活跃消费者的投影里；相同请求的并发重试共用原回执。"""
        p = build_platform()
        # analytics 在任何版本都兼容；auditor 需要字段 4，只能映射 v2+
        p.consumers.register("analytics", {1: INT, 2: STRING})
        n_quarantine, n_workers = 30, 2
        for i in range(n_quarantine):
            self.assertFalse(p.ingest(f"bad-{i}", f"producer-{i}", 1, {1: "x"}).accepted)
        # 发布 v2（新增可选字段 4）后 auditor 才能注册
        self.assertEqual(
            p.publish(p.begin_proposal().add_field(Field(4, "score", FLOAT, default=0.0))),
            2,
        )
        p.consumers.register("auditor", {1: INT, 4: FLOAT})

        results: list = []
        results_lock = threading.Lock()

        def corrector(wid):
            for i in range(wid, n_quarantine, n_workers):
                qid = f"bad-{i}"
                try:
                    # 每个记录只由一个 worker 首次提交，但相同请求会被重试
                    r1 = p.replay_correction(qid, 0, 2, {1: i})
                    r2 = p.replay_correction(qid, 0, 2, {1: i})  # 同请求重试
                    with results_lock:
                        results.append((qid, r1, r2))
                except Exception as exc:  # noqa: BLE001
                    with results_lock:
                        results.append((qid, exc, None))

        def admin():
            # 反复发布 v2 / 回退 v1：回退 v1 会因 auditor 需要字段 4 被拒，
            # 可见版本停在 v2（与并发写入互不产生中间状态）
            for _ in range(20):
                try:
                    p.rollback(1)
                except Exception:  # noqa: BLE001
                    pass

        def ordinary_writer():
            for j in range(20):
                p.ingest(f"ord-{j}", "ordinary", 2, {1: j, 4: 1.0})

        threads = [threading.Thread(target=corrector, args=(k,)) for k in range(n_workers)]
        threads.append(threading.Thread(target=admin))
        threads.append(threading.Thread(target=ordinary_writer))
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(len(results), n_quarantine)
        for qid, r1, r2 in results:
            self.assertTrue(r1.accepted, qid)
            self.assertIs(r2.receipt, r1.receipt)  # 相同请求共用原回执
            rec = p.ingestion.get_quarantine(qid)
            self.assertEqual(rec.status, QuarantineStatus.CORRECTED)
            self.assertEqual(rec.revision, 1)
            self.assertEqual(rec.corrected_schema_version, 2)
            self.assertTrue(rec.reason)  # 原失败原因保留

        corrected = [e for e in p.ingestion.stored_events if e.corrected]
        self.assertEqual(sorted(e.event_id for e in corrected),
                         sorted(f"bad-{i}" for i in range(n_quarantine)))
        self.assertEqual(len(corrected), n_quarantine)  # 不重复发布

        total = len(p.ingestion.stored_events)
        for cid in ("analytics", "auditor"):
            self.assertEqual(
                len(p.consumers.projections_of(cid)), total
            )  # 每条正式事件都在全部活跃消费者的投影里
        stored_ids = [e.event_id for e in p.ingestion.stored_events]
        self.assertEqual(
            [e.event_id for e in p.consumers.projections_of("auditor")], stored_ids
        )


if __name__ == "__main__":
    unittest.main()
