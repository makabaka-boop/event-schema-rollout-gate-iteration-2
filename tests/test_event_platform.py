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
    CorrectionStatus,
    EventPlatform,
    Field,
    FieldType,
    IngestStatus,
    QuarantineStatus,
    RevisionConflictError,
    SchemaError,
    ValidationError,
)
from event_platform.model import SchemaVersion
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


class CorrectionTest(unittest.TestCase):
    """隔离事件的更正重放：交错验证版本切换、默认值、重复请求与部分投影失败。"""

    def test_correction_republishes_event_atomically_and_keeps_audit(self):
        p = build_platform()
        p.consumers.register("analytics", {1: INT, 2: STRING})
        p.consumers.register("audit", {1: INT, 3: BOOL})
        r = p.ingest("bad-1", "producer-a", 1, {1: "oops", 3: True})
        self.assertEqual(r.status, IngestStatus.QUARANTINED)

        res = p.correct("fix-1", "bad-1", 0, 1, {1: 42, 3: True})
        self.assertTrue(res.resolved)
        self.assertEqual((res.correction_id, res.event_id), ("fix-1", "bad-1"))

        # 正式事件与全部消费者投影同时可见 —— 不存在"有人看到投影、有人仍看到失败"
        self.assertEqual([e.event_id for e in p.ingestion.stored_events], ["bad-1"])
        self.assertEqual(projected(p, "analytics"), [("bad-1", 1, {1: 42, 2: "n/a"})])
        self.assertEqual(projected(p, "audit"), [("bad-1", 1, {1: 42, 3: True})])

        # 隔离记录标记已处理、修订递增，原失败内容保留供审计
        (q,) = p.ingestion.quarantined_events
        self.assertEqual(q.status, QuarantineStatus.RESOLVED)
        self.assertEqual(q.revision, 1)
        self.assertEqual(q.payload, {1: "oops", 3: True})  # 原内容未动
        self.assertIn("期望", q.reason)
        self.assertEqual(q.resolution.correction_id, "fix-1")
        self.assertEqual(q.resolution.schema_version, 1)
        self.assertEqual(q.resolution.payload, {1: 42, 2: "n/a", 3: True})

    def test_interleaved_correction_with_version_switching_and_defaults(self):
        p = build_platform()
        p.consumers.register("analytics", {1: INT, 2: STRING})
        p.consumers.register("audit", {1: INT, 3: BOOL})

        # v1 时期两条隔离：类型错误 / 可选字段类型错误
        self.assertFalse(p.ingest("q1", "prod", 1, {1: "bad"}).accepted)
        self.assertFalse(p.ingest("q2", "prod", 1, {1: 1, 2: 3}).accepted)

        # 发布 v2：新增可选字段 4（带默认值）
        p.publish(p.begin_proposal().add_field(Field(4, "score", FLOAT, default=0.0)))

        # 过渡期内按 v1 更正 q1：成功，套用 v1 的默认值
        r1 = p.correct("fix-q1", "q1", 0, 1, {1: 10})
        self.assertTrue(r1.resolved)
        self.assertEqual(p.ingestion.stored_events[-1].schema_version, 1)
        self.assertEqual(p.ingestion.stored_events[-1].payload, {1: 10, 2: "n/a"})
        # 相同更正请求重试 -> 返回原结果，不重复入库
        self.assertIs(p.correct("fix-q1", "q1", 0, 1, {1: 10}), r1)
        self.assertEqual(len(p.ingestion.stored_events), 1)

        # 弃用 v1 后：q2 按 v1 更正被拒（记录保持待处理，可换版本再更正）
        p.registry.deprecate(1)
        r2 = p.correct("fix-q2-v1", "q2", 0, 1, {1: 2, 2: "ok"})
        self.assertEqual(r2.status, CorrectionStatus.REJECTED)
        self.assertIn("弃用", r2.reason)
        self.assertEqual(len(p.ingestion.stored_events), 1)
        # 按 v2 更正 q2：字段 4 的默认值被套用，事件以 v2 入库
        r3 = p.correct("fix-q2-v2", "q2", 0, 2, {1: 2, 2: "ok"})
        self.assertTrue(r3.resolved)
        self.assertEqual(p.ingestion.stored_events[-1].schema_version, 2)
        self.assertEqual(p.ingestion.stored_events[-1].payload, {1: 2, 2: "ok", 4: 0.0})

        # 各消费者投影一致地看到两条重放事件
        self.assertEqual(
            projected(p, "analytics"),
            [("q1", 1, {1: 10, 2: "n/a"}), ("q2", 2, {1: 2, 2: "ok"})],
        )
        self.assertEqual(
            projected(p, "audit"),
            [("q1", 1, {1: 10, 3: None}), ("q2", 2, {1: 2, 3: None})],
        )
        # 两条隔离记录都已处理，原失败内容仍保留
        self.assertEqual(
            [q.status for q in p.ingestion.quarantined_events],
            [QuarantineStatus.RESOLVED, QuarantineStatus.RESOLVED],
        )
        self.assertEqual(p.ingestion.quarantined_events[1].payload, {1: 1, 2: 3})

    def test_duplicate_correction_requests(self):
        p = build_platform()
        p.consumers.register("c", {1: INT})
        self.assertFalse(p.ingest("q1", "p", 1, {1: "bad"}).accepted)

        res = p.correct("fix-1", "q1", 0, 1, {1: 5})
        self.assertTrue(res.resolved)
        # 相同请求重试 -> 原结果（同一对象），不重复入库
        self.assertIs(p.correct("fix-1", "q1", 0, 1, {1: 5}), res)
        self.assertEqual(len(p.ingestion.stored_events), 1)
        # 同一标识携带不同内容 -> 拒绝，不得借用旧成功回执
        with self.assertRaises(ValueError):
            p.correct("fix-1", "q1", 0, 1, {1: 6})
        with self.assertRaises(ValueError):
            p.correct("fix-1", "q1", 0, 2, {1: 5})
        # 换标识但修订已前进 -> 修订冲突
        with self.assertRaises(RevisionConflictError):
            p.correct("fix-2", "q1", 0, 1, {1: 6})
        # 按当前修订提交 -> 记录已处理，不能重复更正
        with self.assertRaises(SchemaError):
            p.correct("fix-3", "q1", 1, 1, {1: 6})
        # 不存在的隔离记录
        with self.assertRaises(SchemaError):
            p.correct("fix-4", "no-such-event", 0, 1, {1: 6})
        # 上述失败都不留正式数据
        self.assertEqual(len(p.ingestion.stored_events), 1)

    def test_rejected_correction_is_also_idempotent(self):
        p = build_platform()
        p.consumers.register("c", {1: INT})
        self.assertFalse(p.ingest("q1", "p", 1, {1: "bad"}).accepted)
        # 更正后的值仍不合法 -> 拒绝；相同请求重试返回原失败结果
        res = p.correct("fix-1", "q1", 0, 1, {1: "still-bad"})
        self.assertEqual(res.status, CorrectionStatus.REJECTED)
        self.assertIs(p.correct("fix-1", "q1", 0, 1, {1: "still-bad"}), res)
        # 记录保持待处理、修订不变，可换标识用合法值再更正
        (q,) = p.ingestion.quarantined_events
        self.assertEqual((q.status, q.revision), (QuarantineStatus.PENDING, 0))
        self.assertTrue(p.correct("fix-2", "q1", 0, 1, {1: 7}).resolved)

    def test_correction_projection_failure_leaves_no_partial_state(self):
        """更正时投影构建中途失败：不入库、任何消费者不留部分投影、记录保持待处理。"""
        p = build_platform()
        p.consumers.register("c1", {1: INT})
        p.consumers.register("c2", {1: INT})
        self.assertFalse(p.ingest("q1", "p", 1, {1: "bad"}).accepted)

        def boom(requirements, normalized):
            raise RuntimeError("模拟投影构建失败")

        with mock.patch("event_platform.ingestion.build_projection", boom):
            res = p.correct("fix-1", "q1", 0, 1, {1: 5})
        self.assertEqual(res.status, CorrectionStatus.REJECTED)
        self.assertIn("投影构建失败", res.reason)
        self.assertEqual(p.ingestion.stored_events, ())
        self.assertEqual(p.consumers.projections_of("c1"), [])
        self.assertEqual(p.consumers.projections_of("c2"), [])
        (q,) = p.ingestion.quarantined_events
        self.assertEqual((q.status, q.revision), (QuarantineStatus.PENDING, 0))
        self.assertIsNone(q.resolution)
        # 故障消除后换新标识重试成功，两个消费者同时看到投影
        self.assertTrue(p.correct("fix-2", "q1", 0, 1, {1: 5}).resolved)
        self.assertEqual(len(p.consumers.projections_of("c1")), 1)
        self.assertEqual(
            p.consumers.projections_of("c1"), p.consumers.projections_of("c2")
        )

    def test_correction_uses_active_consumers_at_commit_moment(self):
        p = build_platform()
        p.consumers.register("gone", {1: INT})
        p.consumers.register("stay", {1: INT})
        self.assertFalse(p.ingest("q1", "p", 1, {1: "bad"}).accepted)
        # 注销发生在更正之前：gone 不属于"同一时刻仍活跃"的集合，不收投影
        p.consumers.deregister("gone")
        self.assertTrue(p.correct("fix-1", "q1", 0, 1, {1: 5}).resolved)
        self.assertEqual(p.consumers.projections_of("gone"), [])
        self.assertEqual(projected(p, "stay"), [("q1", 1, {1: 5})])


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


if __name__ == "__main__":
    unittest.main()
