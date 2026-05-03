import test from "node:test";
import assert from "node:assert/strict";
import { P1LocalEventBuffer } from "../src/canal/event-buffer.js";

function ev(id: string) {
  return { id, type: "t", occurredAt: "2026-05-03T12:00:00.000Z" };
}

test("P1LocalEventBuffer: append assigns monotonic sequences", () => {
  const buf = new P1LocalEventBuffer();
  const s1 = buf.append("src-a", [ev("1")]);
  const s2 = buf.append("src-b", [ev("2"), ev("3")]);
  assert.deepEqual(s1, [1]);
  assert.deepEqual(s2, [2, 3]);
  assert.equal(buf.getMetrics(1_000_000).depth, 3);
});

test("P1LocalEventBuffer: readAfter cursor and trimAfterCommit", () => {
  const buf = new P1LocalEventBuffer();
  buf.append("s", [ev("a"), ev("b")]);
  assert.equal(buf.readAfter(0, 10).length, 2);
  assert.equal(buf.readAfter(0, 1)[0]!.event.id, "a");
  assert.equal(buf.readAfter(1, 10).length, 1);
  assert.equal(buf.readAfter(1, 10)[0]!.event.id, "b");
  buf.trimAfterCommit(1);
  assert.equal(buf.getMetrics().depth, 1);
  assert.equal(buf.readAfter(0, 10)[0]!.sequence, 2);
  buf.trimAfterCommit(2);
  assert.equal(buf.getMetrics().depth, 0);
});

test("P1LocalEventBuffer: oldestAgeMs uses oldest enqueuedAt", () => {
  const buf = new P1LocalEventBuffer();
  buf.append("s", [ev("x")]);
  const m = buf.getMetrics(10_000);
  assert.equal(m.depth, 1);
  assert.ok(m.oldestAgeMs !== null && m.oldestAgeMs >= 0);
});
