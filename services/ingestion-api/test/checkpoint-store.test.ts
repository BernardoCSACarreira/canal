import test from "node:test";
import assert from "node:assert/strict";
import { P1LocalCheckpointStore } from "../src/canal/checkpoint-store.js";

test("P1LocalCheckpointStore: load is null until first save", () => {
  const st = new P1LocalCheckpointStore();
  assert.equal(st.load("g1"), null);
});

test("P1LocalCheckpointStore: save then load returns cursor", () => {
  const st = new P1LocalCheckpointStore();
  st.save("g1", 42, 1000);
  const c = st.load("g1");
  assert.ok(c);
  assert.equal(c!.committedSequence, 42);
  assert.equal(c!.updatedAtMs, 1000);
});

test("P1LocalCheckpointStore: monotonic max on save (regression ignored)", () => {
  const st = new P1LocalCheckpointStore();
  st.save("g1", 10);
  st.save("g1", 5);
  assert.equal(st.load("g1")!.committedSequence, 10);
  st.save("g1", 12);
  assert.equal(st.load("g1")!.committedSequence, 12);
});

test("P1LocalCheckpointStore: load returns a copy", () => {
  const st = new P1LocalCheckpointStore();
  st.save("g1", 1);
  const a = st.load("g1");
  const b = st.load("g1");
  assert.notEqual(a, b);
  if (a) a.committedSequence = 999;
  assert.equal(st.load("g1")!.committedSequence, 1);
});

test("P1LocalCheckpointStore: fence is noop", () => {
  const st = new P1LocalCheckpointStore();
  st.fence("g1", "epoch-1");
  assert.equal(st.load("g1"), null);
});

test("P1LocalCheckpointStore: consumers are isolated", () => {
  const st = new P1LocalCheckpointStore();
  st.save("a", 3);
  st.save("b", 7);
  assert.equal(st.load("a")!.committedSequence, 3);
  assert.equal(st.load("b")!.committedSequence, 7);
});
