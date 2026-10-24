from src.utils.state import StateStore


def test_state_roundtrip_and_pending(tmp_path):
    store = StateStore(tmp_path / "s" / "state.json")
    store.register_batch("b1", [{"path": "a.csv", "sha256": "abc"}], rows=10)
    assert store.has_file("abc") and not store.has_file("zzz")
    assert store.pending_batches("silver") == ["b1"]
    assert store.pending_batches("gold") == []
    store.mark_stage_done("b1", "silver", affected_dates=["2026-01-05"])
    assert store.pending_batches("silver") == []
    assert store.pending_batches("gold") == ["b1"]

    reloaded = StateStore(tmp_path / "s" / "state.json")  # persisted
    assert reloaded.get_batch("b1")["affected_dates"] == ["2026-01-05"]
    reloaded.mark_stage_done("b1", "gold")
    assert reloaded.pending_batches("gold") == []
