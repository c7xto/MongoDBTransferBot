import ui


def test_detailed_progress_uses_delivery_rate_for_eta():
    card = ui.build_progress_card(
        210_000,
        635_624,
        600,
        2,
        ui.TransferState.RUNNING,
        sent=1_000,
        skipped=196_430,
        send_total=10_000,
        delivery_rate=2.0,
        eta_seconds=4_499,
    )

    assert "1,002` / `10,000`" in card
    assert "10.0%" in card
    assert "Sent now › `1,000`" in card
    assert "Skipped this run › `196,430`" in card
    assert "Source checked › `210,000` / `635,624`" in card
    assert "Live speed › `120.0` files/min" in card
    assert "ETA › `1 hr, 14 min`" in card


def test_detailed_progress_waits_for_real_send_rate():
    card = ui.build_progress_card(
        196_430,
        635_624,
        30,
        state=ui.TransferState.RUNNING,
        sent=0,
        skipped=196_430,
        send_total=439_194,
        delivery_rate=0.0,
        eta_seconds=0.0,
    )

    assert "Live speed › `Calculating…`" in card
    assert "ETA › `Calculating…`" in card


def test_snapshot_renderer_keeps_detailed_fields():
    card = ui.build_progress_snapshot_card({
        "count": 20,
        "total": 100,
        "elapsed": 60,
        "failed": 0,
        "sent": 10,
        "skipped": 10,
        "send_total": 80,
        "delivery_rate": 1.0,
        "eta_seconds": 70,
    })

    assert "Transfer › `10` / `80`" in card
    assert "ETA › `1 min`" in card
