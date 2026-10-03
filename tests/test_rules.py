def is_fraud(txn_count, total_amt):
    return txn_count > 4 or total_amt > 400

def test_flags_velocity():
    assert is_fraud(6, 100) is True

def test_flags_amount():
    assert is_fraud(2, 500) is True

def test_ignores_normal():
    assert is_fraud(2, 80) is False

def test_boundary():
    assert is_fraud(4, 400) is False
