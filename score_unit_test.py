from score import process_data_dicts


def run_test():
    res = process_data_dicts([
        get_mocked_tx("0x1234"),
    ])

    expected = [
        get_result("0x1234", 1, 10)
    ]

    assert len(res) == len(expected)
    for i in range(len(res)):
        assert res[i] == expected[i]


def get_result(
    from_address: str,
    count: int,
    points: int,
) -> dict:
    return {
        "from": from_address,
        "hash_count": count,
        "points": points,
    }


def get_mocked_tx(
    from_address: str,
) -> dict:
    return {
        "hash": random_hex_string(),
        "from": from_address,
    }


def random_hex_string() -> str:
    import random
    ran = random.randrange(10 ** 80)
    myhex = "%064x" % ran
    # limit string to 64 characters
    myhex = myhex[:64]
    return myhex


def test_multiple_transactions_same_address():
    # Test multiple transactions from the same address
    print("Running multiple transactions same address test...")
    txs = [get_mocked_tx("0xABCD")] * 5
    res = process_data_dicts(txs)
    expected = [get_result("0xABCD", 5, 50)]
    assert len(res) == len(expected), "Failed: Multiple Transactions Same Address - Length check"
    for i in range(len(res)):
        assert res[i] == expected[i], f"Failed: Multiple Transactions Same Address - Item {i} did not match expected"


def test_multiple_transactions_different_addresses():
    # Test multiple transactions from different addresses
    txs = [get_mocked_tx("0xABCD"), get_mocked_tx("0x1234")]
    res = process_data_dicts(txs)
    expected = [
        get_result("0xABCD", 1, 10),
        get_result("0x1234", 1, 10)
    ]
    # The order might differ, so we need to sort the results before asserting
    res.sort(key=lambda x: x['from'])
    expected.sort(key=lambda x: x['from'])
    assert res == expected


if __name__ == "__main__":
    run_test()
    test_multiple_transactions_same_address()
    test_multiple_transactions_different_addresses()
