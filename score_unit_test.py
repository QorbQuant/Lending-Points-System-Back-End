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


if __name__ == "__main__":
    run_test()
