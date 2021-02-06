from smartbridge_comms import proxy


def test_communication():
    x = proxy.DatabaseManager('./tests/config/credentials.yaml')
    assert x.query_measurements('1000d', False) == 0


def test_csv_import():
    x = proxy.DatabaseManager('./tests/config/credentials.yaml')
    imported = x.add_csv('./tests/test.csv', 'spikes', False)
    assert imported == 9


def test_read_measurements():
    x = proxy.DatabaseManager('./tests/config/credentials.yaml')
    records = x.query_measurements('1000d', False)
    assert records == 9


def test_delete_measurements():
    x = proxy.DatabaseManager('./tests/config/credentials.yaml')
    records = x.delete_measurements('spikes')
    assert records == 9
