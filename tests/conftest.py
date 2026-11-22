
def pytest_configure(config):
    config.addinivalue_line(
        "markers", "integration: uses real UDP sockets on loopback"
    )
