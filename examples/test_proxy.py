from stpstone.utils.connections.netops.sessions.test_proxy import ProxyTester


cls_proxy_tester = ProxyTester("200.174.198.86", 8888)
print(f"Test result: {cls_proxy_tester.test_specific_proxy}")
