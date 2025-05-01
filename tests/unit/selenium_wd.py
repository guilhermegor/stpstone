from unittest import TestCase, main
from unittest.mock import patch, MagicMock
from stpstone.utils.webdriver_tools.selenium_wd import SeleniumWD


class TestSeleniumWDDuckDuckGo(TestCase):

    def setUp(self):
        self.url = "https://duckduckgo.com/?q=python+programming"
        self.headless = True
        self.timeout = 30

    @patch("stpstone.utils.webdriver_tools.selenium_wd.SeleniumWD.get_network_traffic", new_callable=MagicMock)
    def test_duckduckgo_query_simulation(self, mock_network_traffic):
        mock_network_traffic.return_value = [
            {"method": "Network.requestWillBeSent", "params": {"url": "https://api.duckduckgo.com/"}},
            {"method": "Network.responseReceived", "params": {"status": 200}},
        ]
        selenium_wd = SeleniumWD(
            url=self.url,
            bl_headless=self.headless,
            bl_incognito=True
        )
        try:
            selenium_wd.wait(self.timeout)
            self.assertIn("DuckDuckGo", selenium_wd.web_driver.title)
            self.assertIn("python+programming", selenium_wd.web_driver.current_url)
            network_traffic = selenium_wd.get_network_traffic
            self.assertEqual(len(network_traffic), 0)
        finally:
            selenium_wd.web_driver.quit()


if __name__ == "__main__":
    main()
