from unittest import TestCase, main
from stpstone.utils.webdriver_tools.selenium_wd import SeleniumWD


class TestSeleniumWDSPGlobalRatings(TestCase):

    def setUp(self):
        self.url = "https://disclosure.spglobal.com/ratings/en/regulatory/ratings-actions"
        self.headless = True
        self.timeout = 60

    def test_connection_and_element_loading(self):
        selenium_wd = SeleniumWD(
            url=self.url,
            bl_headless=self.headless,
            bl_incognito=True
        )
        try:
            selenium_wd.wait(self.timeout)
            element = selenium_wd.wait_until_el_loaded(
                '//a[@class="link-black link-black-hover text-underline"]'
            )
            self.assertIsNotNone(element, "Target element not found on page")
            self.assertIn("Ratings Actions", selenium_wd.web_driver.title)
            self.assertEqual(self.url, selenium_wd.web_driver.current_url)
        finally:
            selenium_wd.web_driver.quit()

    def test_network_traffic_capture(self):
        selenium_wd = SeleniumWD(
            url=self.url,
            bl_headless=self.headless,
            bl_incognito=True
        )
        try:
            selenium_wd.wait(self.timeout)
            selenium_wd.wait_until_el_loaded(
                '//a[@class="link-black link-black-hover text-underline"]'
            )
            network_traffic = selenium_wd.get_network_traffic
            self.assertGreater(len(network_traffic), 0, "No network traffic captured")
            for entry in network_traffic:
                self.assertIn("method", entry)
                self.assertIn("params", entry)

        finally:
            selenium_wd.web_driver.quit()

if __name__ == "__main__":
    main()
