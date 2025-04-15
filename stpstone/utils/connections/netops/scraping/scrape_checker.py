from urllib import robotparser


class ScrapeChecker:

    def bl_allowed(self, url: str, user_agent: str = "*") -> bool:
        rp = robotparser.RobotFileParser()
        rp.set_url(url + "/robots.txt")
        rp.read()
        return rp.can_fetch(user_agent, url)
