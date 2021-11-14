"""Retrieve the PDF files associated with potential IEA Roadmaps

Running spider: scrapy crawl --set=ROBOTSTXT_OBEY='False' iea
"""

from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
import re, os

exec_path = "C:\\Users\\thomas\\Documents\\Thesis\\coding\\geckodriver.exe"

class Iea:
    name="iea"
    user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:78.0) Gecko/20100101 Firefox/78.0'

    def __init__(self, save_path, **kwargs):
        self.iea_pdf_save_path = save_path
        self.login_url = "https://ssologin.iea.org/account/login?ReturnUrl=/OAuth/Authorize?client_id=webstore&redirect_uri=https%3A%2F%2Fwebstore.iea.org%2FSso%2FLoginCallback%3FreturnUrl%3Dhttps%253A%252F%252Fwebstore.iea.org%252F%2523modal&state=6m58loaSDs6ud53uNwayMg&scope=https%3A%2F%2Fssoapi.iea.org%2Fapi%2Fuserdata&response_type=code&client_id=webstore&redirect_uri=https://webstore.iea.org/Sso/LoginCallback?returnUrl=https%3A%2F%2Fwebstore.iea.org%2F%23modal&state=6m58loaSDs6ud53uNwayMg&scope=https://ssoapi.iea.org/api/userdata&response_type=code"

    def start(self):
        driver = webdriver.Firefox(executable_path=exec_path)
        driver.get("https://webstore.iea.org/")
        driver.find_element_by_id("openLoginModalBtn").click()
        modal = WebDriverWait(driver, 20).until(EC.visibility_of_element_located((By.ID, "loginModal")))
        print(type(modal))
        sso_modal = modal.find_element_by_id("loginFrame").\
            find_element_by_class_name("sso-modal")
        print(type(sso_modal))
        # email = modal.find_element_by_id("loginForm").find_element_by_id("Email")
        # password = modal.find_element_by_id("loginForm").find_element_by_id("Password")
        # password = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.ID, "Password")))
        # email.send_keys("t0m_da_b0m999@hotmail.com")
        # password.send_keys("Tom33rox99!")

    def get_metadata(self, document):
        """This function will be able to extract various informative metadata from each documents json representation.
        e.g. author, year of publication etc.

        Args:
            document (dict): document containing metadata to be extracted.
        """
        return {}

    def get_pdf(self, response, **kwargs):
        """Saves pdf to 

        Args:
            response (scrapy.http.repsonse.Response): response that holds the pdf document
        """
        file_name = re.match(r".*/(.*\.pdf)", response.url).group(1)
        with open(os.path.join(self.iea_pdf_save_path, file_name), "wb") as fp:
            fp.write(response.body)

if __name__ == "__main__":
    iea = Iea("")
    iea.start()