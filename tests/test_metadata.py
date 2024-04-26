import subprocess
import time
import unittest

import requests
from dotenv import dotenv_values

config = dotenv_values(".env")


class MetadataTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server_process = subprocess.Popen(
            ["flask", "--app", "main", "run", "--debug"]
        )
        time.sleep(2)

    def setUp(self):
        self.base_url = "http://localhost:5000"

    def tearDown(self):
        return super().tearDown()

    @classmethod
    def tearDownClass(cls):
        if cls.server_process.poll() is None:
            cls.server_process.terminate()
            cls.server_process.wait()

    def test_get_table_info_without_session(self):
        response = requests.get(self.base_url + "/v1/metadata/source")
        self.assertEqual(response.status_code, 401)
        self.assertEqual(
            response.text,
            "No connection defined in current session, define session credentials first",
        )
        print("Test case: no session before calling metadata: PASSED")

    def test_get_table_with_wrong_credentials(self):
        data = {
            "local_username": config["MY_LOCAL_DB_USERNAME"],
            "local_password": config["MY_LOCAL_DB_PASSWORD"] + "x",
            "local_url": config["MY_LOCAL_DB_URL"],
            "cloud_username": config["MY_CLOUD_DB_USERNAME"],
            "cloud_password": config["MY_CLOUD_DB_PASSWORD"],
            "cloud_url": config["MY_CLOUD_DB_URL"],
        }
        cookie = requests.post(
            self.base_url + "/v1/credentials", data=data
        ).cookies.get("session")
        cookies = {"session": cookie}
        response = requests.get(self.base_url + "/v1/metadata/source", cookies=cookies)
        self.assertEqual(response.status_code, 500)
        self.assertEqual(response.text, "No connection to local database")
        print("Test case: session credentials incorrect when calling metadata: PASSED")

    def test_get_table(self):
        data = {
            "local_username": config["MY_LOCAL_DB_USERNAME"],
            "local_password": config["MY_LOCAL_DB_PASSWORD"],
            "local_url": config["MY_LOCAL_DB_URL"],
            "cloud_username": config["MY_CLOUD_DB_USERNAME"],
            "cloud_password": config["MY_CLOUD_DB_PASSWORD"],
            "cloud_url": config["MY_CLOUD_DB_URL"],
        }
        cookie = requests.post(
            self.base_url + "/v1/credentials", data=data
        ).cookies.get("session")
        cookies = {"session": cookie}

        response = requests.get(self.base_url + "/v1/metadata/source", cookies=cookies)
        expectedResponse = {
            "metadata": {
                "customers": {
                    "columns": [
                        "customerNumber",
                        "customerName",
                        "contactLastName",
                        "contactFirstName",
                        "phone",
                        "addressLine1",
                        "addressLine2",
                        "city",
                        "state",
                        "postalCode",
                        "country",
                        "salesRepEmployeeNumber",
                        "creditLimit",
                    ],
                    "rows": 122,
                },
                "employees": {
                    "columns": [
                        "employeeNumber",
                        "lastName",
                        "firstName",
                        "extension",
                        "email",
                        "officeCode",
                        "reportsTo",
                        "jobTitle",
                    ],
                    "rows": 23,
                },
                "offices": {
                    "columns": [
                        "officeCode",
                        "city",
                        "phone",
                        "addressLine1",
                        "addressLine2",
                        "state",
                        "country",
                        "postalCode",
                        "territory",
                    ],
                    "rows": 7,
                },
                "orderdetails": {
                    "columns": [
                        "orderNumber",
                        "productCode",
                        "quantityOrdered",
                        "priceEach",
                        "orderLineNumber",
                    ],
                    "rows": 2996,
                },
                "orders": {
                    "columns": [
                        "orderNumber",
                        "orderDate",
                        "requiredDate",
                        "shippedDate",
                        "status",
                        "comments",
                        "customerNumber",
                    ],
                    "rows": 326,
                },
                "payments": {
                    "columns": [
                        "customerNumber",
                        "checkNumber",
                        "paymentDate",
                        "amount",
                    ],
                    "rows": 273,
                },
                "productlines": {
                    "columns": [
                        "productLine",
                        "textDescription",
                        "htmlDescription",
                        "image",
                    ],
                    "rows": 7,
                },
                "products": {
                    "columns": [
                        "productCode",
                        "productName",
                        "productLine",
                        "productScale",
                        "productVendor",
                        "productDescription",
                        "quantityInStock",
                        "buyPrice",
                        "MSRP",
                    ],
                    "rows": 110,
                },
            },
            "recovered": {},
        }
        self.assertEqual(response.json(), expectedResponse)
        print("Test case: get source table metadata: PASSED")


if __name__ == "__main__":
    unittest.main()
