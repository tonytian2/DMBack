import unittest
import subprocess
import requests
import time
from dotenv import dotenv_values


config = dotenv_values('.env')
class CredentialsTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server_process = subprocess.Popen(['flask', '--app', 'main','run','--debug'])
        time.sleep(2)

    def setUp(self):
        self.base_url="http://localhost:5000"

    @classmethod
    def tearDownClass(cls):
        if cls.server_process.poll() is None:
            cls.server_process.terminate()
            cls.server_process.wait()

    def test_missing_field(self):
        data = {
            'local_username': "test_username",
            'local_password': "test_password",
            #'local_url': "test_url",
            'cloud_username': "test_username",
            'cloud_password': "test_password",
            'cloud_url': "test_url"
        }
        response = requests.post(self.base_url + '/v1/credentials',data=data)
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.text, "Missing required field: local_url")
        print('Test case: missing field PASSED')

    def test_wrong_local_credentials(self):
        data = {
            'local_username': config['MY_LOCAL_DB_USERNAME'],
            'local_password': config['MY_LOCAL_DB_PASSWORD']+"x",
            'local_url': config['MY_LOCAL_DB_URL'],
            'cloud_username': config['MY_CLOUD_DB_USERNAME'],
            'cloud_password': config['MY_CLOUD_DB_PASSWORD'],
            'cloud_url': config['MY_CLOUD_DB_URL']
        }
        response = requests.post(self.base_url + '/v1/credentials', data=data)
        self.assertEqual(response.status_code, 500)
        self.assertEqual(response.text,"Local credentials incorrect")
        print("Test case: wrong local credentials PASSED")

    def test_wrong_cloud_credentials(self):
        data = {
            'local_username': config['MY_LOCAL_DB_USERNAME'],
            'local_password': config['MY_LOCAL_DB_PASSWORD'],
            'local_url': config['MY_LOCAL_DB_URL'],
            'cloud_username': config['MY_CLOUD_DB_USERNAME'],
            'cloud_password': config['MY_CLOUD_DB_PASSWORD']+"x",
            'cloud_url': config['MY_CLOUD_DB_URL']
        }
        response = requests.post(self.base_url + '/v1/credentials', data=data)
        self.assertEqual(response.status_code,500)
        self.assertEqual(response.text,"Cloud credentials incorrect")
        print("Test case: wrong_cloud_credentials PASSED")

    def test_correct_credentials(self):
        data = {
            'local_username': config['MY_LOCAL_DB_USERNAME'],
            'local_password': config['MY_LOCAL_DB_PASSWORD'],
            'local_url': config['MY_LOCAL_DB_URL'],
            'cloud_username': config['MY_CLOUD_DB_USERNAME'],
            'cloud_password': config['MY_CLOUD_DB_PASSWORD'],
            'cloud_url': config['MY_CLOUD_DB_URL']
        }
        response = requests.post(self.base_url + '/v1/credentials', data=data)
        self.assertEqual(response.status_code,200)
        self.assertEqual(response.text,"OK")
        print("Test case: correct_credentials PASSED")

    def test_delete_session(self):
        response = requests.delete(self.base_url + '/v1/session')
        self.assertEqual(response.status_code,200)
        self.assertEqual(response.text,"OK")
        print("Test case: clear_session PASSED")

if __name__ == '__main__':
    unittest.main()