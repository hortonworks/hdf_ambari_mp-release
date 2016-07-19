from unittest import TestCase
from tempfile import mkdtemp
from shutil import rmtree
import json, nifi_ca_util, nifi_constants, os, unitTests

class TestNifiCAUtil(TestCase):
  def setUp(self):
    self.temp_dir = mkdtemp()

    self.testKey1 = 'testKey1'
    self.testKey2 = 'testKey2'
    self.testVal1 = 'testVal1'
    self.testVal2 = 'testVal2'

    self.test_dict = {
      self.testKey1: self.testVal1,
      self.testKey2: self.testVal2
    }

    self.test_json = os.path.join(self.temp_dir, 'test.json')
    nifi_ca_util.dump_config(self.test_json, self.test_dict)

    self.execute_key = 'execute_key'
    self.execute_value = 'execute'

  def teardown(self):
    rmtree(self.temp_dir)

  def test_dump_load_config(self):
    config_dict = nifi_ca_util.load_config(self.test_json)
    self.assertEqual(self.test_dict, config_dict)

  def test_load_and_overlay_config(self):
    config_dict = nifi_ca_util.load_and_overlay_config(self.test_json, {self.testKey2: 'overlay', 'newKey': ''})
    self.test_dict[self.testKey2] = 'overlay'
    self.test_dict['newKey'] = ''
    self.assertFalse(self.test_dict['newKey'])
    self.assertEqual(self.test_dict, config_dict)

  def test_load_overlay_dump(self):
    config_dict = nifi_ca_util.load_overlay_dump(self.test_json, {self.testKey2: 'overlay', 'newKey': ''})
    self.test_dict[self.testKey2] = 'overlay'
    self.test_dict['newKey'] = ''
    self.assertEqual(self.test_dict, config_dict)
    self.assertEqual(self.test_dict, nifi_ca_util.load_config(self.test_json))

  def helper_test_load_overlay_dump_and_execute(self):
    self.test_dict_orig = nifi_ca_util.load_config(self.test_json)
    nifi_ca_util.dump_config(self.test_json, {self.execute_key: self.execute_value})

  def test_load_overlay_dump_and_execute(self):
    config_dict = nifi_ca_util.load_overlay_dump_and_execute(self.test_json, {self.testKey2: 'overlay', 'newKey': ''}, lambda: self.helper_test_load_overlay_dump_and_execute())
    self.test_dict[self.testKey2] = 'overlay'
    self.test_dict['newKey'] = ''
    self.assertEqual(self.test_dict, self.test_dict_orig)
    self.assertEqual({self.execute_key: self.execute_value}, config_dict)

  def test_get_toolkit_script_fail_directory(self):
    with self.assertRaises(Exception) as context:
      nifi_ca_util.get_toolkit_script('tls-toolkit.sh', '/a/fake/path')

  def test_get_toolkit_script_fail_file(self):
    with self.assertRaises(Exception) as context:
      nifi_ca_util.get_toolkit_script('fakeScript.sh', os.path.join(unitTests.up(__file__, 9), 'target/nifi-toolkit'))

  def test_get_toolkit_script_success(self):
    nifi_ca_util.get_toolkit_script('tls-toolkit.sh', os.path.join(unitTests.up(__file__, 9), 'target/nifi-toolkit'))

  def test_update_unifi_properties(self):
    keyStoreType = 'testKeyStoreType'
    keyStorePassword = 'testKeyStorePassword'
    keyPassword = 'testKeyPassword'
    trustStoreType = 'testTrustStoreType'
    trustStorePassword = 'testTrustStorePassword'
    fakeKey = 'fakeKey'

    nifi_properties = {}

    nifi_ca_util.update_nifi_properties({
        'keyStoreType': keyStoreType, 
        'keyStorePassword': keyStorePassword, 
        'keyPassword': keyPassword, 
        'trustStoreType': trustStoreType, 
        'trustStorePassword': trustStorePassword, 
        'fakeKey': fakeKey
      }, nifi_properties)

    self.assertEqual(keyStoreType, nifi_properties[nifi_constants.NIFI_SECURITY_KEYSTORE_TYPE])
    self.assertEqual(keyStorePassword, nifi_properties[nifi_constants.NIFI_SECURITY_KEYSTORE_PASSWD])
    self.assertEqual(keyPassword, nifi_properties[nifi_constants.NIFI_SECURITY_KEY_PASSWD])
    self.assertEqual(trustStoreType, nifi_properties[nifi_constants.NIFI_SECURITY_TRUSTSTORE_TYPE])
    self.assertEqual(trustStorePassword, nifi_properties[nifi_constants.NIFI_SECURITY_TRUSTSTORE_PASSWD])
