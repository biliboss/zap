# Unit test of the pure parts of extension.sh: id, update.xml, policy plist, profile checks.
#   python3 -I tests/extension/test_policy.py
import importlib.util, json, plistlib, subprocess, tempfile, unittest
import xml.etree.ElementTree as ET
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parents[2] / "skills" / "zap" / "scripts"
spec = importlib.util.spec_from_file_location("zapext", SCRIPTS / "zapext.py")
z = importlib.util.module_from_spec(spec)
spec.loader.exec_module(z)
ID = "a" * 32
URL = "http://127.0.0.1:8765/update.xml"


class T(unittest.TestCase):
    def test_id_from_key(self):
        with tempfile.TemporaryDirectory() as d:
            pem = Path(d) / "k.pem"
            subprocess.run(["openssl", "genrsa", "-out", str(pem), "2048"], check=True, capture_output=True)
            i = z.extension_id(pem)
            self.assertRegex(i, r"^[a-p]{32}$")
            self.assertEqual(i, z.extension_id(pem))

    def test_update_xml(self):
        root = ET.fromstring(z.update_xml(ID, "http://127.0.0.1:8765/z-0.1.0.crx", "0.1.0"))
        ns = "{http://www.google.com/update2/response}"
        app = root.find(ns + "app")
        self.assertEqual(app.get("appid"), ID)
        uc = app.find(ns + "updatecheck")
        self.assertEqual((uc.get("codebase"), uc.get("version")), ("http://127.0.0.1:8765/z-0.1.0.crx", "0.1.0"))

    def test_plist(self):
        p = plistlib.loads(z.policy_plist(ID, URL).encode())
        self.assertEqual(p, {"ExtensionInstallForcelist": [f"{ID};{URL}"]})

    def test_policy_has(self):
        with tempfile.TemporaryDirectory() as d:
            f = Path(d) / "com.google.Chrome.plist"
            self.assertFalse(z.policy_has(f, ID))
            f.write_text(z.policy_plist(ID, URL))
            self.assertTrue(z.policy_has(f, ID))
            self.assertFalse(z.policy_has(f, "b" * 32))

    def test_profile_has(self):
        with tempfile.TemporaryDirectory() as d:
            self.assertIsNone(z.profile_has(d, ID))
            (Path(d) / "Preferences").write_text(json.dumps({"extensions": {"settings": {ID: {"state": 1}, "x": {"state": 0, "path": "/nope"}}}}))
            self.assertEqual(z.profile_has(d, ID), ID)
            self.assertEqual(z.profile_has(d, "c" * 32, "/nope"), None)  # disabled
            (Path(d) / "Secure Preferences").write_text(json.dumps({"extensions": {"settings": {"unpacked": {"path": d, "state": 1}}}}))
            self.assertEqual(z.profile_has(d, None, d), "unpacked")

    def test_bundles(self):
        self.assertEqual(z.BUNDLES["chrome"], "com.google.Chrome")


unittest.main()
