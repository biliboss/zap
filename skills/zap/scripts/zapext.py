"""Helpers of extension.sh: the pure parts (ids, update.xml, policy plist, profile checks).
stdlib only; run with `python3 -I zapext.py <command>`. No network, no sudo."""
import hashlib, json, plistlib, subprocess, sys
from pathlib import Path
from xml.sax.saxutils import quoteattr

BUNDLES = {"chrome": "com.google.Chrome", "chromium": "org.chromium.Chromium",
           "brave": "com.brave.Browser", "cft": "com.google.chrome.for.testing"}
MANIFEST_NAME = "zap \u00b7 WhatsApp Web (read only)"


def extension_id(pem):
    """Chrome's id of a packed extension: sha256 of the DER public key, 16 bytes, hex mapped 0-f -> a-p."""
    der = subprocess.run(["openssl", "rsa", "-in", str(pem), "-pubout", "-outform", "DER"],
                         check=True, capture_output=True).stdout
    return "".join(chr(ord("a") + int(c, 16)) for c in hashlib.sha256(der).hexdigest()[:32])


def update_xml(ext_id, crx_url, version):
    return ("<?xml version='1.0' encoding='UTF-8'?>\n"
            "<gupdate xmlns='http://www.google.com/update2/response' protocol='2.0'>\n"
            f"  <app appid={quoteattr(ext_id)}>\n"
            f"    <updatecheck codebase={quoteattr(crx_url)} version={quoteattr(version)} />\n"
            "  </app>\n</gupdate>\n")


def policy_plist(ext_id, update_url):
    """ExtensionInstallForcelist entries are 'id;update_url'. Exactly the pref a managed Mac reads."""
    return plistlib.dumps({"ExtensionInstallForcelist": [f"{ext_id};{update_url}"]}).decode()


def policy_has(plist_path, ext_id):
    try:
        data = plistlib.loads(Path(plist_path).read_bytes())
    except (OSError, plistlib.InvalidFileException, ValueError):
        return False
    return any(str(e).split(";")[0] == ext_id for e in data.get("ExtensionInstallForcelist", []))


def profile_has(profile, ext_id=None, ext_path=None):
    """Is the extension registered in this profile? Looks at Preferences and Secure Preferences."""
    for name in ("Secure Preferences", "Preferences"):
        try:
            prefs = json.loads((Path(profile) / name).read_text())
        except (OSError, ValueError):
            continue
        for k, v in (prefs.get("extensions", {}).get("settings", {}) or {}).items():
            if not isinstance(v, dict):
                continue
            if k == ext_id or (ext_path and v.get("path") and Path(v["path"]).resolve() == Path(ext_path).resolve()):
                if v.get("state", 1) == 1 or "state" not in v:
                    return k
    return None


def main(a):
    c = a[0]
    if c == "id":
        print(extension_id(a[1]))
    elif c == "update-xml":
        print(update_xml(a[1], a[2], a[3]), end="")
    elif c == "plist":
        print(policy_plist(a[1], a[2]), end="")
    elif c == "policy-has":
        sys.exit(0 if policy_has(a[1], a[2]) else 1)
    elif c == "profile-has":
        k = profile_has(a[1], a[2] or None, a[3] if len(a) > 3 else None)
        print(k or "")
        sys.exit(0 if k else 1)
    elif c == "serve":  # serve <dir> <port> for 120 s on loopback, then exit
        import functools, http.server, threading
        s = http.server.ThreadingHTTPServer(("127.0.0.1", int(a[2])), functools.partial(
            http.server.SimpleHTTPRequestHandler, directory=a[1]))
        threading.Timer(120, s.shutdown).start()
        s.serve_forever()
    elif c == "bundle":
        print(BUNDLES[a[1]])
    else:
        sys.exit(f"zapext: unknown command {c}")


if __name__ == "__main__":
    main(sys.argv[1:])
