#!/bin/sh
# extension.sh: install the read-only WhatsApp Web extension with what the harness offers.
#   extension.sh install|status|update|remove|serve [--browser chrome|chromium|brave|cft]
#                [--method auto|policy|flag|computer-use|manual] [--profile Default] [--dry-run] [--probe]
#
# The extension is not in the Chrome Web Store, so each method is a different way around that:
#   policy        a managed-preferences ExtensionInstallForcelist + a packed .crx. Needs admin: this
#                 script PRINTS the sudo commands and never runs sudo.
#   flag          a SEPARATE profile launched with --load-extension (Chromium, Chrome for Testing,
#                 Brave; Google Chrome stable dropped the flag in 137 and is refused).
#   computer-use  prints a machine-readable plan for an agent that can drive the desktop.
#   manual        the 4 clicks, in Portuguese, for the person.
# auto = policy when the user is an admin, else flag when the browser accepts it, else computer-use
# when ZAP_HAS_COMPUTER_USE=1, else manual.
# Pairing/login of WhatsApp Web stays the owner's. Nothing here opens a chat.
# Env: ZAP_EXT_HOME (data dir), ZAP_EXT_SRC (extension folder), ZAP_EXT_PORT, ZAP_BROWSER_BIN,
#      ZAP_BROWSER_PROFILE (a profile dir for status).
set -eu

HERE=$(cd "$(dirname "$0")" && pwd)
SRC=$(cd "${ZAP_EXT_SRC:-$HERE/../../../extension}" 2>/dev/null && pwd) || { echo "extension: extension folder not found (set ZAP_EXT_SRC)" >&2; exit 1; }
REPO=$(cd "$SRC/.." && pwd)
DATA=${ZAP_EXT_HOME:-$HOME/.local/share/zap/extension}
PORT=${ZAP_EXT_PORT:-8765}
PEM=$DATA/zap-extension.pem
HELPER="python3 -I $HERE/zapext.py"
MANAGED="/Library/Managed Preferences"

cmd=${1:-}
case $cmd in -h|--help) sed -n '2,19p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;; esac
[ $# -gt 0 ] && shift
browser=chrome method=auto profile=Default dry=0 probe=0
while [ $# -gt 0 ]; do
  case $1 in
    --browser) browser=$2; shift 2 ;;
    --method) method=$2; shift 2 ;;
    --profile) profile=$2; shift 2 ;;
    --dry-run) dry=1; shift ;;
    --probe) probe=1; shift ;;
    -h|--help) sed -n '2,19p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "extension: unknown option $1" >&2; exit 2 ;;
  esac
done
case $browser in chrome|chromium|brave|cft) ;; *) echo "extension: --browser is chrome|chromium|brave|cft" >&2; exit 2 ;; esac
case $method in auto|policy|flag|computer-use|manual) ;; *) echo "extension: --method is auto|policy|flag|computer-use|manual" >&2; exit 2 ;; esac

die() { echo "extension: $*" >&2; exit 1; }

bundle=$($HELPER bundle "$browser")
version=$(sed -n 's/.*"version": *"\([^"]*\)".*/\1/p' "$SRC/manifest.json" 2>/dev/null | head -1)

browser_bin() {
  if [ -n "${ZAP_BROWSER_BIN:-}" ]; then echo "$ZAP_BROWSER_BIN"; return; fi
  case $browser in
    chrome) echo "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" ;;
    chromium) echo "/Applications/Chromium.app/Contents/MacOS/Chromium" ;;
    brave) echo "/Applications/Brave Browser.app/Contents/MacOS/Brave Browser" ;;
    cft) for p in "$HOME"/.cache/puppeteer/chrome/*/chrome-mac*/"Google Chrome for Testing.app/Contents/MacOS/Google Chrome for Testing" \
                  /Applications/"Google Chrome for Testing.app/Contents/MacOS/Google Chrome for Testing"; do
           [ -x "$p" ] && { echo "$p"; return; }
         done
         echo "" ;;
  esac
}
bin=$(browser_bin)
need_bin() { [ -n "$bin" ] && [ -x "$bin" ] || die "$browser not found${bin:+ at $bin}; set ZAP_BROWSER_BIN"; }
major() { "$bin" --version 2>/dev/null | sed -n 's/[^0-9]*\([0-9][0-9]*\)\..*/\1/p' | head -1; }

profile_dir() {
  if [ -n "${ZAP_BROWSER_PROFILE:-}" ]; then echo "$ZAP_BROWSER_PROFILE"; return; fi
  if [ "$method" = flag ]; then echo "$DATA/profile-$browser/Default"; return; fi
  case $browser in
    chrome) echo "$HOME/Library/Application Support/Google/Chrome/$profile" ;;
    chromium) echo "$HOME/Library/Application Support/Chromium/$profile" ;;
    brave) echo "$HOME/Library/Application Support/BraveSoftware/Brave-Browser/$profile" ;;
    cft) echo "$HOME/Library/Application Support/Google/Chrome for Testing/$profile" ;;
  esac
}

is_admin() { id -Gn | tr ' ' '\n' | grep -qx admin; }

resolve_method() {
  [ "$method" != auto ] && return
  if is_admin; then method=policy
  elif need_flag_ok; then method=flag
  elif [ "${ZAP_HAS_COMPUTER_USE:-0}" = 1 ]; then method=computer-use
  else method=manual; fi
}
need_flag_ok() {
  [ -n "$bin" ] && [ -x "$bin" ] || return 1
  [ "$browser" = chrome ] && [ "$(major)" -ge 137 ] 2>/dev/null && return 1
  return 0
}

pack() {
  need_bin
  mkdir -p "$DATA"; chmod 700 "$DATA"
  stage=$DATA/pack/zap-extension
  rm -rf "$DATA/pack"; mkdir -p "$DATA/pack"; cp -R "$SRC" "$stage"
  tmpprof=$(mktemp -d "${TMPDIR:-/tmp}/zap-pack.XXXXXX")
  if [ -f "$PEM" ]; then
    "$bin" --user-data-dir="$tmpprof" --pack-extension="$stage" --pack-extension-key="$PEM" >/dev/null 2>&1 || true
  else
    "$bin" --user-data-dir="$tmpprof" --pack-extension="$stage" >/dev/null 2>&1 || true
    [ -f "$DATA/pack/zap-extension.pem" ] && { mv "$DATA/pack/zap-extension.pem" "$PEM"; chmod 600 "$PEM"; }
  fi
  rm -rf "$tmpprof"
  [ -f "$DATA/pack/zap-extension.crx" ] || die "packing failed with $bin"
  mv "$DATA/pack/zap-extension.crx" "$DATA/zap-extension-$version.crx"
  rm -rf "$DATA/pack"
  ext_id=$($HELPER id "$PEM")
}

do_policy() {
  pack
  url="http://127.0.0.1:$PORT/update.xml"
  $HELPER update-xml "$ext_id" "http://127.0.0.1:$PORT/zap-extension-$version.crx" "$version" > "$DATA/update.xml"
  $HELPER plist "$ext_id" "$url" > "$DATA/$bundle.plist"
  echo "extension id: $ext_id   version: $version   (the key stays in $DATA, never commit it)"
  echo "packed: $DATA/zap-extension-$version.crx"
  [ -f "$MANAGED/$bundle.plist" ] && echo "WARNING: $MANAGED/$bundle.plist already exists (MDM or an earlier policy). The cp below replaces it: merge ExtensionInstallForcelist by hand instead if it holds other policies."
  echo
  echo "Needs admin: the owner runs these (this script never runs sudo):"
  echo "  sudo mkdir -p \"$MANAGED\""
  echo "  sudo cp \"$DATA/$bundle.plist\" \"$MANAGED/$bundle.plist\""
  echo "  sudo chown root:wheel \"$MANAGED/$bundle.plist\" && sudo chmod 644 \"$MANAGED/$bundle.plist\""
  echo "  sudo killall cfprefsd"
  echo
  echo "Then serve the package while the browser fetches it (bounded, 120 s):"
  echo "  $0 serve"
  echo "Quit and reopen the browser. Verify at chrome://policy (Reload policies: ExtensionInstallForcelist, status OK)"
  echo "and chrome://extensions (\"Installed by your administrator\"); or run: $0 status --browser $browser"
  echo "Caveat: on macOS the browser may ignore force-install from outside the Web Store on a device that is not managed (MDM). If chrome://policy shows the policy but the extension never appears, use --method flag or manual."
}

do_flag() {
  need_bin
  if [ "$browser" = chrome ]; then
    m=$(major)
    if [ -n "$m" ] && [ "$m" -ge 137 ]; then
      die "Google Chrome $m ignores --load-extension (removed in 137). Use --browser chromium|brave|cft, or --method policy|manual."
    fi
  fi
  pdir=$DATA/profile-$browser
  mkdir -p "$pdir"
  set -- "$bin" "--user-data-dir=$pdir" "--load-extension=$SRC" --no-first-run --no-default-browser-check https://web.whatsapp.com/
  if [ "$dry" = 1 ]; then echo "would run: $*"; return; fi
  nohup "$@" >/dev/null 2>&1 &
  echo "launched $browser with a SEPARATE profile: $pdir"
  echo "The owner scans the WhatsApp Web QR in that window (a login there is separate from the daily browser). Then follow chats in the zap popup."
}

do_computer_use() {
  cat <<EOF
{"method":"computer-use","browser":"$browser","extension_dir":"$(cd "$SRC" && pwd)","steps":[
 {"do":"open_url","url":"chrome://extensions"},
 {"do":"toggle","label":"Developer mode","state":"on"},
 {"do":"click","label":"Load unpacked"},
 {"do":"choose_folder","path":"$(cd "$SRC" && pwd)"},
 {"do":"verify","expect":"card named 'zap · WhatsApp Web (read only)' enabled, no errors"}],
 "then":"$0 status --browser $browser --method computer-use",
 "never":"do not scan the WhatsApp QR and do not open any chat: that is the owner's"}
EOF
}

do_manual() {
  d=$(cd "$SRC" && pwd)
  cat <<EOF
Instalação manual (4 cliques), no navegador ($browser):
  1. Abra chrome://extensions e ligue "Modo do desenvolvedor" (canto superior direito).
  2. Clique em "Carregar sem compactação".
  3. Escolha a pasta: $d
  4. Fixe o ícone "zap" na barra, abra-o e digite o número ou grupo que você quer seguir.
Sem nenhum chat seguido, a extensão não lê nada. Para conferir: $0 status --browser $browser --method manual
EOF
}

do_status() {
  rc=1
  [ -f "$PEM" ] && ext_id=$($HELPER id "$PEM") || ext_id=""
  echo "extension id: ${ext_id:-unknown (no packed key yet: unpacked loads get a path-based id)}"
  if [ -f "$MANAGED/$bundle.plist" ] && [ -n "$ext_id" ] && $HELPER policy-has "$MANAGED/$bundle.plist" "$ext_id"; then
    echo "policy: present ($MANAGED/$bundle.plist)"; rc=0
  else echo "policy: absent"; fi
  pdirs=$(profile_dir)
  found=$($HELPER profile-has "$pdirs" "$ext_id" "$(cd "$SRC" 2>/dev/null && pwd)" || true)
  if [ -z "$found" ] && [ -z "${ZAP_BROWSER_PROFILE:-}" ]; then
    # the flag profile is a different folder: look there too
    found=$($HELPER profile-has "$DATA/profile-$browser/Default" "$ext_id" "$(cd "$SRC" 2>/dev/null && pwd)" || true)
    [ -n "$found" ] && pdirs=$DATA/profile-$browser/Default
  fi
  if [ -n "$found" ]; then echo "profile: registered as $found in $pdirs"; rc=0
  else echo "profile: not registered in $pdirs"; fi
  if [ "$probe" = 1 ]; then
    t=$REPO/tests/extension/test_extension.py
    [ -f "$t" ] || die "no test page: $t"
    echo "probe: loading $SRC into a throwaway Chromium with a fake WhatsApp page (never your profile)"
    if PLAYWRIGHT_BROWSERS_PATH=${ZAP_PW_BROWSERS:-$HOME/.cache/zap-ext-pw} uv run --with playwright python -I "$t" "$SRC"; then
      echo "probe: test events arrived"
    else echo "probe: FAILED"; rc=1; fi
  fi
  return $rc
}

do_remove() {
  resolve_method
  echo "removing ($method)"
  rm -f "$DATA"/zap-extension-*.crx "$DATA/update.xml" "$DATA/$bundle.plist"
  if [ -d "$DATA/profile-$browser" ]; then rm -rf "$DATA/profile-$browser"; echo "flag profile deleted: $DATA/profile-$browser (quit that browser window first)"; fi
  if [ -f "$MANAGED/$bundle.plist" ] && [ -f "$PEM" ] && $HELPER policy-has "$MANAGED/$bundle.plist" "$($HELPER id "$PEM")"; then
    echo "Needs admin: the owner runs"
    echo "  sudo rm \"$MANAGED/$bundle.plist\" && sudo killall cfprefsd"
    echo "then reopens the browser (chrome://policy > Reload policies)."
  fi
  echo "Loaded unpacked (computer-use/manual): chrome://extensions > zap · WhatsApp Web > Remover / Remove."
  echo "The signing key $PEM is kept; delete it by hand to retire the extension id."
}

case $cmd in
  install|update)
    resolve_method
    echo "method: $method  browser: $browser"
    case $method in
      policy) do_policy ;;
      flag) do_flag ;;
      computer-use) do_computer_use ;;
      manual) do_manual ;;
    esac ;;
  status) do_status ;;
  remove) do_remove ;;
  serve)
    [ -f "$DATA/update.xml" ] || die "nothing to serve: run install --method policy first"
    echo "serving $DATA on 127.0.0.1:$PORT for 120 s, then it exits"
    exec $HELPER serve "$DATA" "$PORT" ;;
  *) echo "usage: extension.sh install|status|update|remove|serve [--browser chrome|chromium|brave|cft] [--method auto|policy|flag|computer-use|manual] [--profile Default] [--dry-run] [--probe]" >&2; exit 2 ;;
esac
