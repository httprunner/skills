# ADB reference

## Setup and selection

- List devices: `adb devices -l`
- Target a device: `adb -s <device_id> <command>`
- Restart server:
  - `adb kill-server`
  - `adb start-server`

## Connection (Wi-Fi)

- Enable tcpip (USB required): `adb -s <device_id> tcpip 5555`
- Get device IP:
  - `adb -s <device_id> shell ip route`
  - `adb -s <device_id> shell ip addr show wlan0`
- Connect: `adb connect <ip>:5555`
- Disconnect:
  - `adb disconnect <ip>:5555`
  - `adb disconnect`

## Device info

- Current activity/window focus:
  - `adb -s <device_id> shell dumpsys window | rg -n "mCurrentFocus|mFocusedApp"`
- Screen size: `adb -s <device_id> shell wm size`
- Screen density: `adb -s <device_id> shell wm density`

## App control

- Launch app (monkey):
  - `adb -s <device_id> shell monkey -p <package> -c android.intent.category.LAUNCHER 1`
- Force-stop app: `adb -s <device_id> shell am force-stop <package>`

## Input

- Tap: `adb -s <device_id> shell input tap <x> <y>`
- Long press:
  - `adb -s <device_id> shell input swipe <x> <y> <x> <y> <duration_ms>`
- Swipe:
  - `adb -s <device_id> shell input swipe <x1> <y1> <x2> <y2> <duration_ms>`
- Key events:
  - Back: `adb -s <device_id> shell input keyevent 4`
  - Home: `adb -s <device_id> shell input keyevent KEYCODE_HOME`
  - Enter: `adb -s <device_id> shell input keyevent 66`

## Text input

### ADB Keyboard (preferred)

- Set IME: `adb -s <device_id> shell ime set com.android.adbkeyboard/.AdbIME`
- Send text (base64):
  - `adb -s <device_id> shell am broadcast -a ADB_INPUT_B64 --es msg <base64>`
- Clear text:
  - `adb -s <device_id> shell am broadcast -a ADB_CLEAR_TEXT`

### `input text` (escape required)

- Basic: `adb -s <device_id> shell input text '<escaped>'`
- Escaping hints:
  - Space: replace with `%s`
  - Single quote: escape for shell or use double quotes
  - Backslash: `\\`

## Screenshots

- Fast local capture:
  - `adb -s <device_id> exec-out screencap -p > screen.png`
- Two-step fallback:
  - `adb -s <device_id> shell screencap -p /sdcard/screen.png`
  - `adb -s <device_id> pull /sdcard/screen.png .`

## UI tree

- Dump UI XML:
  - `adb -s <device_id> shell uiautomator dump /sdcard/ui.xml`
  - `adb -s <device_id> pull /sdcard/ui.xml .`
