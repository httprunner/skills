# ADB reference (android-adb-go)

## ADB server

- Start server:
  - `go run scripts/adb_helpers.go start-server`
- Kill server:
  - `go run scripts/adb_helpers.go kill-server`

## Device discovery and selection

- List devices and get serial:
  - `go run scripts/adb_helpers.go devices`
- Target a device:
  - `go run scripts/adb_helpers.go -s SERIAL <command>`

## Connection (Wi-Fi)

- Enable tcpip (USB required):
  - `go run scripts/adb_helpers.go -s SERIAL enable-tcpip [port]`
- Get device IP:
  - `go run scripts/adb_helpers.go -s SERIAL get-ip`
- Connect:
  - `go run scripts/adb_helpers.go connect <ip>:5555`
- Disconnect:
  - `go run scripts/adb_helpers.go disconnect [ip]:5555`

## Device info

- Screen size:
  - `go run scripts/adb_helpers.go -s SERIAL wm-size`
- Current foreground app:
  - `go run scripts/adb_helpers.go -s SERIAL get-current-app`

## App control

- Check app installed (replace with your package):
  - `go run scripts/adb_helpers.go -s SERIAL shell pm list packages | rg -n "<package>"`
- Launch app:
  - By package: `go run scripts/adb_helpers.go -s SERIAL launch <package>`
  - By activity: `go run scripts/adb_helpers.go -s SERIAL launch <package>/<activity>`
  - By schema/URI: `go run scripts/adb_helpers.go -s SERIAL launch <schema://path>`
- Stop app (force-stop):
  - `go run scripts/adb_helpers.go -s SERIAL force-stop <package>`

## Input actions

- Tap:
  - `go run scripts/adb_helpers.go -s SERIAL tap X Y`
- Double tap:
  - `go run scripts/adb_helpers.go -s SERIAL double-tap X Y`
- Long press:
  - `go run scripts/adb_helpers.go -s SERIAL long-press X Y [--duration-ms N]`
- Swipe:
  - `go run scripts/adb_helpers.go -s SERIAL swipe X1 Y1 X2 Y2 [--duration-ms N]`
- Keyevent (examples):
  - Back: `go run scripts/adb_helpers.go -s SERIAL keyevent KEYCODE_BACK`
  - Home: `go run scripts/adb_helpers.go -s SERIAL keyevent KEYCODE_HOME`
  - Enter: `go run scripts/adb_helpers.go -s SERIAL keyevent KEYCODE_ENTER`
- Go back multiple times to reach home (adds small random delays):
  - `for i in {1..5}; do go run scripts/adb_helpers.go -s SERIAL keyevent KEYCODE_BACK; sleep 0.$((RANDOM%6+5)); done`

## Text input (ADBKeyboard)

- Clear text:
  - `go run scripts/adb_helpers.go -s SERIAL clear-text`
- Input text:
  - `go run scripts/adb_helpers.go -s SERIAL text --adb-keyboard "YOUR_TEXT"`

## Screenshots

- Capture to file:
  - `go run scripts/adb_helpers.go -s SERIAL screenshot --out "<path>/shot.png"`

## UI tree

- Dump UI:
  - `go run scripts/adb_helpers.go -s SERIAL dump-ui [--out path] [--parse]`
