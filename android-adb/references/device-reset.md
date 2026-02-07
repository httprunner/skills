# Device Reset For Test Setup

Use this workflow to reset one Android device before test execution.

## Required Inputs

- `serial`: target device serial (required only when multiple devices are online)
- `package_name`: full package or alias
- `apk_path`: APK absolute/relative path

## Package Alias Map

- `douyin` / `抖音` / `aweme` -> `com.ss.android.ugc.aweme`
- `kuaishou` / `快手` / `gifmaker` -> `com.smile.gifmaker`
- `wechat` / `微信` / `weixin` -> `com.tencent.mm`

## Workflow

1. Uninstall target app (allow failure):
```bash
adb uninstall "$PACKAGE_NAME" || true
# multiple devices: adb -s "$SERIAL" uninstall "$PACKAGE_NAME" || true
```
2. Clean app trace files:
```bash
adb shell rm -rf /sdcard/Documents/.Android_*
# multiple devices: adb -s "$SERIAL" shell rm -rf /sdcard/Documents/.Android_*
```
3. Toggle airplane mode:
```bash
adb shell cmd connectivity airplane-mode enable
sleep 5
adb shell cmd connectivity airplane-mode disable
# multiple devices:
adb -s "$SERIAL" shell cmd connectivity airplane-mode enable
sleep 5
adb -s "$SERIAL" shell cmd connectivity airplane-mode disable
```
4. Reinstall APK with dialog handling:
```bash
npx tsx scripts/adb_helpers.ts install-smart "$APK_PATH"
# multiple devices:
npx tsx scripts/adb_helpers.ts -s "$SERIAL" install-smart "$APK_PATH"
```

Run step 4 in the `android-adb` skill directory.

## Rules

- Follow normal ADB targeting rule: pass `serial` only when multiple devices are online.
- Continue when uninstall fails.
- Delegate install and verification handling to `install-smart`.
