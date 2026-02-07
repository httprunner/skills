---
name: android-device-reset
description: Reset one Android device for app test setup by uninstalling a target package, cleaning /sdcard/Documents/.Android_* files, toggling airplane mode, and installing an APK on a single device identified by an explicit serial.
---

# Android Device Reset

## Inputs

- `package_name`: package name or alias
- `apk_path`: APK absolute/relative path
- `serial` (required): target device serial (`DEVICE="$SERIAL"`)

## Resolve package name

- `douyin` / `抖音` / `aweme` -> `com.ss.android.ugc.aweme`
- `kuaishou` / `快手` / `gifmaker` -> `com.smile.gifmaker`
- `wechat` / `微信` / `weixin` -> `com.tencent.mm`

## Workflow

1. Uninstall app: `adb -s "$DEVICE" uninstall "$PACKAGE_NAME" || true`
2. Clean directory: `adb -s "$DEVICE" shell rm -rf /sdcard/Documents/.Android_*`
3. Toggle airplane mode:
- enable: `adb -s "$DEVICE" shell cmd connectivity airplane-mode enable`
- wait 5 seconds
- disable: `adb -s "$DEVICE" shell cmd connectivity airplane-mode disable`
4. Install APK (delegate to `android-adb`):
   - command: `npx tsx scripts/adb_helpers.ts -s "$DEVICE" install-smart "$APK_PATH"`
   - run command in `android-adb` skill directory

## Execution rules

- Require explicit `serial`.
- Continue if uninstall fails.
- Delegate install/UI handling to `android-adb` `install-smart`.
