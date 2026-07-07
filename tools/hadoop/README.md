# Windows Hadoop Natives

This directory contains the Windows native files used only for local Spark and Delta
execution on Windows.

- Source: `cdarlint/winutils`
- Upstream path: `hadoop-3.3.5/bin/`
- Files vendored here: `winutils.exe`, `hadoop.dll`

Why this exists:

- PySpark ships Hadoop Java jars but not the Windows native DLL required by local
  Delta filesystem operations.
- Linux CI does not need these files.
- This scaffold vendors the minimum native files needed so the local offline
  Spark + Delta smoke test can run on Windows.
