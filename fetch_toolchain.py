import os, sys, urllib.request, shutil

OUT_DIR = r"C:\Users\Administrator\WorkBuddy\2026-07-18-17-52-45\toolchain"
os.makedirs(OUT_DIR, exist_ok=True)
SEVEN_ZIP = os.path.join(OUT_DIR, "mingw64.7z")
EXTRACT_TO = os.path.join(OUT_DIR, "mingw64")

URL = ("https://github.com/niXman/mingw-builds-binaries/releases/download/"
       "16.1.0-rt_v14-rev1/x86_64-16.1.0-release-posix-seh-msvcrt-rt_v14-rev1.7z")

print(">> downloading MinGW-w64 toolchain ...")
req = urllib.request.Request(URL, headers={"User-Agent": "Mozilla/5.0"})
with urllib.request.urlopen(req, timeout=120) as resp, open(SEVEN_ZIP, "wb") as f:
    total = int(resp.headers.get("Content-Length", 0) or 0)
    done = 0
    chunk = 1024 * 1024
    while True:
        buf = resp.read(chunk)
        if not buf:
            break
        f.write(buf)
        done += len(buf)
        if total:
            print(f"   {done/1024/1024:.1f}/{total/1024/1024:.1f} MB ({done*100//total}%)", end="\r")
print(f"\n>> saved: {SEVEN_ZIP} ({os.path.getsize(SEVEN_ZIP)/1024/1024:.1f} MB)")

print(">> installing py7zr ...")
import subprocess
py = r"C:\Users\Administrator\.workbuddy\binaries\python\envs\default\Scripts\pip.exe"
subprocess.check_call([py, "install", "py7zr", "-q"])
print(">> extracting ...")
import py7zr
if os.path.isdir(EXTRACT_TO):
    shutil.rmtree(EXTRACT_TO)
with py7zr.SevenZipFile(SEVEN_ZIP, "r") as z:
    z.extractall(OUT_DIR)
print(">> done. listing bin:")
for n in ("gcc.exe", "g++.exe", "ld.exe", "ar.exe", "windres.exe"):
    p = os.path.join(EXTRACT_TO, "bin", n)
    print("   ", n, "->", "OK" if os.path.exists(p) else "MISSING")
