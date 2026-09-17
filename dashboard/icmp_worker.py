"""One bounded ICMP probe in a child process on the dashboard host.

Windows uses IcmpSendEcho, Linux uses iputils ping. No HTTP fallback.
Only this file is executed as a child; it does not import pipeline code.
"""

import ctypes
import ipaddress
import json
import math
import os
import re
import shutil
import socket
import struct
import subprocess
import sys


def windows_probe(address: str, timeout: float) -> dict:
    from ctypes import wintypes

    library = ctypes.WinDLL("iphlpapi.dll", use_last_error=True)
    library.IcmpCreateFile.restype = wintypes.HANDLE
    library.IcmpCloseHandle.argtypes = [wintypes.HANDLE]
    library.IcmpCloseHandle.restype = wintypes.BOOL
    library.IcmpSendEcho.argtypes = [
        wintypes.HANDLE,
        wintypes.ULONG,
        ctypes.c_void_p,
        wintypes.WORD,
        ctypes.c_void_p,
        ctypes.c_void_p,
        wintypes.DWORD,
        wintypes.DWORD,
    ]
    library.IcmpSendEcho.restype = wintypes.DWORD
    handle = library.IcmpCreateFile()
    if handle == ctypes.c_void_p(-1).value:
        raise ctypes.WinError(ctypes.get_last_error())
    try:
        payload = ctypes.create_string_buffer(b"EMP dashboard ICMP")
        reply = ctypes.create_string_buffer(4096)
        destination = int.from_bytes(socket.inet_aton(address), "little")
        count = library.IcmpSendEcho(
            handle,
            destination,
            payload,
            len(payload),
            None,
            reply,
            len(reply),
            math.ceil(timeout * 1000),
        )
        if not count:
            status = ctypes.get_last_error()
            if status in {11002, 11003, 11004, 11005, 11009, 11010, 11013, 11014}:
                return {
                    "status": "no_reply",
                    "reason": "timeout" if status == 11010 else f"icmp_status_{status}",
                    "rtt_ms": None,
                }
            raise OSError(status, "ICMP request failed.")
        # These first three DWORD fields are identical in the 32/64-bit reply layouts.
        source, status, elapsed = struct.unpack_from("<III", reply.raw)
        if status != 0 or source != destination:
            return {
                "status": "no_reply",
                "reason": f"icmp_status_{status}",
                "rtt_ms": None,
            }
        return {"status": "reply", "reason": None, "rtt_ms": elapsed}
    finally:
        library.IcmpCloseHandle(handle)


def linux_probe(address: str, timeout: float) -> dict:
    executable = shutil.which("ping")
    if executable is None:
        raise OSError("iputils ping is not installed on the dashboard host.")
    result = subprocess.run(
        [
            executable,
            "-4",
            "-n",
            "-c",
            "1",
            "-W",
            str(timeout),
            "-w",
            str(math.ceil(timeout + 1)),
            address,
        ],
        capture_output=True,
        timeout=timeout + 2,
        check=False,
        env={**os.environ, "LC_ALL": "C"},
    )
    if result.returncode == 1:
        return {"status": "no_reply", "reason": "no_echo_reply", "rtt_ms": None}
    if result.returncode != 0:
        raise OSError("ICMP probe failed; check ping availability and OS permissions.")
    match = re.search(rb"time([=<])([\d.]+)\s*ms", result.stdout)
    return {
        "status": "reply",
        "reason": None,
        "rtt_ms": float(match[2]) if match else None,
        "rtt_upper_bound": bool(match and match[1] == b"<"),
    }


def main() -> None:
    try:
        host, timeout_text = sys.argv[1:]
        timeout = float(timeout_text)
        address = socket.getaddrinfo(host, None, socket.AF_INET, socket.SOCK_DGRAM)[0][
            4
        ][0]
        target = ipaddress.IPv4Address(address)
        if target.is_multicast or target.is_unspecified or address == "255.255.255.255":
            raise ValueError("The target must resolve to a unicast IPv4 address.")
        if sys.platform == "win32":
            result = windows_probe(address, timeout)
        elif sys.platform.startswith("linux"):
            result = linux_probe(address, timeout)
        else:
            raise OSError("ICMP monitoring currently supports Windows and Linux.")
        result["address"] = address
    except (OSError, ValueError, subprocess.TimeoutExpired) as error:
        result = {
            "status": "error",
            "reason": str(error),
            "rtt_ms": None,
            "address": None,
        }
    print(json.dumps(result, ensure_ascii=True), flush=True)


if __name__ == "__main__":
    main()
