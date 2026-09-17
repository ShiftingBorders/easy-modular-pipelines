"""Deliver desktop/sound notifications on the dashboard machine, never in the browser."""

import asyncio
import json
import os
import shutil
import subprocess
from pathlib import Path


async def deliver(title: str, message: str, channels: dict) -> dict:
    if not channels.get("desktop") and not channels.get("sound"):
        return {"status": "disabled"}
    process = None
    try:
        if os.name == "nt":
            executable = (
                Path(os.environ["SystemRoot"])
                / "System32/WindowsPowerShell/v1.0/powershell.exe"
            )
            script = """
                $ErrorActionPreference = 'Stop'
                $data = [Console]::In.ReadToEnd() | ConvertFrom-Json
                if ($data.sound) { [System.Media.SystemSounds]::Exclamation.Play() }
                if ($data.desktop) {
                    Add-Type -AssemblyName System.Windows.Forms
                    Add-Type -AssemblyName System.Drawing
                    $icon = New-Object System.Windows.Forms.NotifyIcon
                    try {
                        $icon.Icon = [System.Drawing.SystemIcons]::Warning
                        $icon.Visible = $true
                        $icon.ShowBalloonTip(5000, $data.title, $data.message, [System.Windows.Forms.ToolTipIcon]::Warning)
                        $deadline = [DateTime]::UtcNow.AddSeconds(6)
                        while ([DateTime]::UtcNow -lt $deadline) {
                            [System.Windows.Forms.Application]::DoEvents()
                            Start-Sleep -Milliseconds 100
                        }
                    } finally { $icon.Dispose() }
                }
            """
            process = await asyncio.create_subprocess_exec(
                str(executable),
                "-NoProfile",
                "-NonInteractive",
                "-Command",
                script,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.PIPE,
                creationflags=subprocess.CREATE_NO_WINDOW,
            )
            payload = json.dumps(
                {"title": title[:100], "message": message[:1000], **channels},
                ensure_ascii=True,
            ).encode()
            _, error = await asyncio.wait_for(process.communicate(payload), 12)
            if process.returncode:
                raise OSError(
                    error.decode(errors="replace")[-1500:]
                    or "Windows rejected the notification."
                )
        else:
            commands = []
            if channels.get("desktop"):
                executable = shutil.which("notify-send")
                if executable is None:
                    raise OSError("notify-send is unavailable on the dashboard host.")
                commands.append(
                    [
                        executable,
                        "--app-name=EMP Dashboard",
                        "--",
                        title[:100],
                        message[:1000],
                    ]
                )
            if channels.get("sound"):
                executable = shutil.which("canberra-gtk-play")
                if executable is None:
                    raise OSError(
                        "canberra-gtk-play is unavailable on the dashboard host."
                    )
                commands.append([executable, "--id=dialog-warning"])
            for arguments in commands:
                process = await asyncio.create_subprocess_exec(
                    *arguments,
                    stdout=asyncio.subprocess.DEVNULL,
                    stderr=asyncio.subprocess.PIPE,
                )
                _, error = await asyncio.wait_for(process.communicate(), 8)
                if process.returncode:
                    raise OSError(
                        error.decode(errors="replace")[-1500:]
                        or "Notification delivery failed."
                    )
        return {"status": "submitted", "source": "dashboard_host"}
    except (OSError, TimeoutError) as error:
        return {
            "status": "failed",
            "message": str(error) or "Notification deadline exceeded.",
        }
    finally:
        if process is not None and process.returncode is None:
            try:
                process.kill()
            except ProcessLookupError:
                pass
            await process.wait()
