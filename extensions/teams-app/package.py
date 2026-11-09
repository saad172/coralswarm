#!/usr/bin/env python3
"""Build the Teams bot and meeting add-in package without modifying the manifest template."""
import argparse
import json
from pathlib import Path
import re
import struct
import uuid
import zipfile


def guid(value):
    try:
        return str(uuid.UUID(value))
    except ValueError as error:
        raise argparse.ArgumentTypeError("expected a GUID") from error


def version(value):
    if not re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+", value):
        raise argparse.ArgumentTypeError("expected major.minor.patch")
    return value


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--teams-app-id", required=True, type=guid)
    parser.add_argument("--aad-app-id", required=True, type=guid)
    parser.add_argument(
        "--bot-app-id", type=guid,
        help="Bot Framework application ID (defaults to --aad-app-id)",
    )
    parser.add_argument("--version", required=True, type=version)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    bot_app_id = args.bot_app_id or args.aad_app_id
    source = Path(__file__).resolve().parent
    rendered = (source / "manifest.json").read_text(encoding="utf-8").replace(
        "{{TEAMS_APP_ID}}", args.teams_app_id
    ).replace("{{AAD_APP_ID}}", args.aad_app_id).replace(
        "{{BOT_APP_ID}}", bot_app_id
    )
    if "{{" in rendered or "}}" in rendered:
        parser.error("manifest contains unresolved placeholders")
    manifest = json.loads(rendered)
    manifest["version"] = args.version
    if manifest.get("staticTabs"):
        parser.error("personal static tabs are not part of this package")
    if manifest["bots"][0]["botId"] != bot_app_id:
        parser.error("bot ID does not match the supplied bot application")
    if manifest["webApplicationInfo"]["id"] != args.aad_app_id:
        parser.error("web application ID does not match the supplied AAD application")
    # Teams SSO (authentication.getAuthToken) issues tokens whose audience is this
    # resource; it must name the same AAD application as the id.
    if manifest["webApplicationInfo"]["resource"] != f"api://app.coralswarm.example/{args.aad_app_id}":
        parser.error("web application resource must be api://app.coralswarm.example/<AAD app id>")
    assets = {}
    for name, size in (("color.png", 192), ("outline.png", 32)):
        data = (source / name).read_bytes()
        if data[:8] != b"\x89PNG\r\n\x1a\n" or struct.unpack(
            ">II", data[16:24]
        ) != (size, size):
            parser.error(f"{name} must be a {size}x{size} PNG")
        assets[name] = data
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(args.output, "w", zipfile.ZIP_DEFLATED) as package:
        package.writestr("manifest.json", json.dumps(manifest, indent=2) + "\n")
        for name, data in assets.items():
            package.writestr(name, data)
    print(f"Built {args.output} (app {args.teams_app_id}, version {args.version})")


if __name__ == "__main__":
    main()
