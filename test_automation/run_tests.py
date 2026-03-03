#!/usr/bin/env python3
"""
Test runner script for OpenBroadcastNetwork automation tests.

This script provides a convenient way to run the test suite with
common configurations.

Usage:
    # Run all tests
    python run_tests.py

    # Run browser tests only
    python run_tests.py --browser-only

    # Run network tests only
    python run_tests.py --network-only

    # Run in headed mode (visible browser)
    python run_tests.py --headed

    # Run specific test file
    python run_tests.py tests/test_web_viewer.py

    # Run with multiple browsers
    python run_tests.py --browser chromium --browser firefox
"""

import argparse
import subprocess
import sys
import os


def check_dependencies():
    """Check that required dependencies are installed."""
    missing = []

    try:
        import pytest
    except ImportError:
        missing.append("pytest")

    try:
        import playwright
    except ImportError:
        missing.append("playwright")

    try:
        import psutil
    except ImportError:
        missing.append("psutil")

    try:
        import websockets
    except ImportError:
        missing.append("websockets")

    try:
        import aiohttp
    except ImportError:
        missing.append("aiohttp")

    if missing:
        print("Missing dependencies:")
        for dep in missing:
            print(f"  - {dep}")
        print("\nInstall with: pip install -r requirements.txt")
        return False

    # Check Playwright browsers
    try:
        from playwright.sync_api import sync_playwright
        with sync_playwright() as p:
            # Just check if chromium is available
            pass
    except Exception as e:
        print(f"Playwright browsers not installed: {e}")
        print("Install with: playwright install")
        return False

    return True


def build_project():
    """Build the OpenBroadcastNetwork project."""
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    print("Building project...")
    result = subprocess.run(
        ["cargo", "build", "-p", "OpenBroadcastNetwork-node"],
        cwd=project_root,
        capture_output=True,
        text=True
    )

    if result.returncode != 0:
        print("Build failed:")
        print(result.stderr)
        return False

    print("Build successful")
    return True


def run_tests(args):
    """Run the test suite."""
    test_dir = os.path.dirname(os.path.abspath(__file__))

    # Build pytest command
    cmd = ["python", "-m", "pytest"]

    # Add verbosity
    if args.verbose:
        cmd.append("-v")
    else:
        cmd.append("-v")  # Always show test names

    # Add test selection
    if args.browser_only:
        cmd.extend(["-m", "browser"])
    elif args.network_only:
        cmd.extend(["-m", "network"])
    elif args.webrtc_only:
        cmd.extend(["-m", "webrtc"])

    # Exclude slow tests unless requested
    if not args.include_slow:
        cmd.extend(["-m", "not slow"])

    # Browser options
    if args.headed:
        cmd.append("--headed")

    for browser in args.browser:
        cmd.extend(["--browser", browser])

    # Video file
    if args.video:
        cmd.extend(["--video-file", args.video])

    # Port configuration
    cmd.extend(["--base-port", str(args.base_port)])

    # Test timeout
    if args.timeout:
        cmd.extend(["--timeout", str(args.timeout)])

    # HTML report
    if args.html_report:
        cmd.extend(["--html", args.html_report, "--self-contained-html"])

    # Add specific test files/directories
    if args.tests:
        cmd.extend(args.tests)
    else:
        cmd.append("tests/")

    # Run from test directory
    print(f"Running: {' '.join(cmd)}")
    print()

    result = subprocess.run(cmd, cwd=test_dir)
    return result.returncode


def main():
    parser = argparse.ArgumentParser(
        description="Run OpenBroadcastNetwork automation tests"
    )

    # Test selection
    parser.add_argument(
        "--browser-only",
        action="store_true",
        help="Run only browser automation tests"
    )
    parser.add_argument(
        "--network-only",
        action="store_true",
        help="Run only network integration tests"
    )
    parser.add_argument(
        "--webrtc-only",
        action="store_true",
        help="Run only WebRTC tests"
    )
    parser.add_argument(
        "--include-slow",
        action="store_true",
        help="Include slow tests"
    )

    # Browser options
    parser.add_argument(
        "--headed",
        action="store_true",
        help="Run browser tests in headed mode"
    )
    parser.add_argument(
        "--browser",
        action="append",
        default=[],
        help="Browser(s) to test (chromium, firefox, webkit)"
    )

    # Configuration
    parser.add_argument(
        "--video",
        default="test_simple.mp4",
        help="Video file for streaming tests"
    )
    parser.add_argument(
        "--base-port",
        type=int,
        default=9100,
        help="Base port for test servers"
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=300,
        help="Test timeout in seconds"
    )

    # Output
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Verbose output"
    )
    parser.add_argument(
        "--html-report",
        help="Generate HTML report at specified path"
    )

    # Build
    parser.add_argument(
        "--skip-build",
        action="store_true",
        help="Skip building the project"
    )
    parser.add_argument(
        "--check-deps",
        action="store_true",
        help="Check dependencies and exit"
    )

    # Positional arguments for test files
    parser.add_argument(
        "tests",
        nargs="*",
        help="Specific test files or directories to run"
    )

    args = parser.parse_args()

    # Check dependencies
    print("Checking dependencies...")
    if not check_dependencies():
        return 1

    if args.check_deps:
        print("All dependencies satisfied")
        return 0

    # Build project
    if not args.skip_build:
        if not build_project():
            return 1

    # Set default browser if none specified
    if not args.browser:
        args.browser = ["chromium"]

    # Run tests
    return run_tests(args)


if __name__ == "__main__":
    sys.exit(main())
