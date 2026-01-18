#!/bin/bash

# OpenBroadcastNetwork Test Server Management Script
# Handles server lifecycle for testing video streaming

set -e

# Configuration
DEFAULT_VIDEO="Stargate SG1 S01E03.mp4"
DEFAULT_PORT=8080
LOG_DIR="logs"
PID_FILE="$LOG_DIR/server.pid"
CARGO_BIN="target/debug/relay-node"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Helper functions
log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

log_debug() {
    echo -e "${BLUE}[DEBUG]${NC} $1"
}

# Create logs directory if it doesn't exist
create_log_dir() {
    if [ ! -d "$LOG_DIR" ]; then
        mkdir -p "$LOG_DIR"
        log_info "Created logs directory: $LOG_DIR"
    fi
}

# Check if server is running
is_server_running() {
    if [ -f "$PID_FILE" ]; then
        local pid=$(cat "$PID_FILE")
        if kill -0 "$pid" 2>/dev/null; then
            return 0  # Running
        else
            # PID file exists but process is dead
            rm -f "$PID_FILE"
            return 1  # Not running
        fi
    fi
    
    # Also check for any relay-node web-viewer processes
    if pgrep -f "relay-node.*web-viewer" >/dev/null 2>&1; then
        return 0  # Running
    fi
    
    return 1  # Not running
}

# Stop the server
stop_server() {
    log_info "Stopping server..."
    
    # Kill by PID file first
    if [ -f "$PID_FILE" ]; then
        local pid=$(cat "$PID_FILE")
        if kill -0 "$pid" 2>/dev/null; then
            log_debug "Killing server process $pid"
            kill "$pid"
            sleep 2
            if kill -0 "$pid" 2>/dev/null; then
                log_warn "Process $pid still running, force killing..."
                kill -9 "$pid"
            fi
        fi
        rm -f "$PID_FILE"
    fi
    
    # Kill any remaining relay-node web-viewer processes
    local remaining_pids=$(pgrep -f "relay-node.*web-viewer" 2>/dev/null || true)
    if [ -n "$remaining_pids" ]; then
        log_debug "Killing remaining processes: $remaining_pids"
        pkill -f "relay-node.*web-viewer" || true
        sleep 1
        # Force kill if still running
        pkill -9 -f "relay-node.*web-viewer" 2>/dev/null || true
    fi
    
    log_info "Server stopped"
}

# Build the project
build_project() {
    log_info "Building project..."
    cargo build -p OpenBroadcastNetwork-core
    if [ $? -ne 0 ]; then
        log_error "Failed to build core library"
        exit 1
    fi
    
    cargo build -p OpenBroadcastNetwork-node
    if [ $? -ne 0 ]; then
        log_error "Failed to build node binary"
        exit 1
    fi
    
    log_info "Build completed successfully"
}

# Start the server
start_server() {
    local video_file="${1:-$DEFAULT_VIDEO}"
    local log_suffix="${2:-$(date +%Y%m%d_%H%M%S)}"
    local log_file="$LOG_DIR/server_$log_suffix.log"
    
    create_log_dir
    
    if is_server_running; then
        log_warn "Server is already running. Use 'stop' or 'restart' command."
        return 1
    fi
    
    log_info "Starting server with video: $video_file"
    log_info "Logs will be written to: $log_file"
    
    # Start server in background and capture PID
    nohup cargo run -p OpenBroadcastNetwork-node web-viewer --video "$video_file" > "$log_file" 2>&1 &
    local server_pid=$!
    
    # Save PID to file
    echo "$server_pid" > "$PID_FILE"
    
    log_info "Server started with PID: $server_pid"
    log_info "Waiting for server to initialize..."
    
    # Wait for server to start
    local timeout=30
    local count=0
    while [ $count -lt $timeout ]; do
        if curl -s "http://127.0.0.1:$DEFAULT_PORT/" >/dev/null 2>&1; then
            log_info "Server is ready at http://127.0.0.1:$DEFAULT_PORT/"
            return 0
        fi
        sleep 1
        count=$((count + 1))
    done
    
    log_error "Server failed to start within $timeout seconds"
    stop_server
    return 1
}

# Restart the server
restart_server() {
    local video_file="${1:-$DEFAULT_VIDEO}"
    local log_suffix="${2:-restart_$(date +%Y%m%d_%H%M%S)}"
    
    log_info "Restarting server..."
    stop_server
    sleep 2
    start_server "$video_file" "$log_suffix"
}

# Show server status
status() {
    if is_server_running; then
        local pid=$(cat "$PID_FILE" 2>/dev/null || echo "unknown")
        log_info "Server is running (PID: $pid)"
        log_info "Available at: http://127.0.0.1:$DEFAULT_PORT/"
        log_info "WebSocket endpoint: ws://127.0.0.1:$DEFAULT_PORT/stream"
        
        # Show recent log entries
        local latest_log=$(ls -t "$LOG_DIR"/server_*.log 2>/dev/null | head -n1)
        if [ -n "$latest_log" ]; then
            log_info "Recent logs from $latest_log:"
            tail -5 "$latest_log"
        fi
    else
        log_info "Server is not running"
    fi
}

# Follow logs
logs() {
    local log_file="${1}"
    
    if [ -z "$log_file" ]; then
        # Find the most recent log file
        log_file=$(ls -t "$LOG_DIR"/server_*.log 2>/dev/null | head -n1)
    fi
    
    if [ -z "$log_file" ]; then
        log_error "No log files found in $LOG_DIR/"
        return 1
    fi
    
    log_info "Following logs: $log_file"
    tail -f "$log_file"
}

# Quick test function
test_video() {
    local video_file="${1:-$DEFAULT_VIDEO}"
    
    log_info "Quick test with video: $video_file"
    build_project
    restart_server "$video_file" "test_$(date +%H%M%S)"
    
    log_info "Server started. Test URLs:"
    log_info "  Browser: http://127.0.0.1:$DEFAULT_PORT/"
    log_info "  Direct stream: ws://127.0.0.1:$DEFAULT_PORT/stream"
    log_info ""
    log_info "Use './scripts/test_server.sh logs' to follow server output"
    log_info "Use './scripts/test_server.sh stop' to stop the server"
}

# List available videos
list_videos() {
    log_info "Available video files:"
    for video in *.mp4; do
        if [ -f "$video" ]; then
            local size=$(du -h "$video" | cut -f1)
            echo "  - $video ($size)"
        fi
    done
}

# Show help
show_help() {
    echo "OpenBroadcastNetwork Test Server Management"
    echo ""
    echo "Usage: $0 <command> [options]"
    echo ""
    echo "Commands:"
    echo "  start [video_file]     Start the server with specified video (default: $DEFAULT_VIDEO)"
    echo "  stop                   Stop the server"
    echo "  restart [video_file]   Restart the server with specified video"
    echo "  status                 Show server status and recent logs"
    echo "  logs [log_file]        Follow server logs (latest if no file specified)"
    echo "  build                  Build the project"
    echo "  test [video_file]      Quick test: build + restart + show URLs"
    echo "  videos                 List available video files"
    echo "  help                   Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 test                           # Quick test with default video"
    echo "  $0 test \"sample_video.mp4\"      # Test with specific video"
    echo "  $0 start \"test_simple.mp4\"      # Start with small test video"
    echo "  $0 logs                           # Follow latest logs"
    echo "  $0 restart                        # Restart with same video"
}

# Main command handling
case "${1:-help}" in
    start)
        start_server "$2"
        ;;
    stop)
        stop_server
        ;;
    restart)
        restart_server "$2"
        ;;
    status)
        status
        ;;
    logs)
        logs "$2"
        ;;
    build)
        build_project
        ;;
    test)
        test_video "$2"
        ;;
    videos)
        list_videos
        ;;
    help|--help|-h)
        show_help
        ;;
    *)
        log_error "Unknown command: $1"
        echo ""
        show_help
        exit 1
        ;;
esac