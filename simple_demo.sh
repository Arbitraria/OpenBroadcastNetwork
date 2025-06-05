#!/bin/bash

# Simple OpenBroadcastNetwork Demo Script
# Demonstrates CLI visualization features with standalone nodes

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Configuration
DEMO_NODE_PORT=9000
RELAY_BINARY="target/release/relay-node"
NODE_PID=""

# Cleanup function
cleanup() {
    echo -e "${YELLOW}Cleaning up...${NC}"
    if [ ! -z "$NODE_PID" ] && kill -0 $NODE_PID 2>/dev/null; then
        kill $NODE_PID
        echo -e "${GREEN}Stopped demo node (PID: $NODE_PID)${NC}"
    fi
    pkill -f "relay-node" 2>/dev/null || true
    echo -e "${GREEN}Cleanup complete.${NC}"
}

# Trap cleanup on script exit
trap cleanup EXIT

# Build the project
build_project() {
    echo -e "${BLUE}Building OpenBroadcastNetwork...${NC}"
    cargo build --release --quiet
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}Build successful!${NC}"
    else
        echo -e "${RED}Build failed!${NC}"
        exit 1
    fi
}

# Start demo node
start_demo_node() {
    echo -e "${CYAN}Starting demo relay node on port $DEMO_NODE_PORT...${NC}"
    
    # Start standalone node (no bootstrap required)
    $RELAY_BINARY run --role relay --listen 127.0.0.1:$DEMO_NODE_PORT > /tmp/demo_node.log 2>&1 &
    NODE_PID=$!
    
    # Wait for startup
    sleep 3
    
    # Check if process is still running
    if kill -0 $NODE_PID 2>/dev/null; then
        echo -e "${GREEN}Demo node started successfully (PID: $NODE_PID)${NC}"
        return 0
    else
        echo -e "${RED}Failed to start demo node${NC}"
        cat /tmp/demo_node.log
        return 1
    fi
}

# Show network status
show_status() {
    echo -e "${CYAN}=== Network Status ===${NC}"
    $RELAY_BINARY status --node "127.0.0.1:$DEMO_NODE_PORT" 2>/dev/null || echo -e "${RED}Could not connect to demo node${NC}"
}

# List active streams
list_streams() {
    echo -e "${CYAN}=== Active Streams ===${NC}"
    $RELAY_BINARY list-streams --node "127.0.0.1:$DEMO_NODE_PORT" 2>/dev/null || echo -e "${RED}Could not connect to demo node${NC}"
}

# Show network topology in text format
show_topology_text() {
    echo -e "${CYAN}=== Network Topology (Text) ===${NC}"
    $RELAY_BINARY visualize --node "127.0.0.1:$DEMO_NODE_PORT" --format text 2>/dev/null || echo -e "${RED}Could not connect to demo node${NC}"
}

# Generate DOT graph
show_topology_dot() {
    echo -e "${CYAN}=== Network Topology (DOT Graph) ===${NC}"
    local output_file="/tmp/network_topology.dot"
    $RELAY_BINARY visualize --node "127.0.0.1:$DEMO_NODE_PORT" --format dot --output "$output_file" 2>/dev/null
    if [ -f "$output_file" ]; then
        echo -e "${GREEN}DOT graph saved to: $output_file${NC}"
        echo -e "${YELLOW}Content preview:${NC}"
        cat "$output_file"
        echo ""
        echo -e "${BLUE}To generate PNG: dot -Tpng $output_file -o network.png${NC}"
    else
        echo -e "${RED}Could not generate DOT graph${NC}"
    fi
}

# Generate JSON output
show_topology_json() {
    echo -e "${CYAN}=== Network Topology (JSON) ===${NC}"
    $RELAY_BINARY visualize --node "127.0.0.1:$DEMO_NODE_PORT" --format json 2>/dev/null || echo -e "${RED}Could not connect to demo node${NC}"
}

# Show node logs
show_logs() {
    echo -e "${CYAN}=== Demo Node Logs ===${NC}"
    if [ -f "/tmp/demo_node.log" ]; then
        tail -20 /tmp/demo_node.log
    else
        echo -e "${RED}No logs found${NC}"
    fi
}

# Test CLI help
show_help() {
    echo -e "${CYAN}=== CLI Help ===${NC}"
    $RELAY_BINARY --help
}

# Interactive menu
show_menu() {
    echo ""
    echo -e "${PURPLE}=== OpenBroadcastNetwork Demo Menu ===${NC}"
    echo -e "${GREEN}1)${NC} Show Network Status"
    echo -e "${GREEN}2)${NC} List Active Streams"  
    echo -e "${GREEN}3)${NC} Show Network Topology (Text)"
    echo -e "${GREEN}4)${NC} Generate DOT Graph"
    echo -e "${GREEN}5)${NC} Show JSON Topology"
    echo -e "${GREEN}6)${NC} View Node Logs"
    echo -e "${GREEN}7)${NC} Show CLI Help"
    echo -e "${GREEN}8)${NC} Restart Demo Node"
    echo -e "${GREEN}9)${NC} Exit"
    echo ""
    echo -e "${CYAN}Choose an option (1-9): ${NC}"
}

# Main interactive loop
interactive_demo() {
    while true; do
        show_menu
        read -r choice
        
        case $choice in
            1)
                show_status
                ;;
            2)
                list_streams
                ;;
            3)
                show_topology_text
                ;;
            4)
                show_topology_dot
                ;;
            5)
                show_topology_json
                ;;
            6)
                show_logs
                ;;
            7)
                show_help
                ;;
            8)
                if [ ! -z "$NODE_PID" ] && kill -0 $NODE_PID 2>/dev/null; then
                    kill $NODE_PID
                fi
                sleep 2
                start_demo_node
                ;;
            9)
                echo -e "${GREEN}Exiting demo...${NC}"
                exit 0
                ;;
            *)
                echo -e "${RED}Invalid option. Please choose 1-9.${NC}"
                ;;
        esac
        
        echo ""
        echo -e "${YELLOW}Press Enter to continue...${NC}"
        read -r
    done
}

# Main execution
main() {
    echo -e "${PURPLE}"
    echo "██████╗ ██████╗ ███╗   ██╗    ██████╗ ███████╗███╗   ███╗ ██████╗ "
    echo "██╔═══██╗██╔══██╗████╗  ██║    ██╔══██╗██╔════╝████╗ ████║██╔═══██╗"
    echo "██║   ██║██████╔╝██╔██╗ ██║    ██║  ██║█████╗  ██╔████╔██║██║   ██║"
    echo "██║   ██║██╔══██╗██║╚██╗██║    ██║  ██║██╔══╝  ██║╚██╔╝██║██║   ██║"
    echo "╚██████╔╝██████╔╝██║ ╚████║    ██████╔╝███████╗██║ ╚═╝ ██║╚██████╔╝"
    echo " ╚═════╝ ╚═════╝ ╚═╝  ╚═══╝    ╚═════╝ ╚══════╝╚═╝     ╚═╝ ╚═════╝ "
    echo ""
    echo "OpenBroadcastNetwork - Simple CLI Demo"
    echo -e "${NC}"
    
    # Build project
    build_project
    
    # Start demo node
    start_demo_node
    
    # Wait for node to stabilize
    echo -e "${YELLOW}Waiting for node to stabilize...${NC}"
    sleep 3
    
    echo -e "${GREEN}Demo ready! The node is running in standalone mode.${NC}"
    echo -e "${BLUE}This demonstrates the CLI visualization features.${NC}"
    
    # Start interactive demo
    interactive_demo
}

# Check if running directly
if [ "${BASH_SOURCE[0]}" == "${0}" ]; then
    main "$@"
fi