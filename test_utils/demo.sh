#!/bin/bash

# OpenBroadcastNetwork Demo Script
# This script demonstrates the decentralized streaming network with interactive visualization

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
NODES_DIR="/tmp/obn_nodes"
NODE1_PORT=9001
NODE2_PORT=9002
NODE3_PORT=9003
RELAY_BINARY="target/release/relay-node"

# Cleanup function
cleanup() {
    echo -e "${YELLOW}Cleaning up...${NC}"
    pkill -f "relay-node" 2>/dev/null || true
    rm -rf "$NODES_DIR" 2>/dev/null || true
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

# Start a node
start_node() {
    local name=$1
    local port=$2
    local role=$3
    local bootstrap=$4
    
    echo -e "${CYAN}Starting $name on port $port...${NC}"
    
    local cmd="$RELAY_BINARY run --role $role --listen 127.0.0.1:$port"
    if [ ! -z "$bootstrap" ]; then
        cmd="$cmd --bootstrap $bootstrap --dht"
    else
        cmd="$cmd --dht"
    fi
    
    # Start node in background and redirect output to log file
    mkdir -p "$NODES_DIR"
    $cmd > "$NODES_DIR/$name.log" 2>&1 &
    local pid=$!
    echo $pid > "$NODES_DIR/$name.pid"
    
    # Wait a moment for startup
    sleep 2
    
    # Check if process is still running
    if kill -0 $pid 2>/dev/null; then
        echo -e "${GREEN}$name started successfully (PID: $pid)${NC}"
        return 0
    else
        echo -e "${RED}Failed to start $name${NC}"
        cat "$NODES_DIR/$name.log"
        return 1
    fi
}

# Stop all nodes
stop_nodes() {
    echo -e "${YELLOW}Stopping all nodes...${NC}"
    if [ -d "$NODES_DIR" ]; then
        for pidfile in "$NODES_DIR"/*.pid; do
            if [ -f "$pidfile" ]; then
                local pid=$(cat "$pidfile")
                local name=$(basename "$pidfile" .pid)
                if kill -0 $pid 2>/dev/null; then
                    kill $pid
                    echo -e "${GREEN}Stopped $name (PID: $pid)${NC}"
                fi
                rm -f "$pidfile"
            fi
        done
    fi
    pkill -f "relay-node" 2>/dev/null || true
}

# Start the network
start_network() {
    echo -e "${PURPLE}=== Starting OpenBroadcastNetwork Demo ===${NC}"
    
    # Start bootstrap node
    start_node "bootstrap" $NODE1_PORT "relay" ""
    
    # Wait a bit for bootstrap to be ready
    sleep 3
    
    # Start nodes that connect to bootstrap
    start_node "relay1" $NODE2_PORT "relay" "127.0.0.1:$NODE1_PORT"
    start_node "publisher" $NODE3_PORT "publisher" "127.0.0.1:$NODE1_PORT"
    
    echo -e "${GREEN}Network started with 3 nodes!${NC}"
    echo ""
}

# Show network status
show_status() {
    echo -e "${CYAN}=== Network Status ===${NC}"
    $RELAY_BINARY status --node "127.0.0.1:$NODE1_PORT" 2>/dev/null || echo -e "${RED}Could not connect to bootstrap node${NC}"
}

# List active streams
list_streams() {
    echo -e "${CYAN}=== Active Streams ===${NC}"
    $RELAY_BINARY list-streams --node "127.0.0.1:$NODE1_PORT" 2>/dev/null || echo -e "${RED}Could not connect to bootstrap node${NC}"
}

# Show network topology in text format
show_topology_text() {
    echo -e "${CYAN}=== Network Topology (Text) ===${NC}"
    $RELAY_BINARY visualize --node "127.0.0.1:$NODE1_PORT" --format text 2>/dev/null || echo -e "${RED}Could not connect to bootstrap node${NC}"
}

# Generate DOT graph
show_topology_dot() {
    echo -e "${CYAN}=== Network Topology (DOT Graph) ===${NC}"
    local output_file="/tmp/network_topology.dot"
    $RELAY_BINARY visualize --node "127.0.0.1:$NODE1_PORT" --format dot --output "$output_file" 2>/dev/null
    if [ -f "$output_file" ]; then
        echo -e "${GREEN}DOT graph saved to: $output_file${NC}"
        echo -e "${YELLOW}Content preview:${NC}"
        head -20 "$output_file"
        echo ""
        echo -e "${BLUE}To visualize: graphviz tools like 'dot -Tpng $output_file -o network.png'${NC}"
    else
        echo -e "${RED}Could not generate DOT graph${NC}"
    fi
}

# Generate JSON output
show_topology_json() {
    echo -e "${CYAN}=== Network Topology (JSON) ===${NC}"
    $RELAY_BINARY visualize --node "127.0.0.1:$NODE1_PORT" --format json 2>/dev/null || echo -e "${RED}Could not connect to bootstrap node${NC}"
}

# Show node logs
show_logs() {
    echo -e "${CYAN}=== Node Logs ===${NC}"
    if [ -d "$NODES_DIR" ]; then
        for logfile in "$NODES_DIR"/*.log; do
            if [ -f "$logfile" ]; then
                local name=$(basename "$logfile" .log)
                echo -e "${YELLOW}--- $name logs ---${NC}"
                tail -10 "$logfile"
                echo ""
            fi
        done
    else
        echo -e "${RED}No logs found${NC}"
    fi
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
    echo -e "${GREEN}7)${NC} Restart Network"
    echo -e "${GREEN}8)${NC} Stop Nodes"
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
                stop_nodes
                sleep 2
                start_network
                ;;
            8)
                stop_nodes
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
    echo "OpenBroadcastNetwork - Decentralized Live Streaming CDN Demo"
    echo -e "${NC}"
    
    # Build project
    build_project
    
    # Start network
    start_network
    
    # Wait for network to stabilize
    echo -e "${YELLOW}Waiting for network to stabilize...${NC}"
    sleep 5
    
    # Start interactive demo
    interactive_demo
}

# Check if running directly
if [ "${BASH_SOURCE[0]}" == "${0}" ]; then
    main "$@"
fi