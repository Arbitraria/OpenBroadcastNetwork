#!/usr/bin/env python3
"""
Simple connectivity test for OpenBroadcastNetwork (no external dependencies)
Tests HTTP connectivity and analyzes server logs
"""

import urllib.request
import urllib.error
import json
import sys
import os
from datetime import datetime

class SimpleConnectivityTester:
    def __init__(self, base_url="http://127.0.0.1:8080"):
        self.base_url = base_url
        self.results = {}
        
    def test_http_connectivity(self):
        """Test basic HTTP connectivity"""
        print("🔌 Testing HTTP Connectivity")
        print("=" * 40)
        
        try:
            response = urllib.request.urlopen(f"{self.base_url}/", timeout=5)
            status_code = response.getcode()
            
            if status_code == 200:
                print("✅ HTTP endpoint responding (200 OK)")
                self.results['http_status'] = 'success'
                return True
            else:
                print(f"⚠️ HTTP endpoint returned status: {status_code}")
                self.results['http_status'] = f'status_{status_code}'
                return False
                
        except urllib.error.URLError as e:
            print(f"❌ HTTP connection failed: {e}")
            self.results['http_status'] = 'failed'
            return False
        except Exception as e:
            print(f"❌ Unexpected error: {e}")
            self.results['http_status'] = 'error'
            return False
            
    def analyze_server_logs(self):
        """Analyze server startup logs for codec information"""
        print(f"\n📋 Analyzing Server Logs")
        print("=" * 30)
        
        log_file = "server_startup.log"
        if not os.path.exists(log_file):
            print(f"⚠️ Log file '{log_file}' not found")
            return False
            
        try:
            with open(log_file, 'r') as f:
                lines = f.readlines()
                
            # Look for key information
            server_started = False
            audio_codec_detected = None
            video_codec_detected = None
            esds_modifications = 0
            video_file_loaded = False
            
            for line in lines:
                if "Starting web server on http://127.0.0.1:8080" in line:
                    server_started = True
                    
                if "Detected audio codec:" in line:
                    # Extract codec info
                    parts = line.split("Detected audio codec:")
                    if len(parts) > 1:
                        audio_codec_detected = parts[1].strip()
                        
                if "Detected video codec:" in line:
                    # Extract codec info  
                    parts = line.split("Detected video codec:")
                    if len(parts) > 1:
                        video_codec_detected = parts[1].strip()
                        
                if "Successfully applied" in line and "ESDS modifications" in line:
                    # Count ESDS modifications
                    if "1 ESDS modifications" in line:
                        esds_modifications = 1
                        
                if "Successfully loaded video file for streaming" in line:
                    video_file_loaded = True
            
            # Report findings
            print(f"🚀 Server started: {'✅ Yes' if server_started else '❌ No'}")
            print(f"📹 Video file loaded: {'✅ Yes' if video_file_loaded else '❌ No'}")
            print(f"🔧 ESDS modifications: {esds_modifications}")
            
            if audio_codec_detected:
                print(f"🎵 Audio codec: {audio_codec_detected}")
                self.results['audio_codec'] = audio_codec_detected
            else:
                print(f"🎵 Audio codec: ❓ Not detected")
                
            if video_codec_detected:
                print(f"📺 Video codec: {video_codec_detected}")
                self.results['video_codec'] = video_codec_detected
            else:
                print(f"📺 Video codec: ❓ Not detected")
            
            # Store results
            self.results.update({
                'server_started': server_started,
                'video_loaded': video_file_loaded,
                'esds_modifications': esds_modifications
            })
            
            return server_started and video_file_loaded
            
        except Exception as e:
            print(f"❌ Error reading log file: {e}")
            return False
            
    def check_process_status(self):
        """Check if relay-node process is running"""
        print(f"\n🔍 Checking Process Status")
        print("=" * 30)
        
        try:
            import subprocess
            result = subprocess.run(['ps', 'aux'], capture_output=True, text=True)
            
            relay_processes = []
            for line in result.stdout.split('\n'):
                if 'relay-node' in line and 'grep' not in line:
                    relay_processes.append(line.strip())
                    
            if relay_processes:
                print(f"✅ Found {len(relay_processes)} relay-node process(es)")
                for i, process in enumerate(relay_processes):
                    # Extract PID and memory info
                    parts = process.split()
                    if len(parts) >= 11:
                        pid = parts[1]
                        memory = parts[5]
                        print(f"   Process {i+1}: PID {pid}, Memory {memory}KB")
                        
                self.results['process_count'] = len(relay_processes)
                return True
            else:
                print("❌ No relay-node processes found")
                self.results['process_count'] = 0
                return False
                
        except Exception as e:
            print(f"❌ Error checking processes: {e}")
            return False
            
    def test_port_binding(self):
        """Check if port 8080 is bound"""
        print(f"\n🔌 Checking Port Binding")
        print("=" * 25)
        
        try:
            import subprocess
            result = subprocess.run(['netstat', '-tuln'], capture_output=True, text=True)
            
            port_8080_found = False
            for line in result.stdout.split('\n'):
                if ':8080' in line and 'LISTEN' in line:
                    print(f"✅ Port 8080 is bound: {line.strip()}")
                    port_8080_found = True
                    break
                    
            if not port_8080_found:
                print("❌ Port 8080 is not bound")
                
            self.results['port_bound'] = port_8080_found
            return port_8080_found
            
        except Exception as e:
            print(f"❌ Error checking port binding: {e}")
            return False
            
    def analyze_chrome_compatibility(self):
        """Analyze Chrome compatibility based on detected codecs"""
        print(f"\n🌐 Chrome Compatibility Analysis")
        print("=" * 40)
        
        audio_codec = self.results.get('audio_codec', '')
        
        # Check for Chrome-compatible audio codec formats
        chrome_compatible_codecs = [
            'mp4a.40.2',    # Standard AAC-LC
            'mp4a.40.02',   # Zero-padded AAC-LC  
            'mp4a.40.5',    # HE-AAC
            'mp4a.67',      # Alternative AAC-LC
        ]
        
        compatible_found = False
        for codec in chrome_compatible_codecs:
            if codec in audio_codec:
                print(f"✅ Chrome-compatible codec detected: {codec}")
                compatible_found = True
                break
                
        if not compatible_found:
            print(f"⚠️ Current audio codec may not be Chrome-compatible")
            print(f"   Detected: {audio_codec}")
            print(f"   Chrome prefers: mp4a.40.2 or mp4a.40.02")
            
        # Check ESDS modification status
        esds_mods = self.results.get('esds_modifications', 0)
        if esds_mods > 0:
            print(f"✅ ESDS modifications applied ({esds_mods})")
            print(f"   This should resolve 'object type 0x40' errors")
        else:
            print(f"⚠️ No ESDS modifications detected")
            print(f"   May encounter 'object type 0x40 does not match' errors")
            
        self.results['chrome_compatible'] = compatible_found and esds_mods > 0
        return compatible_found and esds_mods > 0
        
    def generate_report(self):
        """Generate comprehensive test report"""
        print(f"\n📄 Connectivity Test Report")
        print("=" * 50)
        
        print(f"🕐 Test completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print()
        
        # Summary of all tests
        tests = [
            ("HTTP Connectivity", self.results.get('http_status') == 'success'),
            ("Process Running", self.results.get('process_count', 0) > 0),
            ("Port 8080 Bound", self.results.get('port_bound', False)),
            ("Server Started", self.results.get('server_started', False)),
            ("Video File Loaded", self.results.get('video_loaded', False)),
            ("ESDS Modified", self.results.get('esds_modifications', 0) > 0),
            ("Chrome Compatible", self.results.get('chrome_compatible', False))
        ]
        
        all_passed = True
        for test_name, passed in tests:
            status = "✅ PASS" if passed else "❌ FAIL"
            print(f"  {test_name:<20} {status}")
            if not passed:
                all_passed = False
                
        print()
        
        if all_passed:
            print("🎉 SUCCESS: Server is ready for browser testing!")
            print("   → All connectivity checks passed")
            print("   → Chrome-compatible configuration detected")
            print("   → Ready to test at http://127.0.0.1:8080/")
        else:
            print("⚠️ ISSUES DETECTED: Server needs attention")
            failed_tests = [name for name, passed in tests if not passed]
            print(f"   → Failed tests: {', '.join(failed_tests)}")
            
        return all_passed

def main():
    """Main test execution"""
    print("🧪 OpenBroadcastNetwork Simple Connectivity Test")
    print("=" * 60)
    print()
    
    tester = SimpleConnectivityTester()
    
    # Run all tests
    http_ok = tester.test_http_connectivity()
    process_ok = tester.check_process_status()
    port_ok = tester.test_port_binding()
    logs_ok = tester.analyze_server_logs()
    chrome_ok = tester.analyze_chrome_compatibility()
    
    # Generate final report
    overall_success = tester.generate_report()
    
    if overall_success:
        print("\n🎯 NEXT STEP: Test in Chrome browser at http://127.0.0.1:8080/")
    else:
        print("\n🔧 NEXT STEP: Address failed tests before browser testing")
    
    sys.exit(0 if overall_success else 1)

if __name__ == "__main__":
    main()