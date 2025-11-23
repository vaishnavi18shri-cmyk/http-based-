# http-based-
"""Socket Master - Remote Process Execution"""
import socket
import json
import threading
import time
import sys
from datetime import datetime

class Master:
    def __init__(self, host='0.0.0.0', port=5555, timeout=30):
        self.host = host
        self.port = port
        self.timeout = timeout
        self.workers = {}
        self.lock = threading.Lock()
        self.running = False
        self.allowed_commands = ['echo', 'dir', 'ls', 'pwd', 'whoami', 'hostname', 
                                'python', 'ping', 'ipconfig', 'ifconfig', 'date']
    
    def start(self):
        self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server.bind((self.host, self.port))
        self.server.listen(5)
        self.running = True
        
        print(f"Master started on {self.host}:{self.port}")
        print(f"Timeout: {self.timeout}s")
        
        threading.Thread(target=self.accept_workers, daemon=True).start()
        threading.Thread(target=self.monitor_heartbeats, daemon=True).start()
        self.command_loop()
    
    def accept_workers(self):
        while self.running:
            try:
                self.server.settimeout(1.0)
                client, addr = self.server.accept()
                threading.Thread(target=self.handle_worker, args=(client, addr), daemon=True).start()
            except socket.timeout:
                continue
            except:
                break
    
    def handle_worker(self, client, addr):
        worker_id = None
        try:
            client.settimeout(10.0)
            data = self.recv_json(client)
            if data and data.get('type') == 'register':
                worker_id = data['worker_id']
                with self.lock:
                    self.workers[worker_id] = {
                        'socket': client,
                        'address': addr,
                        'last_heartbeat': time.time(),
                        'status': 'online'
                    }
                print(f"Worker registered: {worker_id} ({addr[0]})")
                self.send_json(client, {'type': 'ack', 'timeout': self.timeout})
                
                while self.running:
                    client.settimeout(5.0)
                    data = self.recv_json(client)
                    if data is None:
                        continue
                    if data.get('type') == 'heartbeat':
                        with self.lock:
                            if worker_id in self.workers:
                                self.workers[worker_id]['last_heartbeat'] = time.time()
                    elif data.get('type') == 'result':
                        self.display_result(worker_id, data)
        except Exception as e:
            if worker_id:
                print(f"Worker {worker_id} error: {e}")
        finally:
            if worker_id:
                with self.lock:
                    if worker_id in self.workers:
                        self.workers[worker_id]['status'] = 'offline'
                print(f"Worker disconnected: {worker_id}")
            try:
                client.close()
            except:
                pass
    
    def monitor_heartbeats(self):
        while self.running:
            time.sleep(10)
            with self.lock:
                for wid, info in self.workers.items():
                    if time.time() - info['last_heartbeat'] > 30 and info['status'] == 'online':
                        info['status'] = 'offline'
                        print(f"Worker {wid} offline (no heartbeat)")
    
    def command_loop(self):
        print("\nCommands: list, send <worker_id> <cmd>, broadcast <cmd>, exit\n")
        while self.running:
            try:
                cmd = input(">>> ").strip()
                if not cmd:
                    continue
                
                parts = cmd.split(maxsplit=1)
                action = parts[0].lower()
                
                if action == 'exit':
                    self.shutdown()
                    break
                elif action == 'list':
                    self.list_workers()
                elif action == 'send' and len(parts) > 1:
                    args = parts[1].split(maxsplit=1)
                    if len(args) == 2:
                        self.send_command(args[0], args[1])
                elif action == 'broadcast' and len(parts) > 1:
                    self.broadcast_command(parts[1])
            except (KeyboardInterrupt, EOFError):
                break
    
    def list_workers(self):
        with self.lock:
            if not self.workers:
                print("No workers registered")
                return
            print(f"\n{'ID':<20} {'Address':<20} {'Status'}")
            print("-" * 60)
            for wid, info in self.workers.items():
                addr = f"{info['address'][0]}:{info['address'][1]}"
                status = "online" if info['status'] == 'online' else "offline"
                print(f"{wid:<20} {addr:<20} {status}")
            print()
    
    def send_command(self, worker_id, command):
        if command.split()[0] not in self.allowed_commands:
            print(f"Command not allowed: {command.split()[0]}")
            return
        
        with self.lock:
            if worker_id not in self.workers:
                print(f"Worker not found: {worker_id}")
                return
            worker = self.workers[worker_id]
            if worker['status'] != 'online':
                print(f"Worker offline: {worker_id}")
                return
            
            try:
                msg = {'type': 'command', 'command': command, 'timeout': self.timeout}
                self.send_json(worker['socket'], msg)
                print(f"Command sent to {worker_id}")
            except:
                print(f"Failed to send command")
    
    def broadcast_command(self, command):
        if command.split()[0] not in self.allowed_commands:
            print(f"Command not allowed: {command.split()[0]}")
            return
        
        with self.lock:
            online = [w for w in self.workers.values() if w['status'] == 'online']
            if not online:
                print("No online workers")
                return
            
            print(f"Broadcasting to {len(online)} workers")
            for worker in online:
                try:
                    msg = {'type': 'command', 'command': command, 'timeout': self.timeout}
                    self.send_json(worker['socket'], msg)
                except:
                    pass
    
    def display_result(self, worker_id, data):
        print(f"\n--- Result from {worker_id} ---")
        print(f"Command: {data.get('command')}")
        print(f"Status: {'Success' if data.get('success') else 'Failed'}")
        print(f"Duration: {data.get('duration', 0):.2f}s")
        if data.get('success'):
            print(data.get('output', ''))
        else:
            print(f"Error: {data.get('error')}")
        print()
    
    def send_json(self, sock, data):
        msg = json.dumps(data).encode('utf-8')
        sock.sendall(len(msg).to_bytes(4, 'big') + msg)
    
    def recv_json(self, sock):
        try:
            length_bytes = self.recv_exact(sock, 4)
            if not length_bytes:
                return None
            length = int.from_bytes(length_bytes, 'big')
            data = self.recv_exact(sock, length)
            return json.loads(data.decode('utf-8')) if data else None
        except:
            return None
    
    def recv_exact(self, sock, n):
        data = b''
        while len(data) < n:
            chunk = sock.recv(n - len(data))
            if not chunk:
                return None
            data += chunk
        return data
    
    def shutdown(self):
        print("Shutting down...")
        self.running = False
        with self.lock:
            for worker in self.workers.values():
                try:
                    worker['socket'].close()
                except:
                    pass
        try:
            self.server.close()
        except:
            pass
        sys.exit(0)

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--host', default='0.0.0.0')
    parser.add_argument('--port', type=int, default=5555)
    parser.add_argument('--timeout', type=int, default=30)
    args = parser.parse_args()
    
    Master(args.host, args.port, args.timeout).start()
