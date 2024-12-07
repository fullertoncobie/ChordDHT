import socket
import threading
import time
import random
import hashlib
import pickle
import sys
from typing import Dict, Optional, Tuple, NamedTuple

M = 9  # FIXME: Test environment, normally = hashlib.sha1().digest_size * 8
NODES = 2**M
BUF_SZ = 4096
BACKLOG = 100
TEST_BASE = 43544

class NodeInfo(NamedTuple):
    """Represents a node's identity and location in the network"""
    id: int
    ip: str
    port: int

def generate_node_id(node_name: str) -> int:
    sha1_hash = hashlib.sha1(node_name.encode()).hexdigest()
    return int(sha1_hash, 16) % (2**M)

class ModRange(object):
    def __init__(self, start, stop, divisor):
        self.divisor = divisor
        self.start = start % self.divisor
        self.stop = stop % self.divisor
        if self.start < self.stop:
            self.intervals = (range(self.start, self.stop),)
        elif self.stop == 0:
            self.intervals = (range(self.start, self.divisor),)
        else:
            self.intervals = (range(self.start, self.divisor), range(0, self.stop))

    def __contains__(self, id):
        for interval in self.intervals:
            if id in interval:
                return True
        return False

class FingerEntry(object):
    def __init__(self, n: int, k: int, node: Optional[NodeInfo] = None):
        if not (0 <= n < NODES and 0 < k <= M):
            raise ValueError('invalid finger entry values')
        self.start = (n + 2**(k-1)) % NODES
        self.next_start = (n + 2**k) % NODES if k < M else n
        self.interval = ModRange(self.start, self.next_start, NODES)
        self.node = node

    def __contains__(self, id):
        return id in self.interval

class ChordNode(object):
    def __init__(self, node_name: str, existing_node_port: int = None):
        """
        Initialize node and join existing network if specified
        
        Args:
            node_name: String to generate node ID
            existing_node_port: port, or None to start new network
        """
        self.node_id = generate_node_id(node_name)
        self.ip = 'localhost'  
        
        # Create socket with system-assigned port
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.bind((self.ip, 0))
        self.port = self.socket.getsockname()[1]
        
        # Create NodeInfo for self
        self.node_info = NodeInfo(self.node_id, self.ip, self.port)
        
        print(f"Initialized node {self.node_id} on {self.ip}:{self.port}")
        
        self.finger = [None] + [FingerEntry(self.node_id, k) for k in range(1, M+1)]
        self._predecessor: Optional[NodeInfo] = None
        self.running = True
        
        # Start listening
        self.socket.listen(BACKLOG)
        self.rpc_thread = threading.Thread(target=self._listen)
        self.rpc_thread.daemon = True
        self.rpc_thread.start()

        if existing_node_port is not None:
            try:
                # Create proper address tuple for RPC call
                existing_node = self.call_rpc((self.ip, existing_node_port), 'get_node_info')
                if existing_node is None:
                    print(f"Node {self.node_id}: Could not connect to existing node at port {existing_node_port}")
                    self.stop()
                    raise Exception("Failed to connect to existing node")
                    
                print(f"Node {self.node_id}: Found node {existing_node.id} at {existing_node.ip}:{existing_node.port}")
                self.join_network(existing_node)
                self.start_maintenance()
            except Exception as e:
                print(f"Node {self.node_id}: Failed to join network: {e}")
                self.stop()
                raise
        else:
            print(f"Node {self.node_id}: Starting new network")
            self.start_new_network()

    def join_network(self, node: NodeInfo):
        """Join the network through the specified node"""
        print(f"Node {self.node_id}: Joining network through node {node.id}")
        self.init_finger_table(node)
        self.update_others()

    def init_finger_table(self, node: NodeInfo):
        """Initialize finger table using information from node"""
        print(f"Node {self.node_id}: Initializing finger table through node {node.id}")
        
        successor = self.call_rpc((node.ip, node.port), 'find_successor', self.finger[1].start)
        if successor and successor.id != self.node_id:
            self.finger[1].node = successor
            print(f"Node {self.node_id}: Found successor {successor.id}")

            # Get predecessor from successor
            pred = self.call_rpc((successor.ip, successor.port), 'predecessor')
            if pred and pred.id != self.node_id:
                self.predecessor = pred
            
            # Tell successor to update its predecessor to us
            self.call_rpc((successor.ip, successor.port), 'set_predecessor', self.node_info)
            
            # Initialize rest of finger table
            for i in range(1, M):
                if self.finger[i + 1].start in ModRange(self.node_id, self.finger[i].node.id, NODES):
                    self.finger[i + 1].node = self.finger[i].node
                else:
                    next_node = self.call_rpc(
                        (node.ip, node.port),
                        'find_successor',
                        self.finger[i + 1].start
                    )
                    if next_node and next_node.id != self.node_id:
                        self.finger[i + 1].node = next_node

    def call_rpc(self, addr: Tuple[str, int], method: str, *args, **kwargs) -> Optional[any]:
        """Make RPC call to node at specified address"""
        if addr is None:
            print(f"Warning: Attempted RPC call to None address from node {self.node_id}")
            return None
            
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect(addr)
            
            # Map property access to getter/setter methods
            if method == 'predecessor' and not args:
                method = 'get_predecessor'
            elif method == 'predecessor' and args:
                method = 'set_predecessor'
            elif method == 'successor' and not args:
                method = 'get_successor'
            elif method == 'successor' and args:
                method = 'set_successor'
            
            request = pickle.dumps({
                'method': method,
                'args': args,
                'kwargs': kwargs
            })
            
            s.sendall(request)
            s.shutdown(socket.SHUT_WR)
            
            data = b""
            while True:
                chunk = s.recv(BUF_SZ)
                if not chunk:
                    break
                data += chunk
            
            if data:
                response = pickle.loads(data)
                return response.get('result')
            return None
            
        except Exception as e:
            print(f"RPC call failed from node {self.node_id} to {addr}: {e}")
            return None
        finally:
            s.close()

    def _handle_client(self, client_socket: socket.socket):
        try:
            data = b""
            while True:
                chunk = client_socket.recv(BUF_SZ)
                if not chunk:
                    break
                data += chunk
            
            if data:
                request = pickle.loads(data)
                method = request['method']
                args = request.get('args', [])
                kwargs = request.get('kwargs', {})
                
                if method == 'get_node_info':
                    result = self.node_info
                elif method == 'get_predecessor':
                    result = self._predecessor
                elif method == 'set_predecessor':
                    self._predecessor = args[0]
                    result = None
                elif method == 'get_successor':
                    result = self.successor
                elif method == 'set_successor':
                    self.set_successor(args[0])
                    result = None
                else:
                    func = getattr(self, method)
                    result = func(*args, **kwargs)
                
                response = pickle.dumps({
                    'status': 'success',
                    'result': result
                })
                
                client_socket.sendall(response)
                
        except Exception as e:
            print(f"Error handling RPC on node {self.node_id}: {e}")
        finally:
            client_socket.close()

    def find_successor(self, id: int) -> Optional[NodeInfo]:
        """Find the successor node for an ID"""
        if id is None:
            return None
        np = self.find_predecessor(id)
        if np is None or np.id == self.node_id:
            return self.successor
        return self.call_rpc((np.ip, np.port), 'successor')

    def find_predecessor(self, id: int) -> Optional[NodeInfo]:
        """Find the predecessor node for an ID"""
        if id is None:
            return None
        n_prime = self.node_info
        while True:
            succ = self.call_rpc((n_prime.ip, n_prime.port), 'successor')
            if succ is None:
                return n_prime
            if id in ModRange((n_prime.id + 1) % NODES, succ.id, NODES):
                break
            n_next = self.call_rpc((n_prime.ip, n_prime.port), 'closest_preceding_finger', id)
            if n_next.id == n_prime.id:
                break
            n_prime = n_next
        return n_prime

    def stabilize(self):
        """Verify node's immediate successor and tell successor about node"""
        succ = self.successor
        if not succ:
            return
            
        succ = self.call_rpc((succ.ip, succ.port), 'predecessor')
        
        if (succ and succ.id != self.node_id and 
            succ.id in ModRange((self.node_id + 1) % NODES, succ.id, NODES)):
            print(f"Node {self.node_id}: Updating successor from {succ.id} to {succ.id}")
            self.successor = succ
            succ = succ
        
        # Notify successor about us
        if succ.id != self.node_id:  # Don't notify self
            self.call_rpc((succ.ip, succ.port), 'notify', self.node_info)

    def notify(self, n_prime: NodeInfo):
            """n_prime thinks it might be our predecessor"""
            if n_prime.id == self.node_id:
                return  # Don't set self as predecessor
                
            should_update = (
                self.predecessor is None or 
                n_prime.id in ModRange(self.predecessor.id, self.node_id, NODES)
            )
            
            if should_update:
                # Only log if actually changing to a different node
                if (self.predecessor is None or 
                    self.predecessor.id != n_prime.id):
                    old_pred = self.predecessor
                    print(f"Node {self.node_id}: Updated predecessor from "
                        f"{old_pred.id if old_pred else None} to {n_prime.id}")
                
                self._predecessor = n_prime

    def fix_fingers(self):
        """Refresh a random finger table entry"""
        i = random.randint(1, M)
        self.finger[i].node = self.find_successor(self.finger[i].start)

    def check_predecessor(self):
        """Check if predecessor has failed"""
        if self.predecessor:
            try:
                self.call_rpc((self.predecessor.ip, self.predecessor.port), 'ping')
            except Exception:
                self.predecessor = None

    def update_others(self):
        """Update all nodes whose finger tables should refer to node"""
        print(f"Node {self.node_id}: Updating finger tables of other nodes")
        for i in range(1, M + 1):
            # Find last node p whose i-th finger might be this node
            predecessor_id = (self.node_id - 2**(i-1) + NODES) % NODES
            p = self.find_predecessor(predecessor_id)
            
            if p and p.id != self.node_id:
                print(f"Node {self.node_id}: Updating finger table of node {p.id} at index {i}")
                self.call_rpc((p.ip, p.port), 'update_finger_table', self.node_info, i)

    def update_finger_table(self, s: NodeInfo, i: int):
        """Update the finger table with node s at position i"""
        # Check if s should be the i-th finger of this node
        if (self.finger[i].node and s.id != self.node_id and 
            s.id in ModRange(self.finger[i].start, self.finger[i].node.id, NODES)):
            
            print(f"Node {self.node_id}: Updating finger {i} from {self.finger[i].node.id} to {s.id}")
            old_node = self.finger[i].node
            self.finger[i].node = s
            
            # Propagate the update to the predecessor
            if self.predecessor and self.predecessor.id != s.id:
                self.call_rpc(
                    (self.predecessor.ip, self.predecessor.port),
                    'update_finger_table',
                    s,
                    i
                )

    def closest_preceding_finger(self, id: int) -> NodeInfo:
        """Return closest finger preceding id"""
        for i in range(M, 0, -1):
            if (self.finger[i].node and 
                self.finger[i].node.id in ModRange((self.node_id + 1) % NODES, id, NODES)):
                return self.finger[i].node
        return self.node_info

    def start_new_network(self):
        """Initialize as the first node in a new network"""
        print(f"Node {self.node_id}: Starting new network")
        
        # Set self as both successor and predecessor
        for i in range(1, M + 1):
            self.finger[i].node = self.node_info
        self.predecessor = self.node_info
                
        self.start_maintenance()

    def start_maintenance(self):
        """Start the periodic maintenance tasks"""
        def run_periodically(func, interval):
            while True:
                try:
                    func()
                except Exception as e:
                    print(f"Error in {func.__name__}: {e}")
                time.sleep(interval)
        
        tasks = [
            (self.stabilize, 1),
            (self.fix_fingers, 2),
            (self.check_predecessor, 3)
        ]
        
        for func, interval in tasks:
            thread = threading.Thread(target=run_periodically, args=(func, interval))
            thread.daemon = True
            thread.start()
        
    def store_data(self, key: str, value: bytes) -> bool:
        """Store a key-value pair on this node"""
        try:
            if not hasattr(self, '_data_store'):
                self._data_store = {}
                
            self._data_store[key] = value
            print(f"Node {self.node_id}: Stored data for key '{key}'")
            return True
            
        except Exception as e:
            print(f"Node {self.node_id}: Failed to store data for key '{key}': {e}")
            return False

    def get_data(self, key: str) -> Optional[str]:
        """Retrieve a value for a given key from this node"""
        try:
            if hasattr(self, '_data_store'):
                value = self._data_store.get(key)
                if value is not None:
                    print(f"Node {self.node_id}: Retrieved {key}:{value}")
                    return value
                print(f"Node {self.node_id}: Key {key} not found")
            else:
                print(f"Node {self.node_id}: No data store initialized")
            return None
        
        except Exception as e:
            print(f"Node {self.node_id}: Error retrieving {key}: {e}")
            return None

    def _listen(self):
        while self.running:
            try:
                client, _ = self.socket.accept()
                threading.Thread(target=self._handle_client, args=(client,)).start()
            except Exception as e:
                if self.running:
                    print(f"Error accepting connection on node {self.node_id}: {e}")

    def ping(self):
        """Simple method to check if node is alive"""
        return True

    def stop(self):
        """Cleanup method to stop the node"""
        self.running = False
        self.socket.close()

    def get_successor(self) -> Optional[NodeInfo]:
        """Get the node's successor (first finger entry)"""
        return self.finger[1].node

    def set_successor(self, node: NodeInfo):
        """Set the node's successor (first finger entry)"""
        self.finger[1].node = node

    def get_predecessor(self) -> Optional[NodeInfo]:
        """Get the node's predecessor"""
        return self._predecessor

    def set_predecessor(self, node: Optional[NodeInfo]):
        """Set the node's predecessor"""
        self._predecessor = node

    # Property decorators for easier access
    predecessor = property(get_predecessor, set_predecessor)
    successor = property(get_successor, set_successor)

def main():
    node_name = sys.argv[1]
    existing_port = int(sys.argv[2]) if len(sys.argv) > 2 else None

    try:
        node = ChordNode(node_name, existing_port)

        # Keep main thread alive
        while True:
            time.sleep(1)

    except KeyboardInterrupt:
        print("\nShutting down...")
        node.stop()

if __name__ == "__main__":
    main()