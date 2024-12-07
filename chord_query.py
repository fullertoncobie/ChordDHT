import socket
import pickle
import hashlib
import sys
from chord_node import NodeInfo
from typing import Optional, Tuple
from dataclasses import dataclass

M = 9

@dataclass
class QueryResult:
    """Represents the result of a query operation"""
    key: str
    value: Optional[str]
    node_id: Optional[int]
    node_port: Optional[int]
    success: bool
    error: Optional[str] = None

    def _print_results(self):
        print(f"\nQuery Result:")
        print(f"Key: {self.key}")
        print(f"Node ID: {self.node_id}")
        print(f"Node Port: {self.node_port}")       
        
        if self.success:
            try:
                unpickled_value = pickle.loads(self.value)
                print(f"Value: {unpickled_value}")
            except:
                print(f"Value: {self.value}")
        else:
            print(f"Success: {self.success}")
            print(f"Error: {self.error}")

class ChordQuery:
    def __init__(self, network_port: int):
        """Initialize connection to Chord network through an existing node"""
        self.network_port = network_port
        self.network_ip = 'localhost'
    
    def _find_responsible_node(self, key: str) -> Tuple[int, Optional[Tuple[str, int, int]]]:
        """Find the node responsible for a given key"""
        # Hash the key using same method as node IDs
        key_hash = int(hashlib.sha1(key.encode()).hexdigest(), 16) % (2**M)
        
        try:
            # Connect to the entry node
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((self.network_ip, self.network_port))
                
                # First get the node info of our entry point
                request = pickle.dumps({
                    'method': 'get_node_info',
                    'args': [],
                    'kwargs': {}
                })
                
                s.sendall(request)
                s.shutdown(socket.SHUT_WR)
                
                data = b""
                while True:
                    chunk = s.recv(4096)
                    if not chunk:
                        break
                    data += chunk
                    
                if not data:
                    print("No data received from get_node_info")
                    return key_hash, None
                    
                # Node to start our search from
                response = pickle.loads(data)
                entry_node = response.get('result')
                if not entry_node:
                    print("No result in response from get_node_info")
                    return key_hash, None
                
                print(f"Found entry node: {entry_node}")
                
                # Make a new connection for find_successor call
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s2:
                    s2.connect((entry_node.ip, entry_node.port))
                    
                    # Call find_successor to locate responsible node
                    request = pickle.dumps({
                        'method': 'find_successor',
                        'args': [key_hash],
                        'kwargs': {}
                    })
                    
                    s2.sendall(request)
                    s2.shutdown(socket.SHUT_WR)
                    
                    data = b""
                    while True:
                        chunk = s2.recv(4096)
                        if not chunk:
                            break
                        data += chunk
                    
                    if data:
                        response = pickle.loads(data)
                        result = response.get('result')
                        if result:
                            print(f"Found responsible node: {result}")
                            return key_hash, (result.ip, result.port, result.id)
                        else:
                            print("No result in response from find_successor")
                    else:
                        print("No data received from find_successor")
                        
        except Exception as e:
            print(f"Error finding node for key {key}: {e}")
            import traceback
            traceback.print_exc()
            
        return key_hash, None

    def _query_node(self, node_addr: Tuple[str, int], key: str) -> Optional[str]:
        """Query a specific node for a key's value"""
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect(node_addr)
                
                request = pickle.dumps({
                    'method': 'get_data',
                    'args': [key],
                    'kwargs': {}
                })
                
                s.sendall(request)
                s.shutdown(socket.SHUT_WR)
                
                data = b""
                while True:
                    chunk = s.recv(4096)
                    if not chunk:
                        break
                    data += chunk
                    
                if data:
                    response = pickle.loads(data)
                    return response.get('result')
                    
        except Exception as e:
            print(f"Error querying data for key {key} on node {node_addr}: {e}")
            
        return None

    def query(self, key: str) -> QueryResult:
        """Query the network for a value associated with the given key"""
        try:
            # Find the responsible node
            key_hash, node_info = self._find_responsible_node(key)
            
            if not node_info:
                return QueryResult(
                    key=key,
                    value=None,
                    node_id=None,
                    node_port=None,
                    success=False,
                    error="Could not find responsible node"
                )
            
            node_ip, node_port, node_id = node_info
            
            # Query the node for the value
            value = self._query_node((node_ip, node_port), key)
            
            if value is not None:
                return QueryResult(
                    key=key,
                    value=value,
                    node_id=node_id,
                    node_port=node_port,
                    success=True
                )
            else:
                return QueryResult(
                    key=key,
                    value=None,
                    node_id=node_id,
                    node_port=node_port,
                    success=False,
                    error="Key not found"
                )
                
        except Exception as e:
            return QueryResult(
                key=key,
                value=None,
                node_id=None,
                node_port=None,
                success=False,
                error=str(e)
            )

    def query_all_keys(self, keys: list[str]) -> dict[str, QueryResult]:
        """Query multiple keys at once"""
        results = {}
        for key in keys:
            results[key] = self.query(key)
        return results

def main():
    if len(sys.argv) != 3:
        print("Usage: python chord_query.py <network_port> <key>")
        sys.exit(1)

    try:
        network_port = int(sys.argv[1])
        desired_key = sys.argv[2]

        querier = ChordQuery(network_port)
        result = querier.query(desired_key) 
        result._print_results()

    except ValueError:
        print("Error: network_port must be an integer")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()