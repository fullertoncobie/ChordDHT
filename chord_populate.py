import socket
import pickle
import hashlib
import csv
import sys
import time
from chord_node import NodeInfo
from typing import Dict, Optional

M = 9

class ChordPopulate:
    def __init__(self, network_port: int):
        """Initialize connection to Chord network through an existing node"""
        self.network_port = network_port
        self.network_ip = 'localhost'
        self.port_to_socket = {} 

    def _get_socket(self, port: int) -> socket.socket:
        """Get or create a socket for the given port"""
        if port not in self.port_to_socket:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.connect((self.network_ip, port))
            self.port_to_socket[port] = sock
        return self.port_to_socket[port]

    def _make_request(self, port: int, method: str, *args) -> Optional[any]:
        """Make an RPC call to a node"""
        failures = 0
        max_failures = 3
        
        while failures < max_failures:
            try:
                # Create new socket for each request
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                sock.settimeout(5) 
                
                sock.connect((self.network_ip, port))
                request = pickle.dumps({
                    'method': method,
                    'args': args,
                    'kwargs': {}
                })
                
                sock.sendall(request)
                sock.shutdown(socket.SHUT_WR)
                
                response = pickle.loads(sock.recv(4096))
                sock.close()
                return response.get('result')
                
            except Exception as e:
                failures += 1
                if failures == max_failures:
                    print(f"Request to port {port} method {method} failed after {failures} attempts: {e}")
                sock.close()
                time.sleep(1)  
                
        return None

    def populate_from_file(self, filename: str, batch_size: int = 125) -> Dict[str, bool]:
        """Read data from CSV file and populate into Chord network in batches preventing socket exhaustion"""
        results = {}
        batch = []

        try:
            with open(filename, 'r', encoding='utf-8-sig') as f:
                reader = csv.reader(f)
                headers = next(reader)
                print(f"Found columns: {headers}")
                
                batch_count = 0
                for row in reader:
                    if not row or len(row) <= 3:
                        continue

                    # Create key from Player ID and Year
                    key = f"{row[0].strip()}_{row[3].strip()}"
                    key_hash = int(hashlib.sha1(key.encode()).hexdigest(), 16) % (2**M)
                    
                    # dData for batch processing
                    value = pickle.dumps({headers[i]: row[i] for i in range(len(headers))})
                    batch.append((key, key_hash, value))

                    if len(batch) >= batch_size:
                        print(f"\nProcessing batch {batch_count + 1}...")
                        self._process_batch(batch, results)
                        batch = []
                        batch_count += 1
                        # Add delay between batches to allow for node maintenance
                        time.sleep(1)

                # Process any remaining data
                if batch:
                    print(f"\nProcessing final batch...")
                    self._process_batch(batch, results)

        except Exception as e:
            print(f"Error processing file {filename}: {e}")
            import traceback
            traceback.print_exc()

        return results

    def _process_batch(self, batch: list, results: Dict[str, bool]):
        """Process a batch of keys and store data into the Chord network"""
        for key, key_hash, value in batch:
            successor = self._make_request(self.network_port, 'find_successor', key_hash)
            
            if successor:
                success = self._make_request(successor.port, 'store_data', key, value)
                results[key] = success
                status = "Success" if success else "Failed"
                print(f"Storing key '{key}' (hash: {key_hash}) on node {successor.port}: {status}")
            else:
                results[key] = False
                print(f"Could not find responsible node for key '{key}' (hash: {key_hash})")
            
            # Delay between batches to combat socket exhaustion...
            time.sleep(0.0001)

    def __del__(self):
        """Cleanup when object is destroyed"""
        for sock in self.port_to_socket.values():
            sock.close()

def main():
    if len(sys.argv) != 3:
        print("Usage: python chord_populate.py <network_port> <csv_file>")
        sys.exit(1)

    try:
        network_port = int(sys.argv[1])
        csv_file = sys.argv[2]

        populator = ChordPopulate(network_port)
        results = populator.populate_from_file(csv_file)

        success_count = sum(1 for success in results.values() if success)
        print(f"\nPopulation Summary:")
        print(f"Total keys attempted: {len(results)}")
        print(f"Successfully stored: {success_count}")
        print(f"Failed: {len(results) - success_count}")

    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()