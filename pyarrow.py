import pyarrow.flight as flight
import pandas as pd
import ssl
import socket
import time

# Connection parameters
host = "your_dremio_host"  # Try with IP address instead of hostname
port = 9407  # Your specific port
pat = "your_personal_access_token"

# Basic network connectivity test
print(f"Testing basic network connectivity to {host}:{port}")
try:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(5)
    result = s.connect_ex((host, port))
    if result == 0:
        print(f"Port {port} is open on {host}")
    else:
        print(f"Port {port} is not accessible on {host} (error code: {result})")
        print("This may indicate a firewall issue or the server isn't listening on this port")
except Exception as e:
    print(f"Socket connection error: {e}")
finally:
    s.close()

# Try a modified Arrow Flight approach
print("\nAttempting Arrow Flight connection with various configurations...")

# Explicitly disable SSL verification for testing
ssl_context = ssl.create_default_context()
ssl_context.check_hostname = False
ssl_context.verify_mode = ssl.CERT_NONE

# Test different connection methods
connection_methods = [
    # Standard TLS connection with verification disabled
    {"type": "tls", "verify": False, "function": flight.Location.for_grpc_tls, 
     "args": [host, port], "kwargs": {"disable_server_verification": True}},
    
    # Insecure connection
    {"type": "insecure", "function": flight.Location.for_grpc, 
     "args": [host, port], "kwargs": {}},
    
    # Try with URI format
    {"type": "uri-tls", "function": flight.Location.for_uri, 
     "args": [f"grpc+tls://{host}:{port}"], "kwargs": {}},
    
    # Try with insecure URI format
    {"type": "uri-insecure", "function": flight.Location.for_uri, 
     "args": [f"grpc://{host}:{port}"], "kwargs": {}},
]

for method in connection_methods:
    print(f"\nTrying {method['type']} connection...")
    try:
        location = method["function"](*method["args"], **method["kwargs"])
        print(f"Creating Flight client with location: {location}")
        
        # Create client with retries
        max_retries = 3
        retry_delay = 2  # seconds
        
        for attempt in range(max_retries):
            try:
                client = flight.FlightClient(location)
                print(f"Successfully created Flight client on attempt {attempt+1}")
                
                # Test with PAT authorization
                options = flight.FlightCallOptions(headers=[
                    (b'authorization', f"token {pat}".encode())
                ])
                
                # Try a simple ping or info action to test connectivity
                try:
                    print("Testing connection with a simple action...")
                    # Different ways to check if connection works
                    try:
                        # Try listing flights (may not be supported by all servers)
                        flights = list(client.list_flights(options))
                        print(f"Connection successful! Found {len(flights)} flights")
                        
                    except Exception as e1:
                        print(f"List flights failed: {e1}")
                        try:
                            # Try a simple descriptor
                            descriptor = flight.FlightDescriptor.for_command("SELECT 1")
                            info = client.get_flight_info(descriptor, options)
                            print(f"Get flight info successful! Got {info}")
                            
                        except Exception as e2:
                            print(f"Get flight info failed: {e2}")
                    
                    # If we got here without an exception, we have a working connection
                    print(f"Found a working connection using {method['type']} method")
                    
                    # Try executing a real query
                    sql_query = "SELECT 1 as test_col"
                    print(f"Executing test query: {sql_query}")
                    
                    try:
                        descriptor = flight.FlightDescriptor.for_command(sql_query)
                        flight_info = client.get_flight_info(descriptor, options)
                        
                        # Debug information about endpoints
                        print(f"Flight info has {len(flight_info.endpoints)} endpoints")
                        for i, endpoint in enumerate(flight_info.endpoints):
                            print(f"Endpoint {i}:")
                            print(f"  Ticket: {endpoint.ticket}")
                            print(f"  Locations: {[loc.uri.decode('utf-8') for loc in endpoint.locations]}")
                        
                        if flight_info.endpoints:
                            reader = client.do_get(flight_info.endpoints[0].ticket, options)
                            table = reader.read_all()
                            df = table.to_pandas()
                            print("Query successful!")
                            print(df)
                            
                            # We have a working connection - exit method loop
                            return
                    except Exception as e:
                        print(f"Error executing test query: {e}")
                    
                except Exception as e:
                    print(f"Connection test failed: {e}")
                
                break  # Break retry loop if we get here
                
            except Exception as e:
                print(f"Failed to create client on attempt {attempt+1}: {e}")
                if attempt < max_retries - 1:
                    print(f"Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                    
    except Exception as e:
        print(f"Could not create location for {method['type']}: {e}")

print("\nAll connection methods failed. Recommendations:")
print("1. Verify the port number is correct for Arrow Flight (typically 32010, not 9407)")
print("2. Check if SSL/TLS is properly configured on the server")
print("3. Verify network connectivity and firewall rules")
print("4. Confirm the Dremio Flight service is enabled on the server")
print("5. Check Dremio server logs for more specific error information")