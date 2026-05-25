#!/usr/bin/env python3
"""
WebSocket client example for Device WebSocket Service
Demonstrates write and read operations
"""

import asyncio
import json
import websockets
import uuid
from datetime import datetime

# Configuration
WS_URL = "ws://localhost:8080/ws"
CLIENT_ID = str(uuid.uuid4())  # Should be provided by Gateway
CONSUMER_ID = "device-1"

async def example_write():
    """Example: Send command to device"""
    print("\n=== Write Example ===")
    print(f"Client ID: {CLIENT_ID}")
    print(f"Consumer ID: {CONSUMER_ID}")
    
    headers = {
        "X-Client-ID": CLIENT_ID,
    }
    
    try:
        async with websockets.connect(WS_URL, subprotocols=[], extra_headers=headers.items()) as ws:
            # Send command
            request_id = str(uuid.uuid4())
            message = {
                "type": "command",
                "request_id": request_id,
                "consumer_id": CONSUMER_ID,
                "contract_name": "relay.set",
                "payload": {"state": True}
            }
            
            print(f"\nSending command: {json.dumps(message, indent=2)}")
            await ws.send(json.dumps(message))
            
            # Wait for response (30 second timeout per spec)
            print("Waiting for response...")
            try:
                response = await asyncio.wait_for(ws.recv(), timeout=35)
                response_data = json.loads(response)
                print(f"Response: {json.dumps(response_data, indent=2)}")
                
                if response_data.get("type") == "error":
                    print(f"Error: {response_data.get('code')} - {response_data.get('message')}")
                else:
                    print("✓ Command executed successfully")
                    
            except asyncio.TimeoutError:
                print("✗ Response timeout (>30s)")
                
    except Exception as e:
        print(f"✗ Connection error: {e}")

async def example_read():
    """Example: Subscribe to device events"""
    print("\n=== Read Example ===")
    print(f"Client ID: {CLIENT_ID}")
    print(f"Consumer ID: {CONSUMER_ID}")
    
    headers = {
        "X-Client-ID": CLIENT_ID,
    }
    
    try:
        async with websockets.connect(WS_URL, subprotocols=[], extra_headers=headers.items()) as ws:
            # Subscribe to events
            subscription = {
                "type": "subscribe",
                "consumer_id": CONSUMER_ID,
                "contracts": ["temperature", "humidity", "battery.level"]
            }
            
            print(f"\nSending subscription: {json.dumps(subscription, indent=2)}")
            await ws.send(json.dumps(subscription))
            
            # Listen for events (30 second demo)
            print("Listening for events (30s timeout)...")
            start_time = asyncio.get_event_loop().time()
            event_count = 0
            
            try:
                while True:
                    # Set timeout to exit after 30 seconds
                    elapsed = asyncio.get_event_loop().time() - start_time
                    if elapsed > 30:
                        print(f"\n✓ Stopped after 30s (received {event_count} events)")
                        break
                        
                    remaining_time = 30 - elapsed
                    event = await asyncio.wait_for(ws.recv(), timeout=remaining_time)
                    event_data = json.loads(event)
                    
                    if event_data.get("type") == "error":
                        print(f"✗ Error: {event_data.get('code')} - {event_data.get('message')}")
                        break
                    else:
                        if event_data.get("type") == "subscription":
                            print(f"✓ Subscription accepted={event_data.get('accepted')} denied={event_data.get('denied')}")
                            continue
                        event_count += 1
                        print(f"[Event #{event_count}] {event_data.get('contract_name')}: {event_data.get('payload')}")
                        
            except asyncio.TimeoutError:
                print(f"✓ Timeout after 30s (received {event_count} events)")
                
    except Exception as e:
        print(f"✗ Connection error: {e}")

async def example_multiple_commands():
    """Example: Send multiple commands"""
    print("\n=== Multiple Commands Example ===")
    
    headers = {
        "X-Client-ID": CLIENT_ID,
    }
    
    try:
        async with websockets.connect(WS_URL, subprotocols=[], extra_headers=headers.items()) as ws:
            commands = [
                {
                    "contract_name": "relay.set",
                    "payload": {"state": True}
                },
                {
                    "contract_name": "brightness.set",
                    "payload": {"level": 80}
                },
                {
                    "contract_name": "color.set",
                    "payload": {"r": 255, "g": 128, "b": 0}
                },
            ]
            
            for cmd in commands:
                request_id = str(uuid.uuid4())
                message = {
                    "type": "command",
                    "request_id": request_id,
                    "consumer_id": CONSUMER_ID,
                    "contract_name": cmd["contract_name"],
                    "payload": cmd["payload"]
                }
                
                print(f"\nSending: {cmd['contract_name']}")
                await ws.send(json.dumps(message))
                
                try:
                    response = await asyncio.wait_for(ws.recv(), timeout=35)
                    response_data = json.loads(response)
                    
                    if response_data.get("type") == "error":
                        print(f"  ✗ Error: {response_data.get('code')}")
                    else:
                        print(f"  ✓ Status: {response_data.get('status')}")
                        
                except asyncio.TimeoutError:
                    print(f"  ✗ Timeout")
                    
                # Small delay between commands
                await asyncio.sleep(0.5)
                
    except Exception as e:
        print(f"✗ Connection error: {e}")

async def main():
    """Run examples"""
    print("Device WebSocket Service - Client Examples")
    print("=" * 50)
    
    # Run examples
    await example_write()
    await example_read()
    await example_multiple_commands()
    
    print("\n" + "=" * 50)
    print("Examples complete")

if __name__ == "__main__":
    asyncio.run(main())
