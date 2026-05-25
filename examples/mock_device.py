#!/usr/bin/env python3
"""
Mock MQTT Device example
Simulates a real device that:
1. Listens to devices/{id}/commands
2. Responds to devices/{id}/responses
3. Publishes events to devices/{id}/events
"""

import json
import time
import asyncio
import random
from datetime import datetime
import paho.mqtt.client as mqtt

# Configuration
MQTT_BROKER = "localhost"
MQTT_PORT = 1883
DEVICE_ID = "device-1"
DEVICE_TOKEN = "consumer-token"

# Topics
CMD_TOPIC = f"devices/{DEVICE_ID}/commands"
RESP_TOPIC = f"devices/{DEVICE_ID}/responses"
EVENT_TOPIC = f"devices/{DEVICE_ID}/events"

class MockDevice:
    def __init__(self, device_id):
        self.device_id = device_id
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        self.client.username_pw_set(self.device_id, DEVICE_TOKEN)
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.is_running = False
        
    def on_connect(self, client, userdata, connect_flags, reason_code, properties):
        """Callback when device connects to MQTT"""
        if reason_code == 0:
            print(f"✓ Connected to MQTT broker")
            # Subscribe to commands
            self.client.subscribe(CMD_TOPIC)
            print(f"✓ Subscribed to: {CMD_TOPIC}")
        else:
            print(f"✗ Connection failed: {reason_code}")
    
    def on_message(self, client, userdata, msg):
        """Callback when command is received"""
        try:
            command = json.loads(msg.payload.decode())
            print(f"\n[COMMAND] Received: {command['contract_name']}")
            print(f"  Request ID: {command['request_id']}")
            print(f"  Payload: {command['payload']}")
            
            # Process command
            self.process_command(command)
            
            # Send response
            self.send_response(command)
            
        except Exception as e:
            print(f"✗ Error processing command: {e}")
    
    def process_command(self, command):
        """Simulate command processing"""
        contract = command['contract_name']
        
        if contract == "relay.set":
            state = command['payload'].get('state')
            print(f"  -> Setting relay to {state}")
            time.sleep(0.5)  # Simulate processing
            
        elif contract == "brightness.set":
            level = command['payload'].get('level')
            print(f"  -> Setting brightness to {level}%")
            time.sleep(0.3)
            
        elif contract == "color.set":
            r, g, b = command['payload'].get('r'), command['payload'].get('g'), command['payload'].get('b')
            print(f"  -> Setting color to RGB({r}, {g}, {b})")
            time.sleep(0.3)
        
        else:
            print(f"  -> Unknown contract")
    
    def send_response(self, command):
        """Send response to server"""
        response = {
            "request_id": command['request_id'],
            "consumer_id": command['consumer_id'],
            "contract_name": command['contract_name'],
            "status": "ok",
            "payload": command['payload'],
            "ts": datetime.utcnow().isoformat() + "Z"
        }
        
        self.client.publish(RESP_TOPIC, json.dumps(response), qos=1)
        print(f"  -> Response sent")
    
    def publish_events(self):
        """Periodically publish events"""
        contracts = [
            {
                "name": "temperature",
                "value": round(20 + random.uniform(-5, 5), 1)
            },
            {
                "name": "humidity",
                "value": round(40 + random.uniform(-10, 10), 1)
            },
            {
                "name": "battery.level",
                "value": int(80 + random.uniform(-20, 20))
            }
        ]
        
        while self.is_running:
            for contract in contracts:
                event = {
                    "consumer_id": self.device_id,
                    "contract_name": contract['name'],
                    "payload": {"value": contract['value']},
                    "ts": datetime.utcnow().isoformat() + "Z"
                }
                
                self.client.publish(EVENT_TOPIC, json.dumps(event), qos=1)
                print(f"[EVENT] Published {contract['name']}: {contract['value']}")
                
                # Simulate measurement changes
                if contract['name'] == "temperature":
                    contract['value'] = round(20 + random.uniform(-5, 5), 1)
                elif contract['name'] == "humidity":
                    contract['value'] = round(40 + random.uniform(-10, 10), 1)
                else:
                    contract['value'] = int(80 + random.uniform(-20, 20))
                
                time.sleep(1)
            
            # Wait before next cycle
            time.sleep(4)
    
    def connect(self):
        """Connect to MQTT broker"""
        try:
            self.client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
            self.is_running = True
            print(f"Connecting to MQTT broker at {MQTT_BROKER}:{MQTT_PORT}...")
        except Exception as e:
            print(f"✗ Failed to connect: {e}")
            return False
        return True
    
    def run(self):
        """Run device"""
        if not self.connect():
            return
        
        # Start MQTT loop in background
        self.client.loop_start()
        
        try:
            print(f"\n✓ Mock device '{self.device_id}' is running")
            print("Commands:")
            print(f"  - Listen on:  {CMD_TOPIC}")
            print(f"  - Respond on: {RESP_TOPIC}")
            print(f"  - Events on:  {EVENT_TOPIC}")
            print("\nPress Ctrl+C to stop\n")
            
            # Publish events
            self.publish_events()
            
        except KeyboardInterrupt:
            print("\n\nShutting down...")
        finally:
            self.is_running = False
            self.client.loop_stop()
            self.client.disconnect()
            print("✓ Device disconnected")

if __name__ == "__main__":
    device = MockDevice(DEVICE_ID)
    device.run()
