import os

# Inventory of routers in GNS3 network
# You can override these via environment variables or edit this file directly
ROUTERS = {
    "Router1": {
        "id": "Router1",
        "name": "Core-Router-1",
        "host": os.getenv("ROUTER1_HOST", "192.168.122.101"),
        "port": int(os.getenv("ROUTER1_PORT", 22)),
        "username": os.getenv("ROUTER1_USER", "admin"),
        "password": os.getenv("ROUTER1_PASSWORD", "admin123"),
        "secret": os.getenv("ROUTER1_SECRET", "admin123"),
        "device_type": "cisco_ios",
        "description": "GNS3 Router 1 (Gateway / Core)"
    },
    "Router2": {
        "id": "Router2",
        "name": "Distribution-Router-2",
        "host": os.getenv("ROUTER2_HOST", "192.168.122.102"),
        "port": int(os.getenv("ROUTER2_PORT", 22)),
        "username": os.getenv("ROUTER2_USER", "admin"),
        "password": os.getenv("ROUTER2_PASSWORD", "admin123"),
        "secret": os.getenv("ROUTER2_SECRET", "admin123"),
        "device_type": "cisco_ios",
        "description": "GNS3 Router 2 (Edge / Distribution)"
    }
}

def get_router_config(router_id: str):
    return ROUTERS.get(router_id)
