import time
from datetime import datetime

class MockRouter:
    """
    Simulates a Cisco IOS Router terminal response for testing
    when GNS3 is not active or for initial development verification.
    """
    def __init__(self, router_id: str, host: str):
        self.router_id = router_id
        self.host = host
        self.prompt = f"{router_id}#"
        
    def execute(self, commands: list[str]) -> str:
        # Simulate slight network latency (0.2s - 0.5s)
        time.sleep(0.3)
        
        output_lines = [
            f"Connecting to {self.router_id} ({self.host}:22) via Mock SSH...",
            f"[Authenticated as admin - Privilege Level 15]",
            f"{self.prompt} terminal length 0"
        ]
        
        for cmd in commands:
            cmd = cmd.strip()
            output_lines.append(f"{self.prompt} {cmd}")
            simulated_response = self._get_command_output(cmd)
            output_lines.append(simulated_response)
            
        output_lines.append(f"{self.prompt} exit")
        output_lines.append(f"[Connection to {self.host} closed by foreign host]")
        return "\n".join(output_lines)
        
    def _get_command_output(self, cmd: str) -> str:
        cmd_lower = cmd.lower()
        
        if "show ip int" in cmd_lower or "show ip interface brief" in cmd_lower:
            ip_last = "101" if self.router_id == "Router1" else "102"
            peer_last = "102" if self.router_id == "Router1" else "101"
            return f"""Interface                  IP-Address      OK? Method Status                Protocol
GigabitEthernet0/0         192.168.122.{ip_last} YES NVRAM  up                    up      
GigabitEthernet0/1         10.0.0.{ip_last}     YES NVRAM  up                    up      
Loopback0                  1.1.1.{ip_last}       YES NVRAM  up                    up      """

        elif "show ip route" in cmd_lower:
            ip_last = "101" if self.router_id == "Router1" else "102"
            peer_last = "102" if self.router_id == "Router1" else "101"
            return f"""Codes: L - local, C - connected, S - static, R - RIP, M - mobile, B - BGP
       D - EIGRP, EX - EIGRP external, O - OSPF, IA - OSPF inter area 

Gateway of last resort is 192.168.122.1 to network 0.0.0.0

S*    0.0.0.0/0 [1/0] via 192.168.122.1
      10.0.0.0/8 is variably subnetted, 2 subnets, 2 masks
C        10.0.0.0/24 is directly connected, GigabitEthernet0/1
L        10.0.0.{ip_last}/32 is directly connected, GigabitEthernet0/1
      192.168.122.0/24 is variably subnetted, 2 subnets, 2 masks
C        192.168.122.0/24 is directly connected, GigabitEthernet0/0
L        192.168.122.{ip_last}/32 is directly connected, GigabitEthernet0/0
O     1.1.1.{peer_last}/32 [110/2] via 10.0.0.{peer_last}, 00:14:22, GigabitEthernet0/1"""

        elif "show version" in cmd_lower:
            return f"""Cisco IOS Software, 7200 Software (C7200-ADVIPSERVICESK9-M), Version 15.2(4)M11, RELEASE SOFTWARE (fc2)
ROM: System Bootstrap, Version 12.4(122r)T, RELEASE SOFTWARE (fc1)
{self.router_id} uptime is 4 hours, 28 minutes
System returned to ROM by reload
System image file is "tftp://255.255.255.255/c7200-advipservicesk9-mz.152-4.M11.bin"

Cisco 7206VXR (NPE400) processor with 487424K/36864K bytes of memory.
Processor board ID 20657492
R7000 CPU at 150MHz, Implementation 39, Rev 2.1, 256KB L2, 512KB L3 Cache
2 FastEthernet interfaces
2 GigabitEthernet interfaces
Configuration register is 0x2102"""

        elif "show running" in cmd_lower or "show run" in cmd_lower:
            return f"""Building configuration...

Current configuration : 1845 bytes
!
version 15.2
service timestamps debug datetime msec
service timestamps log datetime msec
no service password-encryption
!
hostname {self.router_id}
!
ip domain-name lab.local
!
username admin privilege 15 secret 5 $1$mERr$hx5rVt7rPNoS4wqbXKX7m0
!
interface Loopback0
 ip address 1.1.1.1 255.255.255.255
!
interface GigabitEthernet0/0
 ip address dhcp
 duplex auto
 speed auto
!
interface GigabitEthernet0/1
 ip address 10.0.0.1 255.255.255.0
 duplex auto
 speed auto
!
router ospf 1
 router-id 1.1.1.1
 network 10.0.0.0 0.0.0.255 area 0
 network 1.1.1.0 0.0.0.255 area 0
!
line vty 0 4
 transport input ssh
 login local
!
end"""

        elif "ping" in cmd_lower:
            target = cmd.split()[-1] if len(cmd.split()) > 1 else "192.168.122.1"
            return f"""Type escape sequence to abort.
Sending 5, 100-byte ICMP Echos to {target}, timeout is 2 seconds:
!!!!!
Success rate is 100 percent (5/5), round-trip min/avg/max = 1/2/4 ms"""

        elif "conf t" in cmd_lower or "configure terminal" in cmd_lower:
            return f"""Enter configuration commands, one per line.  End with CNTL/Z."""

        else:
            return f"""Command '{cmd}' executed successfully on {self.router_id}.
[Device response captured]"""
