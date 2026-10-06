import time
import socket
import os
import paramiko
from mock_router import MockRouter

class RouterClient:
    """
    Paramiko SSH Client for Cisco IOS routers in GNS3.
    Supports interactive shell with pagination disable (terminal length 0).
    Has automatic mock fallback when GNS3 is not online.
    """
    def __init__(self, config: dict, timeout: int = 8):
        self.config = config
        self.router_id = config.get("id", "Router")
        self.host = config.get("host", "127.0.0.1")
        self.port = config.get("port", 22)
        self.username = config.get("username", "admin")
        self.password = config.get("password", "admin123")
        self.secret = config.get("secret", "admin123")
        self.timeout = timeout
        
        # Check global environment variable to force mock mode
        self.force_mock = os.getenv("MOCK_ROUTER", "auto").lower() == "true"
        self.allow_mock_fallback = os.getenv("MOCK_FALLBACK", "true").lower() == "true"

    def execute_commands(self, commands: list[str]) -> tuple[bool, str, bool]:
        """
        Executes a list of CLI commands on the router.
        Returns: (success: bool, output: str, is_mock: bool)
        """
        if self.force_mock:
            mock = MockRouter(self.router_id, self.host)
            return True, mock.execute(commands), True

        try:
            output = self._ssh_execute(commands)
            return True, output, False
        except (socket.timeout, TimeoutError) as e:
            err_msg = f"[SSH Timeout] Could not connect to {self.router_id} at {self.host}:{self.port} within {self.timeout}s"
            if self.allow_mock_fallback:
                mock = MockRouter(self.router_id, self.host)
                fallback_output = mock.execute(commands)
                return True, f"{err_msg}\n>>> [FALLBACK TO SIMULATION MODE] <<<\n\n{fallback_output}", True
            return False, err_msg, False
        except paramiko.AuthenticationException as e:
            return False, f"[SSH Auth Failed] Invalid username or password for {self.router_id} ({self.host}): {str(e)}", False
        except (paramiko.SSHException, socket.error) as e:
            err_msg = f"[SSH Connection Error] Failed connecting to {self.router_id} ({self.host}:{self.port}): {str(e)}"
            if self.allow_mock_fallback:
                mock = MockRouter(self.router_id, self.host)
                fallback_output = mock.execute(commands)
                return True, f"{err_msg}\n>>> [FALLBACK TO SIMULATION MODE] <<<\n\n{fallback_output}", True
            return False, err_msg, False
        except Exception as e:
            return False, f"[Unexpected Error] {str(e)}", False

    def _ssh_execute(self, commands: list[str]) -> str:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        
        # Connect to Cisco Router
        ssh.connect(
            hostname=self.host,
            port=self.port,
            username=self.username,
            password=self.password,
            timeout=self.timeout,
            look_for_keys=False,
            allow_agent=False
        )

        # Open interactive shell channel (standard for Cisco IOS)
        channel = ssh.invoke_shell(width=120, height=40)
        time.sleep(0.5)

        # Clear login banner
        if channel.recv_ready():
            channel.recv(4096)

        # Disable terminal pagination so output doesn't pause at --More--
        channel.send("terminal length 0\n")
        time.sleep(0.5)
        if channel.recv_ready():
            channel.recv(4096)

        output_buffer = []
        for cmd in commands:
            channel.send(cmd.strip() + "\n")
            time.sleep(0.6)
            
            # Read all available bytes for this command
            cmd_output = ""
            start_wait = time.time()
            while time.time() - start_wait < 5.0:
                if channel.recv_ready():
                    chunk = channel.recv(4096).decode("utf-8", errors="ignore")
                    cmd_output += chunk
                    # If prompt is found at end of chunk, command is likely finished
                    if "#" in chunk or ">" in chunk:
                        break
                time.sleep(0.2)
            output_buffer.append(cmd_output if cmd_output else f"{cmd}\n[No output received]")

        channel.send("exit\n")
        time.sleep(0.2)
        ssh.close()
        
        return "\n".join(output_buffer)
