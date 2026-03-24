import os
import urllib.parse
from sqlalchemy import create_engine
from dotenv import load_dotenv
from sshtunnel import SSHTunnelForwarder

load_dotenv()

class DBConnector:
    _engine = None
    _tunnel = None # Keep track of the tunnel too

    @classmethod
    def get_engine(cls):
        if cls._engine is None:
            bastion_ip = os.getenv('BASTION_IP')
            ssh_key_content = os.getenv("SSH_PRIVATE_KEY")
            
            if not bastion_ip:
                raise ValueError("BASTION_IP is missing from environment variables!")

            if not ssh_key_content:
                raise ValueError("SSH_PRIVATE_KEY is missing from Env Vars!")

            # --- ADD THIS PART HERE ---
            # 1. Create a temporary .pem file for Render
            pkey_path = "/tmp/bastion.pem"
            with open(pkey_path, "w") as f:
                f.write(ssh_key_content.strip())
            os.chmod(pkey_path, 0o600) # SSH requires strict permissions
            # ---------------------------

            # 2. Start the Tunnel FIRST
            cls._tunnel = SSHTunnelForwarder(
                (bastion_ip, 22),
                ssh_username="ec2-user",
                ssh_pkey=pkey_path,
                remote_bind_address=(os.getenv('AWS_DB_HOST'), 5432),
                local_bind_address=('127.0.0.1', 5433)
            )
            cls._tunnel.start()

            # 3. Point SQLAlchemy to the TUNNEL, not the RDS directly
            # We use localhost:5433 because that's where the tunnel is listening
            user = os.getenv('DB_USER')
            password = urllib.parse.quote_plus(str(os.getenv('DB_PASSWORD')))
            db_url = f"postgresql://{user}:{password}@127.0.0.1:5433/{os.getenv('DB_NAME')}"
            
            cls._engine = create_engine(db_url, pool_size=10)
            
        return cls._engine

    @classmethod
    def dispose(cls):
        """Call this at the very end of main.py"""
        if cls._engine:
            cls._engine.dispose()
        if cls._tunnel:
            cls._tunnel.stop()