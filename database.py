from typing import Optional
import sqlalchemy as sa
from sqlalchemy import text
from .exceptions import DatabaseCreationError
import re

class DatabaseManager:
    def __init__(self, db_instance):
        self.db = db_instance
        self.engine = db_instance.engine

    def _sanitize_db_name(self, name: str) -> str:
        """Sanitize database name to prevent SQL injection"""
        # Remove special characters and spaces, keep alphanumeric and underscore
        sanitized = re.sub(r'[^a-zA-Z0-9_]', '', name)
        return sanitized.lower()

    def create_tenant_database(self, tenant_identifier: str) -> bool:
        """
        Create a new database for a tenant
        Returns True if successful, False otherwise
        """
        try:
            db_name = self._sanitize_db_name(f"tenant_{tenant_identifier}")
            
            # Create database if it doesn't exist
            with self.engine.connect() as conn:
                conn.execute(text(f"CREATE DATABASE IF NOT EXISTS {db_name}"))
                
                # Test connection to new database
                test_engine = sa.create_engine(
                    f"{self.engine.url.drivername}://{self.engine.url.username}:{self.engine.url.password}@{self.engine.url.host}/{db_name}"
                )
                test_engine.connect().close()
                
            return True
            
        except Exception as e:
            raise DatabaseCreationError(f"Failed to create tenant database: {str(e)}")

    def get_tenant_connection_string(self, tenant_identifier: str) -> str:
        """Get the connection string for a tenant's database"""
        db_name = self._sanitize_db_name(f"tenant_{tenant_identifier}")
        return f"{self.engine.url.drivername}://{self.engine.url.username}:{self.engine.url.password}@{self.engine.url.host}/{db_name}"