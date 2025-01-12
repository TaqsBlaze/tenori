from typing import Optional
import sqlalchemy as sa
from sqlalchemy import text
from .exceptions import DatabaseCreationError
from .hash import MySqlHash
import re
from flask import current_app
from flask_sqlalchemy import SQLAlchemy

class DatabaseManager:
    def __init__(self, db_instance):
        self.db = db_instance
        self._db_instance = db_instance
        self._engine = None

    @property
    def engine(self):
        """Lazy load engine only when needed and within app context"""
        if self._engine is None:
            if not current_app:
                raise RuntimeError(
                    "No application context found. "
                    "Initialize MultiTenantManager within a Flask app context or route."
                )
            self._engine = self._db_instance.engine
        return self._engine

    def _sanitize_db_name(self, name: str) -> str:
        """Sanitize database name to prevent SQL injection"""
        sanitized = re.sub(r'[^a-zA-Z0-9_]', '', name)
        return sanitized.lower()

    def create_tenant_database(self, tenant_identifier: str) -> bool:
        """
        Create a new database for a tenant
        Returns True if successful, False otherwise
        """
        try:
            db_name = self._sanitize_db_name(f"tenant_{tenant_identifier.username}")
            passwd = MySqlHash(tenant_identifier.password)
            passwd = passwd.generate_hash()
            # Create database if it doesn't exist
            with self.engine.connect() as conn:
                conn.execute(text(f"CREATE USER '{tenant_identifier.username}'@'localhost' IDENTIFIED VIA mysql_native_password USING '{passwd}';"))
                conn.execute(text(f"GRANT SELECT, INSERT, UPDATE, DELETE, FILE ON *.* TO '{tenant_identifier.username}'@'localhost' REQUIRE NONE WITH MAX_QUERIES_PER_HOUR 0 MAX_CONNECTIONS_PER_HOUR 0 MAX_UPDATES_PER_HOUR 0 MAX_USER_CONNECTIONS 0;"))
                conn.execute(text(f"CREATE DATABASE IF NOT EXISTS `{db_name}`;"))
                conn.execute(text(f"GRANT ALL PRIVILEGES ON `{db_name}`.* TO '{tenant_identifier.username}'@'localhost';"))

                
                # Create a new SQLAlchemy instance for the tenant's database
                tenant_engine = sa.create_engine(
                    f"{self.engine.url.drivername}://{tenant_identifier.username}:{tenant_identifier.password}@{self.engine.url.host}/{db_name}"
                )
                
                # Create all tables in the new database
                with current_app.app_context():
                    original_bind = self.db.session.bind  # Store the original bind
                    try:
                        self.db.session.bind = tenant_engine  # Bind session to tenant's engine
                        self.db.create_all()  # Create tables
                    finally:
                        self.db.session.bind = original_bind  # Restore the original bind

                return True
            
        except Exception as e:
            raise DatabaseCreationError(f"Failed to create tenant database: {str(e)}")

    def get_tenant_connection_string(self, tenant_identifier: str) -> str:
        """Get the connection string for a tenant's database"""
        db_name = self._sanitize_db_name(f"tenant_{tenant_identifier}")
        return f"{self.engine.url.drivername}://{self.engine.url.username}:{self.engine.url.password}@{self.engine.url.host}/{db_name}"