from typing import Any, Optional
from .database import DatabaseManager
from .exceptions import InvalidConfigurationError

class MultiTenantManager:
    def __init__(self, db_instance):
        """
        Initialize the multi-tenant manager
        
        Args:
            db_instance: SQLAlchemy database instance
        """
        self.db_manager = DatabaseManager(db_instance)

    def create_tenant(self, user: Any) -> bool:
        """
        Create a new tenant for a user
        
        Args:
            user: User object that must have an 'id' attribute
            
        Returns:
            bool: True if tenant creation was successful
            
        Raises:
            InvalidConfigurationError: If user object doesn't have required attributes
        """
        if not hasattr(user, 'id'):
            raise InvalidConfigurationError("User object must have an 'id' attribute")

        tenant_id = str(user.id)
        return self.db_manager.create_tenant_database(user)

    def get_tenant_connection(self, user: Any) -> str:
        """
        Get the connection string for a tenant's database
        
        Args:
            user: User object that must have an 'id' attribute
            
        Returns:
            str: Database connection string
        """
        if not hasattr(user, 'id'):
            raise InvalidConfigurationError("User object must have an 'id' attribute")

        return self.db_manager.get_tenant_connection_string(str(user.id))   