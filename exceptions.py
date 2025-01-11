class MultiTenantError(Exception):
    """Base exception for multi-tenant operations"""
    pass

class DatabaseCreationError(MultiTenantError):
    """Raised when database creation fails"""
    pass

class InvalidConfigurationError(MultiTenantError):
    """Raised when configuration is invalid"""
    pass