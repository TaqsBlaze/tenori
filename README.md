
<center>

![alt text](https://raw.githubusercontent.com/TaqsBlaze/tenori/refs/heads/main/logo/tenori.png)

# Tenori
### Flask Multi Tenant Package
</center>
A lightweight, secure Python package for easily adding multi tenancy to Flask applications using dedicated databases per tenant.

## Features

- 🔐 Secure database creation with SQL injection prevention
- 🎯 Simple integration with existing Flask-SQLAlchemy applications
- ⚡ Automated tenant database management
- 🛠️ Flexible configuration options
- 📝 Type hints for better IDE support

## Installation

```bash
pip install tenori
```

## Quick Start

```python
from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from tenori import MultiTenantManager

app = Flask(__name__)
db = SQLAlchemy(app)

# Initialize the multi-tenant manager
tenant_manager = MultiTenantManager(db)

# Create a new tenant database
@app.route('/signup', methods=['POST'])
def create_tenant():
    try:
        success = tenant_manager.create_tenant(current_user)
        if success:
            return {"status": "success"}
    except Exception as e:
        return {"status": "error", "message": str(e)}
```

## Requirements

- Python 3.10+
- Flask
- Flask-SQLAlchemy
- SQLAlchemy

## Configuration

The package requires a properly configured Flask-SQLAlchemy instance. Your database user must have privileges to create new databases.


## API Reference

### MultiTenantManager

The main class for managing multi tenancy.

```python
manager = MultiTenantManager(db_instance)
```

#### Methods

- `create_tenant(user)`: Creates a new database for the specified user
  - Parameters:
    - `user`: User object (must have an 'id' attribute)
  - Returns:
    - `bool`: True if successful, False otherwise

- `get_tenant_connection(user)`: Gets the connection string for a tenant's database
  - Parameters:
    - `user`: User object (must have an 'id' attribute)
  - Returns:
    - `str`: Database connection string

## Error Handling

The package provides custom exceptions for different scenarios:

- `MultiTenantError`: Base exception class
- `DatabaseCreationError`: Raised when database creation fails
- `InvalidConfigurationError`: Raised when configuration is invalid

## Security Considerations

- Database names are automatically sanitized to prevent SQL injection
- Each tenant gets their own isolated database
- Database credentials are handled securely
- Connection strings are generated dynamically

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Support

If you encounter any issues or need support, please open an issue on GitHub.