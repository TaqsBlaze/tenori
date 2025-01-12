import hashlib





class MySqlHash:
    
    def __init__(self, password = None):
        
        self.password = password
        
        
    def generate_hash(self) -> str:
        # Step 1: First SHA-1 hash
        hash1 = hashlib.sha1(self.password.encode('utf-8')).digest()
        # Step 2: Second SHA-1 hash
        hash2 = hashlib.sha1(hash1).hexdigest().upper()
        # Step 3: Prepend '*'
        mysql_hash = f"*{hash2}"
        return mysql_hash