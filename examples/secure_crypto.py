import os
from stpstone.utils.security.secure_crypto import SecureCrypto

# Create an instance of the crypto helper
crypto = SecureCrypto()

# 1. Password hashing with bcrypt
hashed_pw = crypto.bcrypt_hash_password("my_secure_password")
print(f"BCrypt hashed password: {hashed_pw}")
print(f"Password verification: {crypto.bcrypt_verify_password('my_secure_password', hashed_pw)}")

# 2. PBKDF2 key derivation
key, salt = crypto.pbkdf2_derive_key("password123")
print(f"PBKDF2 derived key: {key.hex()}")

# 3. SHA-256 hashing
hash_result = crypto.sha256_hash("data to hash")
print(f"SHA-256 hash: {hash_result}")

# 4. AES encryption/decryption
key = os.urandom(32)  # 256-bit key for AES-256
encrypted = crypto.aes_encrypt("secret message", key)
print(f"AES encrypted: {encrypted}")
decrypted = crypto.aes_decrypt(encrypted, key)
print(f"AES decrypted: {decrypted}")

# 5. Password-based encryption
encrypted_data = crypto.encrypt_with_password("top secret", "password123")
print(f"Password encrypted: {encrypted_data}")
decrypted_data = crypto.decrypt_with_password(encrypted_data, "password123")
print(f"Password decrypted: {decrypted_data}")

# 6. Secure token generation
token = crypto.generate_secure_token()
print(f"Secure token: {token}")