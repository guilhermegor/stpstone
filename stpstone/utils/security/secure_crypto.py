import os
import base64
import hashlib
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives import padding as sym_padding
from cryptography.hazmat.backends import default_backend
import bcrypt


class SecureCrypto:
    """
    A class providing secure cryptographic operations including:
    - bcrypt password hashing and verification
    - PBKDF2 key derivation
    - SHA-256 hashing
    - AES-256 encryption/decryption (CBC mode with random IV)
    - Secure random token generation
    """
    
    # AES block size in bytes
    AES_BLOCK_SIZE = 16
    # PBKDF2 iterations (recommended minimum is 600,000 as of 2023)
    PBKDF2_ITERATIONS = 600000
    
    def __init__(self, pbkdf2_iterations=None):
        """Initialize the crypto instance with optional custom parameters"""
        if pbkdf2_iterations is not None:
            self.PBKDF2_ITERATIONS = pbkdf2_iterations
    
    def generate_salt(self, size=16):
        """Generate a cryptographically secure random salt"""
        return os.urandom(size)
    
    def bcrypt_hash_password(self, password: str) -> str:
        """
        Hash a password using bcrypt with automatically generated salt.
        Returns a string containing the hashed password.
        """
        if not isinstance(password, str):
            raise TypeError("Password must be a string")
        
        # Convert to bytes and hash with bcrypt
        password_bytes = password.encode('utf-8')
        hashed = bcrypt.hashpw(password_bytes, bcrypt.gensalt())
        return hashed.decode('utf-8')
    
    def bcrypt_verify_password(self, password: str, hashed_password: str) -> bool:
        """
        Verify a password against a bcrypt hashed password.
        Returns True if the password matches, False otherwise.
        """
        try:
            password_bytes = password.encode('utf-8')
            hashed_bytes = hashed_password.encode('utf-8')
            return bcrypt.checkpw(password_bytes, hashed_bytes)
        except (ValueError, TypeError):
            return False
    
    def pbkdf2_derive_key(self, password: str, salt: bytes = None, iterations: int = None) -> tuple:
        """
        Derive a cryptographic key using PBKDF2-HMAC-SHA256.
        Returns a tuple of (derived_key_bytes, salt_used).
        """
        if salt is None:
            salt = self.generate_salt(16)
        if iterations is None:
            iterations = self.PBKDF2_ITERATIONS
        
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,  # 32 bytes = 256 bits
            salt=salt,
            iterations=iterations,
            backend=default_backend()
        )
        key = kdf.derive(password.encode('utf-8'))
        return (key, salt)
    
    def sha256_hash(self, data: str) -> str:
        """Generate SHA-256 hash of the input data"""
        if not isinstance(data, str):
            raise TypeError("Data must be a string")
        
        sha256 = hashlib.sha256()
        sha256.update(data.encode('utf-8'))
        return sha256.hexdigest()
    
    def aes_encrypt(self, plaintext: str, key: bytes) -> str:
        """
        Encrypt plaintext using AES-256 in CBC mode with PKCS7 padding.
        Returns a base64 encoded string containing the IV and ciphertext.
        """
        if not isinstance(plaintext, str):
            raise TypeError("Plaintext must be a string")
        if len(key) != 32:
            raise ValueError("Key must be 32 bytes (256 bits) for AES-256")
        
        # Generate random IV
        iv = os.urandom(self.AES_BLOCK_SIZE)
        
        # Pad the plaintext
        padder = sym_padding.PKCS7(self.AES_BLOCK_SIZE * 8).padder()  # Fixed padding import
        padded_data = padder.update(plaintext.encode('utf-8')) + padder.finalize()
        
        # Encrypt
        cipher = Cipher(
            algorithms.AES(key),
            modes.CBC(iv),
            backend=default_backend()
        )
        encryptor = cipher.encryptor()
        ciphertext = encryptor.update(padded_data) + encryptor.finalize()
        
        # Combine IV and ciphertext and return as base64
        combined = iv + ciphertext
        return base64.b64encode(combined).decode('utf-8')
    
    def aes_decrypt(self, encrypted_data: str, key: bytes) -> str:
        """
        Decrypt data encrypted with aes_encrypt().
        Returns the decrypted plaintext string.
        """
        if len(key) != 32:
            raise ValueError("Key must be 32 bytes (256 bits) for AES-256")
        
        # Decode base64 and split IV from ciphertext
        combined = base64.b64decode(encrypted_data)
        iv = combined[:self.AES_BLOCK_SIZE]
        ciphertext = combined[self.AES_BLOCK_SIZE:]
        
        # Decrypt
        cipher = Cipher(
            algorithms.AES(key),
            modes.CBC(iv),
            backend=default_backend()
        )
        decryptor = cipher.decryptor()
        padded_plaintext = decryptor.update(ciphertext) + decryptor.finalize()
        
        # Unpad
        unpadder = sym_padding.PKCS7(self.AES_BLOCK_SIZE * 8).unpadder()  # Fixed padding import
        plaintext = unpadder.update(padded_plaintext) + unpadder.finalize()
        
        return plaintext.decode('utf-8')
    
    def generate_secure_token(self, length=32) -> str:
        """
        Generate a cryptographically secure random token.
        Returns a URL-safe base64 encoded string.
        """
        if length < 16:
            raise ValueError("Token length should be at least 16 bytes for security")
        
        random_bytes = os.urandom(length)
        return base64.urlsafe_b64encode(random_bytes).decode('utf-8').rstrip('=')
    
    def encrypt_with_password(self, plaintext: str, password: str) -> str:
        """
        High-level method to encrypt data with a password.
        Uses PBKDF2 to derive a key from the password, then AES-256 encryption.
        Returns a base64 encoded string containing salt, IV, and ciphertext.
        """
        # Derive key from password
        key, salt = self.pbkdf2_derive_key(password)
        
        # Encrypt the data
        encrypted = self.aes_encrypt(plaintext, key)
        
        # Combine salt and encrypted data
        combined = salt + base64.b64decode(encrypted)
        return base64.b64encode(combined).decode('utf-8')
    
    def decrypt_with_password(self, encrypted_data: str, password: str) -> str:
        """
        Decrypt data encrypted with encrypt_with_password().
        Returns the decrypted plaintext string.
        """
        # Decode base64 and split components
        combined = base64.b64decode(encrypted_data)
        salt = combined[:16]  # Salt size matches what pbkdf2_derive_key uses
        encrypted_part = combined[16:]
        
        # Derive key from password and salt
        key, _ = self.pbkdf2_derive_key(password, salt)
        
        # Decrypt the data
        return self.aes_decrypt(base64.b64encode(encrypted_part).decode('utf-8'), key)