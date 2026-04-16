"""Secure cryptographic operations implementation.

This module provides a class for performing various cryptographic operations including:
- Password hashing and verification using bcrypt
- Key derivation using PBKDF2
- SHA-256 hashing
- AES-256 encryption/decryption
- Secure random token generation
"""

import base64
import hashlib
import os
from typing import Optional

import bcrypt
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes, padding as sym_padding
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker


class SecureCrypto(metaclass=TypeChecker):
	"""Class providing secure cryptographic operations.

	Attributes
	----------
	AES_BLOCK_SIZE : int
		AES block size in bytes (default: 16)
	PBKDF2_ITERATIONS : int
		PBKDF2 iterations count (default: 600_000)
	"""

	AES_BLOCK_SIZE = 16
	PBKDF2_ITERATIONS = 600_000

	def __init__(self, pbkdf2_iterations: Optional[int] = None) -> None:
		"""Initialize the crypto instance with optional custom parameters.

		Parameters
		----------
		pbkdf2_iterations : Optional[int]
			Custom number of iterations for PBKDF2 (default: None)
		"""
		if pbkdf2_iterations is not None:
			self.PBKDF2_ITERATIONS = pbkdf2_iterations

	def _validate_password(self, password: str) -> None:
		"""Validate password input.

		Parameters
		----------
		password : str
			Password to validate

		Raises
		------
		TypeError
			If password is not a string
		"""
		if not isinstance(password, str):
			raise TypeError("Password must be a string")

	def _validate_key_length(self, key: bytes) -> None:
		"""Validate AES key length.

		Parameters
		----------
		key : bytes
			Key to validate

		Raises
		------
		ValueError
			If key length is not 32 bytes
		"""
		if len(key) != 32:
			raise ValueError("Key must be 32 bytes (256 bits) for AES-256")

	def _validate_token_length(self, length: int) -> None:
		"""Validate token length.

		Parameters
		----------
		length : int
			Token length to validate

		Raises
		------
		ValueError
			If length is less than 16 bytes
		"""
		if length < 16:
			raise ValueError("Token length should be at least 16 bytes for security")

	def generate_salt(self, size: int = 16) -> bytes:
		"""Generate a cryptographically secure random salt.

		Parameters
		----------
		size : int
			Size of the salt in bytes (default: 16)

		Returns
		-------
		bytes
			Generated salt
		"""
		return os.urandom(size)

	def bcrypt_hash_password(self, password: str) -> str:
		"""Hash a password using bcrypt with automatically generated salt.

		Parameters
		----------
		password : str
			Password to hash

		Returns
		-------
		str
			Hashed password as UTF-8 string
		"""
		self._validate_password(password)
		password_bytes = password.encode("utf-8")
		hashed = bcrypt.hashpw(password_bytes, bcrypt.gensalt())
		return hashed.decode("utf-8")

	def bcrypt_verify_password(self, password: str, hashed_password: str) -> bool:
		"""Verify a password against a bcrypt hashed password.

		Parameters
		----------
		password : str
			Password to verify
		hashed_password : str
			Hashed password to compare against

		Returns
		-------
		bool
			True if password matches, False otherwise
		"""
		try:
			self._validate_password(password)
			password_bytes = password.encode("utf-8")
			hashed_bytes = hashed_password.encode("utf-8")
			return bcrypt.checkpw(password_bytes, hashed_bytes)
		except (ValueError, TypeError):
			return False

	def pbkdf2_derive_key(
		self, password: str, salt: Optional[bytes] = None, iterations: Optional[int] = None
	) -> tuple[bytes, bytes]:
		"""Derive a cryptographic key using PBKDF2-HMAC-SHA256.

		Parameters
		----------
		password : str
			Password to derive key from
		salt : Optional[bytes]
			Optional salt value (default: None)
		iterations : Optional[int]
			Optional iteration count (default: None)

		Returns
		-------
		tuple[bytes, bytes]
			tuple containing derived key and salt used
		"""
		self._validate_password(password)
		if salt is None:
			salt = self.generate_salt(16)
		if iterations is None:
			iterations = self.PBKDF2_ITERATIONS

		kdf = PBKDF2HMAC(
			algorithm=hashes.SHA256(),
			length=32,
			salt=salt,
			iterations=iterations,
			backend=default_backend(),
		)
		key = kdf.derive(password.encode("utf-8"))
		return (key, salt)

	def sha256_hash(self, data: str) -> str:
		"""Generate SHA-256 hash of the input data.

		Parameters
		----------
		data : str
			Data to hash

		Returns
		-------
		str
			Hex digest of the hash

		Raises
		------
		TypeError
			If data is not a string
		"""
		if not isinstance(data, str):
			raise TypeError("Data must be a string")

		sha256 = hashlib.sha256()
		sha256.update(data.encode("utf-8"))
		return sha256.hexdigest()

	def aes_encrypt(self, plaintext: str, key: bytes) -> str:
		"""Encrypt plaintext using AES-256 in CBC mode with PKCS7 padding.

		Parameters
		----------
		plaintext : str
			Text to encrypt
		key : bytes
			Encryption key (must be 32 bytes)

		Returns
		-------
		str
			Base64 encoded string containing IV and ciphertext

		Raises
		------
		TypeError
			If plaintext is not a string
		"""
		if not isinstance(plaintext, str):
			raise TypeError("Plaintext must be a string")
		self._validate_key_length(key)

		iv = os.urandom(self.AES_BLOCK_SIZE)
		padder = sym_padding.PKCS7(self.AES_BLOCK_SIZE * 8).padder()
		padded_data = padder.update(plaintext.encode("utf-8")) + padder.finalize()

		cipher = Cipher(algorithms.AES(key), modes.CBC(iv), backend=default_backend())
		encryptor = cipher.encryptor()
		ciphertext = encryptor.update(padded_data) + encryptor.finalize()

		combined = iv + ciphertext
		return base64.b64encode(combined).decode("utf-8")

	def aes_decrypt(self, encrypted_data: str, key: bytes) -> str:
		"""Decrypt data encrypted with aes_encrypt().

		Parameters
		----------
		encrypted_data : str
			Data to decrypt (base64 encoded)
		key : bytes
			Decryption key (must be 32 bytes)

		Returns
		-------
		str
			Decrypted plaintext
		"""
		self._validate_key_length(key)

		combined = base64.b64decode(encrypted_data)
		iv = combined[: self.AES_BLOCK_SIZE]
		ciphertext = combined[self.AES_BLOCK_SIZE :]

		cipher = Cipher(algorithms.AES(key), modes.CBC(iv), backend=default_backend())
		decryptor = cipher.decryptor()
		padded_plaintext = decryptor.update(ciphertext) + decryptor.finalize()

		unpadder = sym_padding.PKCS7(self.AES_BLOCK_SIZE * 8).unpadder()
		plaintext = unpadder.update(padded_plaintext) + unpadder.finalize()

		return plaintext.decode("utf-8")

	def generate_secure_token(self, length: int = 32) -> str:
		"""Generate a cryptographically secure random token.

		Parameters
		----------
		length : int
			Token length in bytes (default: 32)

		Returns
		-------
		str
			URL-safe base64 encoded token
		"""
		self._validate_token_length(length)
		random_bytes = os.urandom(length)
		return base64.urlsafe_b64encode(random_bytes).decode("utf-8").rstrip("=")

	def encrypt_with_password(self, plaintext: str, password: str) -> str:
		"""Encrypt data with a password using PBKDF2 and AES-256.

		Parameters
		----------
		plaintext : str
			Text to encrypt
		password : str
			Password to use for encryption

		Returns
		-------
		str
			Base64 encoded string containing salt, IV, and ciphertext
		"""
		key, salt = self.pbkdf2_derive_key(password)
		encrypted = self.aes_encrypt(plaintext, key)
		combined = salt + base64.b64decode(encrypted)
		return base64.b64encode(combined).decode("utf-8")

	def decrypt_with_password(self, encrypted_data: str, password: str) -> str:
		"""Decrypt data encrypted with encrypt_with_password().

		Parameters
		----------
		encrypted_data : str
			Data to decrypt (base64 encoded)
		password : str
			Password used for encryption

		Returns
		-------
		str
			Decrypted plaintext
		"""
		combined = base64.b64decode(encrypted_data)
		salt = combined[:16]
		encrypted_part = combined[16:]

		key, _ = self.pbkdf2_derive_key(password, salt)
		return self.aes_decrypt(base64.b64encode(encrypted_part).decode("utf-8"), key)
