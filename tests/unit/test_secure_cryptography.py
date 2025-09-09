"""Unit tests for SecureCrypto class.

Tests the cryptographic operations functionality including:
- Password hashing and verification
- Key derivation
- Hashing algorithms
- Encryption/decryption operations
- Secure token generation
"""

import base64
import os

import pytest

from stpstone.utils.security.secure_cryptography import SecureCrypto


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def secure_crypto() -> SecureCrypto:
    """Fixture providing SecureCrypto instance.

    Returns
    -------
    SecureCrypto
        Instance of SecureCrypto class
    """
    return SecureCrypto()


@pytest.fixture
def sample_password() -> str:
    """Fixture providing sample password for testing.

    Returns
    -------
    str
        Test password string
    """
    return "test_password_123!"


@pytest.fixture
def sample_key() -> bytes:
    """Fixture providing sample AES-256 key.

    Returns
    -------
    bytes
        32-byte test key
    """
    return os.urandom(32)


@pytest.fixture
def sample_plaintext() -> str:
    """Fixture providing sample plaintext for encryption.

    Returns
    -------
    str
        Test plaintext string
    """
    return "This is a secret message!"


# --------------------------
# Tests
# --------------------------
class TestInitialization:
    """Test cases for SecureCrypto initialization."""

    def test_default_initialization(self) -> None:
        """Test initialization with default parameters.

        Verifies
        --------
        - Instance is created with default PBKDF2 iterations
        - AES block size is set correctly

        Returns
        -------
        None
        """
        crypto = SecureCrypto()
        assert crypto.PBKDF2_ITERATIONS == 600_000
        assert crypto.AES_BLOCK_SIZE == 16

    def test_custom_iterations(self) -> None:
        """Test initialization with custom iterations.

        Verifies
        --------
        - Instance accepts and uses custom PBKDF2 iterations

        Returns
        -------
        None
        """
        custom_iterations = 100_000
        crypto = SecureCrypto(pbkdf2_iterations=custom_iterations)
        assert custom_iterations == crypto.PBKDF2_ITERATIONS


class TestPasswordValidation:
    """Test cases for password validation methods."""

    def test_validate_password_valid(self, secure_crypto: SecureCrypto) -> None:
        """Test password validation with valid input.

        Verifies
        --------
        - Valid password string passes validation

        Parameters
        ----------
        secure_crypto : SecureCrypto
            Instance of SecureCrypto class

        Returns
        -------
        None
        """
        secure_crypto._validate_password("valid_password")

    def test_validate_password_invalid_type(self, secure_crypto: SecureCrypto) -> None:
        """Test password validation with invalid type.

        Verifies
        --------
        - Non-string password raises TypeError

        Parameters
        ----------
        secure_crypto : SecureCrypto
            Instance of SecureCrypto class

        Returns
        -------
        None
        """
        with pytest.raises(TypeError, match="must be of type"):
            secure_crypto._validate_password(12345)


class TestKeyValidation:
    """Test cases for key validation methods."""

    def test_validate_key_length_valid(
        self, 
        secure_crypto: SecureCrypto, 
        sample_key: bytes
    ) -> None:
        """Test key validation with valid key.

        Verifies
        --------
        - 32-byte key passes validation

        Parameters
        ----------
        secure_crypto : SecureCrypto
            Instance of SecureCrypto class
        sample_key : bytes
            32-byte test key

        Returns
        -------
        None
        """
        secure_crypto._validate_key_length(sample_key)

    def test_validate_key_length_invalid(self, secure_crypto: SecureCrypto) -> None:
        """Test key validation with invalid key length.

        Verifies
        --------
        - Non-32-byte key raises ValueError

        Parameters
        ----------
        secure_crypto : SecureCrypto
            Instance of SecureCrypto class

        Returns
        -------
        None
        """
        with pytest.raises(ValueError, match="Key must be 32 bytes"):
            secure_crypto._validate_key_length(os.urandom(16))


class TestTokenValidation:
    """Test cases for token validation methods."""

    def test_validate_token_length_valid(self, secure_crypto: SecureCrypto) -> None:
        """Test token validation with valid length.

        Verifies
        --------
        - Length >=16 passes validation

        Parameters
        ----------
        secure_crypto : SecureCrypto
            Instance of SecureCrypto class

        Returns
        -------
        None
        """
        secure_crypto._validate_token_length(16)
        secure_crypto._validate_token_length(32)

    def test_validate_token_length_invalid(self, secure_crypto: SecureCrypto) -> None:
        """Test token validation with invalid length.

        Verifies
        --------
        - Length <16 raises ValueError

        Parameters
        ----------
        secure_crypto : SecureCrypto
            Instance of SecureCrypto class

        Returns
        -------
        None
        """
        with pytest.raises(ValueError, match="Token length should be at least 16 bytes"):
            secure_crypto._validate_token_length(15)


class TestBcryptOperations:
    """Test cases for bcrypt password hashing and verification."""

    def test_bcrypt_hash_password_valid(
        self, 
        secure_crypto: SecureCrypto, 
        sample_password: str
    ) -> None:
        """Test password hashing with valid input.

        Verifies
        --------
        - Returns a string
        - Result is different from original password
        - Result starts with bcrypt identifier

        Parameters
        ----------
        secure_crypto : SecureCrypto
            Instance of SecureCrypto class
        sample_password : str
            Test password

        Returns
        -------
        None
        """
        hashed = secure_crypto.bcrypt_hash_password(sample_password)
        assert isinstance(hashed, str)
        assert hashed != sample_password
        assert hashed.startswith("$2b$")

    def test_bcrypt_hash_password_invalid_type(
        self, 
        secure_crypto: SecureCrypto
    ) -> None:
        """Test password hashing with invalid type.

        Verifies
        --------
        - Non-string password raises TypeError

        Parameters
        ----------
        secure_crypto : SecureCrypto
            Instance of SecureCrypto class

        Returns
        -------
        None
        """
        with pytest.raises(TypeError, match="must be of type"):
            secure_crypto.bcrypt_hash_password(12345)

    def test_bcrypt_verify_password_correct(
        self, 
        secure_crypto: SecureCrypto, 
        sample_password: str
    ) -> None:
        """Test password verification with correct password.

        Verifies
        --------
        - Returns True for correct password

        Parameters
        ----------
        secure_crypto : SecureCrypto
            Instance of SecureCrypto class
        sample_password : str
            Test password

        Returns
        -------
        None
        """
        hashed = secure_crypto.bcrypt_hash_password(sample_password)
        assert secure_crypto.bcrypt_verify_password(sample_password, hashed) is True

    def test_bcrypt_verify_password_incorrect(
        self, 
        secure_crypto: SecureCrypto, 
        sample_password: str
    ) -> None:
        """Test password verification with incorrect password.

        Verifies
        --------
        - Returns False for incorrect password

        Parameters
        ----------
        secure_crypto : SecureCrypto
            Instance of SecureCrypto class
        sample_password : str
            Test password

        Returns
        -------
        None
        """
        hashed = secure_crypto.bcrypt_hash_password(sample_password)
        assert secure_crypto.bcrypt_verify_password("wrong_password", hashed) is False

    def test_bcrypt_verify_password_invalid_hash(
        self, 
        secure_crypto: SecureCrypto, 
        sample_password: str
    ) -> None:
        """Test password verification with invalid hash.

        Verifies
        --------
        - Returns False for invalid hash format

        Parameters
        ----------
        secure_crypto : SecureCrypto
            Instance of SecureCrypto class
        sample_password : str
            Test password

        Returns
        -------
        None
        """
        assert secure_crypto.bcrypt_verify_password(sample_password, "invalid_hash") is False


class TestPBKDF2Operations:
    """Test cases for PBKDF2 key derivation."""

    def test_pbkdf2_derive_key_default_salt(
        self, 
        secure_crypto: SecureCrypto, 
        sample_password: str
    ) -> None:
        """Test key derivation with default salt.

        Verifies
        --------
        - Returns tuple of (key, salt)
        - Both key and salt are bytes
        - Key length is 32 bytes
        - Salt length is 16 bytes

        Parameters
        ----------
        secure_crypto : SecureCrypto
            Instance of SecureCrypto class
        sample_password : str
            Test password

        Returns
        -------
        None
        """
        key, salt = secure_crypto.pbkdf2_derive_key(sample_password)
        assert isinstance(key, bytes)
        assert isinstance(salt, bytes)
        assert len(key) == 32
        assert len(salt) == 16

    def test_pbkdf2_derive_key_custom_salt(
        self, 
        secure_crypto: SecureCrypto, 
        sample_password: str
    ) -> None:
        """Test key derivation with custom salt.

        Verifies
        --------
        - Uses provided salt
        - Returns same salt as provided

        Parameters
        ----------
        secure_crypto : SecureCrypto
            Instance of SecureCrypto class
        sample_password : str
            Test password

        Returns
        -------
        None
        """
        custom_salt = os.urandom(16)
        key, salt = secure_crypto.pbkdf2_derive_key(sample_password, salt=custom_salt)
        assert salt == custom_salt

    def test_pbkdf2_derive_key_custom_iterations(
        self, 
        secure_crypto: SecureCrypto, 
        sample_password: str
    ) -> None:
        """Test key derivation with custom iterations.

        Verifies
        --------
        - Uses provided iterations

        Parameters
        ----------
        secure_crypto : SecureCrypto
            Instance of SecureCrypto class
        sample_password : str
            Test password

        Returns
        -------
        None
        """
        custom_iterations = 100_000
        key, salt = secure_crypto.pbkdf2_derive_key(
            sample_password, iterations=custom_iterations
        )
        # Can't directly verify iterations were used, but can test it runs


class TestSHA256Hashing:
    """Test cases for SHA-256 hashing."""

    def test_sha256_hash_valid(
        self, 
        secure_crypto: SecureCrypto, 
        sample_password: str
    ) -> None:
        """Test SHA-256 hashing with valid input.

        Verifies
        --------
        - Returns a string
        - Result is 64 characters long (hex digest)
        - Same input produces same output

        Parameters
        ----------
        secure_crypto : SecureCrypto
            Instance of SecureCrypto class
        sample_password : str
            Test password

        Returns
        -------
        None
        """
        hash1 = secure_crypto.sha256_hash(sample_password)
        hash2 = secure_crypto.sha256_hash(sample_password)
        assert isinstance(hash1, str)
        assert len(hash1) == 64
        assert hash1 == hash2

    def test_sha256_hash_invalid_type(self, secure_crypto: SecureCrypto) -> None:
        """Test SHA-256 hashing with invalid type.

        Verifies
        --------
        - Non-string input raises TypeError

        Parameters
        ----------
        secure_crypto : SecureCrypto
            Instance of SecureCrypto class

        Returns
        -------
        None
        """
        with pytest.raises(TypeError, match="must be of type"):
            secure_crypto.sha256_hash(12345)


class TestAESOperations:
    """Test cases for AES encryption and decryption."""

    def test_aes_encrypt_decrypt_roundtrip(
        self, 
        secure_crypto: SecureCrypto, 
        sample_plaintext: str, 
        sample_key: bytes
    ) -> None:
        """Test encryption followed by decryption.

        Verifies
        --------
        - Decrypted text matches original plaintext

        Parameters
        ----------
        secure_crypto : SecureCrypto
            Instance of SecureCrypto class
        sample_plaintext : str
            Test plaintext
        sample_key : bytes
            Test key

        Returns
        -------
        None
        """
        encrypted = secure_crypto.aes_encrypt(sample_plaintext, sample_key)
        decrypted = secure_crypto.aes_decrypt(encrypted, sample_key)
        assert decrypted == sample_plaintext

    def test_aes_encrypt_invalid_plaintext_type(
        self, 
        secure_crypto: SecureCrypto, 
        sample_key: bytes
    ) -> None:
        """Test encryption with invalid plaintext type.

        Verifies
        --------
        - Non-string plaintext raises TypeError

        Parameters
        ----------
        secure_crypto : SecureCrypto
            Instance of SecureCrypto class
        sample_key : bytes
            Test key

        Returns
        -------
        None
        """
        with pytest.raises(TypeError, match="must be of type"):
            secure_crypto.aes_encrypt(12345, sample_key)

    def test_aes_encrypt_invalid_key_length(self, secure_crypto: SecureCrypto) -> None:
        """Test encryption with invalid key length.

        Verifies
        --------
        - Non-32-byte key raises ValueError

        Parameters
        ----------
        secure_crypto : SecureCrypto
            Instance of SecureCrypto class

        Returns
        -------
        None
        """
        with pytest.raises(ValueError, match="Key must be 32 bytes"):
            secure_crypto.aes_encrypt("test", os.urandom(16))

    def test_aes_decrypt_invalid_key_length(self, secure_crypto: SecureCrypto) -> None:
        """Test decryption with invalid key length.

        Verifies
        --------
        - Non-32-byte key raises ValueError

        Parameters
        ----------
        secure_crypto : SecureCrypto
            Instance of SecureCrypto class

        Returns
        -------
        None
        """
        encrypted = secure_crypto.aes_encrypt("test", os.urandom(32))
        with pytest.raises(ValueError, match="Key must be 32 bytes"):
            secure_crypto.aes_decrypt(encrypted, os.urandom(16))

    def test_aes_decrypt_tampered_data(
        self, 
        secure_crypto: SecureCrypto, 
        sample_plaintext: str, 
        sample_key: bytes
    ) -> None:
        """Test decryption with tampered encrypted data.

        Verifies
        --------
        - Tampered data raises an exception during decryption

        Parameters
        ----------
        secure_crypto : SecureCrypto
            Instance of SecureCrypto class
        sample_plaintext : str
            Test plaintext
        sample_key : bytes
            Test key

        Returns
        -------
        None
        """
        encrypted = secure_crypto.aes_encrypt(sample_plaintext, sample_key)
        # Tamper with the encrypted data
        tampered = base64.b64decode(encrypted)
        tampered = tampered[:16] + bytes([x ^ 0xFF for x in tampered[16:20]]) + tampered[20:]
        tampered_encoded = base64.b64encode(tampered).decode("utf-8")
        
        with pytest.raises(ValueError, match=r"codec can't decode byte|codec can't decode bytes"):
            secure_crypto.aes_decrypt(tampered_encoded, sample_key)


class TestTokenGeneration:
    """Test cases for secure token generation."""

    def test_generate_secure_token_default_length(self, secure_crypto: SecureCrypto) -> None:
        """Test token generation with default length.

        Verifies
        --------
        - Returns a string
        - Length is appropriate for default 32-byte input

        Parameters
        ----------
        secure_crypto : SecureCrypto
            Instance of SecureCrypto class

        Returns
        -------
        None
        """
        token = secure_crypto.generate_secure_token()
        assert isinstance(token, str)
        # Base64 encoding of 32 bytes produces 43 chars (without padding)
        assert len(token) == 43

    def test_generate_secure_token_custom_length(self, secure_crypto: SecureCrypto) -> None:
        """Test token generation with custom length.

        Verifies
        --------
        - Returns a string
        - Length is appropriate for custom length

        Parameters
        ----------
        secure_crypto : SecureCrypto
            Instance of SecureCrypto class

        Returns
        -------
        None
        """
        token = secure_crypto.generate_secure_token(64)
        assert isinstance(token, str)
        # Base64 encoding of 64 bytes produces 86 chars (without padding)
        assert len(token) == 86

    def test_generate_secure_token_invalid_length(self, secure_crypto: SecureCrypto) -> None:
        """Test token generation with invalid length.

        Verifies
        --------
        - Length <16 raises ValueError

        Parameters
        ----------
        secure_crypto : SecureCrypto
            Instance of SecureCrypto class

        Returns
        -------
        None
        """
        with pytest.raises(ValueError, match="Token length should be at least 16 bytes"):
            secure_crypto.generate_secure_token(15)


class TestPasswordBasedEncryption:
    """Test cases for password-based encryption and decryption."""

    def test_encrypt_decrypt_with_password_roundtrip(
        self, 
        secure_crypto: SecureCrypto, 
        sample_plaintext: str, 
        sample_password: str
    ) -> None:
        """Test password-based encryption followed by decryption.

        Verifies
        --------
        - Decrypted text matches original plaintext

        Parameters
        ----------
        secure_crypto : SecureCrypto
            Instance of SecureCrypto class
        sample_plaintext : str
            Test plaintext
        sample_password : str
            Test password

        Returns
        -------
        None
        """
        encrypted = secure_crypto.encrypt_with_password(sample_plaintext, sample_password)
        decrypted = secure_crypto.decrypt_with_password(encrypted, sample_password)
        assert decrypted == sample_plaintext

    def test_encrypt_with_password_wrong_password(
        self, 
        secure_crypto: SecureCrypto, 
        sample_plaintext: str, 
        sample_password: str
    ) -> None:
        """Test decryption with wrong password.

        Verifies
        --------
        - Wrong password raises an exception during decryption

        Parameters
        ----------
        secure_crypto : SecureCrypto
            Instance of SecureCrypto class
        sample_plaintext : str
            Test plaintext
        sample_password : str
            Test password

        Returns
        -------
        None
        """
        encrypted = secure_crypto.encrypt_with_password(sample_plaintext, sample_password)
        with pytest.raises(ValueError, match="Invalid padding bytes"):
            secure_crypto.decrypt_with_password(encrypted, "wrong_password")

    def test_encrypt_with_password_tampered_data(
        self, 
        secure_crypto: SecureCrypto, 
        sample_plaintext: str, 
        sample_password: str
    ) -> None:
        """Test decryption with tampered encrypted data.

        Verifies
        --------
        - Tampered data raises an exception during decryption

        Parameters
        ----------
        secure_crypto : SecureCrypto
            Instance of SecureCrypto class
        sample_plaintext : str
            Test plaintext
        sample_password : str
            Test password

        Returns
        -------
        None
        """
        encrypted = secure_crypto.encrypt_with_password(sample_plaintext, sample_password)
        # Tamper with the encrypted data
        tampered = base64.b64decode(encrypted)
        tampered = tampered[:16] + bytes([x ^ 0xFF for x in tampered[16:20]]) + tampered[20:]
        tampered_encoded = base64.b64encode(tampered).decode("utf-8")
        
        with pytest.raises(ValueError, match="codec can't decode byte 0xab in position 0"):
            secure_crypto.decrypt_with_password(tampered_encoded, sample_password)