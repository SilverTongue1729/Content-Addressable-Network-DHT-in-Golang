created a main.go in cmd/security-demo to show security implementation (AES-GCM, HMAC)

Security Features Verified
Our testing and demonstration confirmed that our security implementation is working correctly:
- Encryption Works: Data values are properly encrypted using AES-GCM and can only be read by someone with the correct key.
- Integrity Protection Works: Any tampering with the encrypted data is detected during the decryption process. This protects against both accidental corruption and malicious tampering.
- Key Separation Works: Data encrypted with one key cannot be decrypted with a different key. This ensures that only authorized users with the correct key can access the data.


# Access Control Mechanisms Implemented

1. **Key Namespacing**
   - Keys are prefixed with username (e.g., "alice:profile")
   - Prevents data collisions between users using the same key
   - Ensures data isolation and privacy between users

2. **Permission-Based Access Control**
   - Granular CRUD permissions using bitmasks:
     - PermissionRead: Ability to read data
     - PermissionCreate: Ability to create new data
     - PermissionUpdate: Ability to modify existing data
     - PermissionDelete: Ability to remove data
     - PermissionAdmin: Administrative privileges
     - PermissionAll: All permissions combined

3. **Owner-Based Access Control**
   - Data owners automatically receive all permissions
   - Only owners can modify permissions of other users by default

4. **User Authentication**
   - Password-based authentication system
   - All operations require valid username/password credentials
   - Prevents unauthorized access to the system

5. **Per-Data Item Access Lists**
   - Each data item tracks its own access permissions
   - Different users can have different permission levels for the same data

6. **Secure Key Management**
   - Each user has a unique encryption key
   - Data keys are encrypted with user-specific keys
   - Ensures that only authorized users can decrypt data

7. **Data Integrity Verification**
   - HMAC signatures verify data hasn't been tampered with
   - Ensures integrity of stored data

8. **Permission Modification Controls**
   - Only owners or users with admin permission can modify access permissions
   - Permissions can be granted, modified, or revoked

9. **Encrypted Storage**
   - All data is encrypted before storage
   - Protects confidentiality even if underlying storage is compromised

10. **Password Hashing**
    - Passwords are hashed rather than stored in plaintext
    - Protects user credentials even if the credential store is exposed


DO THIS - Periodic integrity checks
DEMOS FOR ROUTING 
KEEP THIS AFTER REPLICATION IS DONE TO EXTENDED NEIGHBOURS ALSO - Conflict resolution for inconsistent data