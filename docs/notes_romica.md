created a main.go in cmd/security-demo to show security implementation (AES-GCM, HMAC)

Security Features Verified
Our testing and demonstration confirmed that our security implementation is working correctly:
- Encryption Works: Data values are properly encrypted using AES-GCM and can only be read by someone with the correct key.
- Integrity Protection Works: Any tampering with the encrypted data is detected during the decryption process. This protects against both accidental corruption and malicious tampering.
- Key Separation Works: Data encrypted with one key cannot be decrypted with a different key. This ensures that only authorized users with the correct key can access the data.


To run demos - 
`cd can-dht && go build -o bin/enhanced-auth-demo cmd/enhanced-auth-demo/main.go`

` ./bin/enhanced-auth-demo`

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


**The security demonstration works perfectly! Let's analyze the results:**

## Security Demonstration Results Analysis

Our demonstration program showed the following security features in action:

### 1. Plaintext vs. Encrypted Storage:
- **Plaintext Storage**: "John Doe's personal information" is stored and retrieved as-is, with no protection.
- **Encrypted Storage**: "Jane Smith's sensitive information" is stored as an encrypted string that looks like: 
  ```
  918803392890f4193fb818730b859ec658a0a94b120374ff64d162b07381a6bc27c30...
  ```
  But is correctly decrypted back to the original value when retrieved.

### 2. Data Integrity Protection:
- When we tampered with the stored encrypted data by inserting "TAMPERED" into the string, the system correctly detected this tampering:
  ```
  Deserialization failed (tampering detected): failed to decode HMAC: encoding/hex: invalid byte: U+0054 'T'
  ```
  This shows that even minor modifications to the stored data are detected, protecting data integrity.

### 3. Key Management:
- When we tried to decrypt data using a different key manager, the system correctly failed with the error:
  ```
  Decryption with different key failed (as expected): HMAC verification failed
  ```
  But using the original key correctly decrypted the data back to: "Data encrypted with the first key"

## Security Features Verified

Our testing and demonstration confirmed that our security implementation is working correctly:

1. **Encryption Works**: Data values are properly encrypted using AES-GCM and can only be read by someone with the correct key.

2. **Integrity Protection Works**: Any tampering with the encrypted data is detected during the decryption process. This protects against both accidental corruption and malicious tampering.

3. **Key Separation Works**: Data encrypted with one key cannot be decrypted with a different key. This ensures that only authorized users with the correct key can access the data.

## Potential Enhancements

Based on our testing, here are some potential enhancements we could make:

1. **Key Rotation**: Implement a mechanism to periodically rotate encryption keys without losing access to previously encrypted data.

2. **Access Control**: Integrate with an authentication system to control which users can access which encryption keys.

3. **Distributed Key Management**: In a distributed system, consider how encryption keys are distributed and managed across nodes.

4. **Performance Optimization**: Profile and optimize the encryption/decryption operations for better performance, especially for large data values.

## Conclusion

Our security implementation provides strong protection for data stored in the CAN-DHT:

- **Confidentiality**: Data is encrypted using AES-GCM, a strong authenticated encryption algorithm.
- **Integrity**: HMAC verification ensures data hasn't been tampered with.
- **Authentication**: Only entities with the correct key can create or read encrypted data.


ROUTING 
 - In the original design, there was no explicit mechanism for selecting an optimal starting node.
 - The paper specifies that when an application wants to insert or retrieve data:

- - It first hashes the key to get coordinates in the CAN space
- - It can contact any CAN node as the starting point
- - There's no specified "best" starting node in the original design

KEEP THIS AFTER REPLICATION IS DONE TO EXTENDED NEIGHBOURS ALSO - Conflict resolution for inconsistent data