# RDB File Format Overview

RDB (Redis Database) files are used by Redis to persist data on disk. This document provides an overview of the RDB file format, detailing its various sections and encoding methods.

## Sections of the RDB File

An RDB file is composed of the following sections, in order:

1. **Header Section** (9 letters in hex, reading 2digits at a time)
2. **Metadata Section** (FA - Indicates the start of a metadata subsection.)
3. **Database Section** (FE - Indicates the start of a database subsection.)
4. **End of File Section**

RDB files utilize special encodings to store different types of data. The relevant encodings for this overview are "size encoding" and "string encoding." These are explained towards the end of this document.

The breakdown below is based on [Redis RDB File Format by Jan-Erik Rediger](https://github.com/sripathikrishnan/redis-rdb-tools). Only the parts relevant to this stage are included.

---

## Header Section

RDB files begin with a header section that identifies the file format and version. An example of a header section is:

```
52 45 44 49 53 30 30 31 31  // Magic string + version number (ASCII): "REDIS0011".
```

- **Magic String:** `REDIS`
- **Version Number:** `0011` (indicating RDB version 11)

**Note:** In this challenge, all test RDB files use version 11, so the header is always `REDIS0011`.

---

## Metadata Section

Following the header is the metadata section, which contains zero or more metadata subsections. Each subsection specifies a single metadata attribute.

### Example Metadata Subsection

```
FA                             // Indicates the start of a metadata subsection.
09 72 65 64 69 73 2D 76 65 72  // Metadata attribute name (string encoded): "redis-ver".
06 36 2E 30 2E 31 36           // Metadata attribute value (string encoded): "6.0.16".
```

- **FA:** Start of metadata subsection
- **Metadata Name:** "redis-ver" (string encoded)
- **Metadata Value:** "6.0.16" (string encoded)

**Note:** Both the metadata name and value are always string encoded.

---

## Database Section

The database section contains zero or more database subsections, each describing a single database.

### Example Database Subsection

```
FE                       // Indicates the start of a database subsection.
00                       // Database index (size encoded): 0.

FB                       // Indicates that hash table size information follows.
03                       // Size of the key-value hash table (size encoded): 3.
02                       // Size of the expires hash table (size encoded): 2.
00                       // 1-byte flag specifying value type and encoding: 0 (string).

06 66 6F 6F 62 61 72     // Key name (string encoded): "foobar".
06 62 61 7A 71 75 78     // Value (string encoded): "bazqux".

FC                       // Indicates that the key "foo" has an expire timestamp in milliseconds.
15 72 E7 07 8F 01 00 00  // Expire timestamp (8-byte unsigned long, little-endian): 1713824559637.

00                       // Value type: string.
03 66 6F 6F              // Key name: "foo".
03 62 61 72              // Value: "bar".

FD                       // Indicates that the key "baz" has an expire timestamp in seconds.
52 ED 2A 66              // Expire timestamp (4-byte unsigned integer, little-endian): 1714089298.

00                       // Value type: string.
03 62 61 7A              // Key name: "baz".
03 71 75 78              // Value: "qux".
```

### Key-Value Pair Storage

Each key-value pair is stored as follows:

1. **Optional Expire Information:**
   - **Timestamp in Seconds:**
     ```
     FD
     Expire timestamp in seconds (4-byte unsigned integer)
     ```
   - **Timestamp in Milliseconds:**
     ```
     FC
     Expire timestamp in milliseconds (8-byte unsigned long)
     ```
2. **Value Type:** 1-byte flag indicating the type and encoding of the value.
3. **Key:** String encoded.
4. **Value:** Encoding depends on the value type.

---

## End of File Section

This section marks the end of the RDB file and includes a checksum.

### Example End of File Section

```
FF                       // Indicates the end of the file.
89 3b b7 4e f8 0f 77 19  // 8-byte CRC64 checksum of the entire file.
```

---

## Size Encoding

Size-encoded values specify the size of various elements within the RDB file, such as database indexes and hash table sizes. String encoding begins with a size-encoded value indicating the number of characters in the string.

### Encoding Scheme

The first two bits of a size-encoded value determine how the value should be parsed:

- **0b00 (0x00):**
  - **Size:** Remaining 6 bits of the byte.
  - **Example:**
    ```
    0A
    00001010  // Size is 10
    ```

- **0b01 (0x40):**
  - **Size:** Next 14 bits (remaining 6 bits in the first byte combined with the next byte), in big-endian.
  - **Example:**
    ```
    42 BC
    01000010 10111100  // Size is 700
    ```

- **0b10 (0x80):**
  - **Size:** Next 4 bytes, in big-endian.
  - **Example:**
    ```
    80 00 00 42 68
    10000000 00000000 00000000 01000010 01101000  // Size is 17000
    ```

- **0b11 (0xC0):**
  - **Type of String Encoding:** Remaining 6 bits specify the string type.
  - **Refer to the String Encoding section for details.**

---

## String Encoding

A string-encoded value consists of two parts:

1. **Size of the String:** Size-encoded.
2. **String Content:** The actual string data.

### Examples

- **Simple String:**
  ```
  0D 48 65 6C 6C 6F 2C 20 57 6F 72 6C 64 21
  // Size: 13 ("Hello, World!")
  ```

- **String Encoded as 8-bit Integer:**
  ```
  C0 7B
  // String: "123" (0x7B represents the integer 123)
  ```

- **String Encoded as 16-bit Integer:**
  ```
  C1 39 30
  // String: "12345" (0x3039 in little-endian represents 12345)
  ```

- **String Encoded as 32-bit Integer:**
  ```
  C2 87 D6 12 00
  // String: "1234567" (0x0012D687 in little-endian represents 1234567)
  ```

- **LZF Compressed String:**
  ```
  C3 ...
  // Compressed with the LZF algorithm (not encountered in this challenge)
  ```

---

## The KEYS Command

The `KEYS` command in Redis returns all keys that match a given pattern as a RESP array.

### Examples

- **Set Keys:**
  ```sh
  $ redis-cli SET foo bar
  OK
  $ redis-cli SET baz qux
  OK
  ```

- **Retrieve Keys Matching Pattern "f*":**
  ```sh
  $ redis-cli KEYS "f*"
  1) "foo"
  ```

- **Retrieve All Keys (Pattern "*"):**
  ```sh
  $ redis-cli KEYS "*"
  1) "baz"
  2) "foo"
  ```

### Implementation Requirement

In this stage, you must add support for the `KEYS` command. However, only the `*` pattern needs to be supported.

---

## Tests

The tester will create an RDB file with a single key and execute your program as follows:

```sh
$ ./your_program.sh --dir <dir> --dbfilename <filename>
```

Then, it will send a `KEYS "*"` command to your server:

```sh
$ redis-cli KEYS "*"
```

**Expected Response:**

Your server must respond with a RESP array containing the key from the RDB file:

```
*1\r\n$3\r\nfoo\r\n
```

---

# Summary

This document provides an overview of the Redis RDB file format, detailing its sections, encoding methods, and the implementation requirements for supporting the `KEYS` command with the `*` pattern. Understanding this structure is essential for parsing RDB files and implementing Redis-compatible functionality.