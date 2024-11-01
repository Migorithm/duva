// SIZE ENCODING
// Size-encoded values specify the size of something. Here are some examples:
// The database indexes and hash table sizes are size encoded.
// String encoding begins with a size-encoded value that specifies the number of characters in the string.
// List encoding begins with a size-encoded value that specifies the number of elements in the list.
// The first two bits of a size-encoded value indicate how the value should be parsed. Here's a guide (bits are shown in both hexadecimal and binary):
/*
    If the first two bits are 0b00:
    The size is the remaining 6 bits of the byte.
    In this example, the size is 10:
    0A
    00001010

    If the first two bits are 0b01:
    The size is the next 14 bits
    (remaining 6 bits in the first byte, combined with the next byte),
    in big-endian (read left-to-right).
    In this example, the size is 700:
    42 BC
    01000010 10111100

    If the first two bits are 0b10:
    Ignore the remaining 6 bits of the first byte.
    The size is the next 4 bytes, in big-endian (read left-to-right).
    In this example, the size is 17000:
    80 00 00 42 68
    10000000 00000000 00000000 01000010 01101000

    If the first two bits are 0b11:
    The remaining 6 bits specify a type of string encoding.
    See string encoding section.
*/
