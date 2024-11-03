use std::str;
fn string_encode(data: &[u8]) -> Option<Vec<u8>> {
    let s = match str::from_utf8(data) {
        Ok(s) => s,
        Err(_) => return None, 
    };

    if s.chars().all(|c| c.is_ascii_digit()) {
        if let Ok(num) = s.parse::<u32>() {
            if num <= 0xFF {
                // 8비트 정수
                return Some(vec![0xC0, num as u8]);
            } else if num <= 0xFFFF {
                // 16비트 정수 (리틀 엔디안)
                return Some(vec![
                    0xC1,
                    (num & 0xFF) as u8,
                    ((num >> 8) & 0xFF) as u8,
                ]);
            } else if num <= 0xFFFF_FFFF {
                return Some(vec![
                    0xC2,
                    (num & 0xFF) as u8, // num & 00000000 00000000 00000000 11111111
                    ((num >> 8) & 0xFF) as u8, // (num >> 8) & 00000000 00000000 11111111 00000000
                    ((num >> 16) & 0xFF) as u8, // (num >> 16) & 00000000 11111111 00000000 00000000
                    ((num >> 24) & 0xFF) as u8,  // (num >> 24) & 11111111 00000000 00000000 00000000
                ]);
            } 
        }
    }

    let length = data.len();
    if length > 0xFF {
        return None;
    }
    let mut encoded = Vec::with_capacity(1 + length);
    encoded.push(length as u8); 
    encoded.extend_from_slice(data);
    Some(encoded)
}

fn string_decode(data: &[u8]) -> Option<String> {
    if data.is_empty() {
        return Some(String::new());
    }

    let header = data[0];
    let content = &data[1..];

    match header {
        0xC0 => {
            // 8비트 정수
            if content.len() < 1 {
                return None;
            }
            let num = content[0];
            Some(num.to_string())
        }
        0xC1 => {
            // 16비트 정수 (리틀 엔디안)
            if content.len() < 2 {
                return None;
            }
            let num = u16::from_le_bytes([content[0], content[1]]);
            Some(num.to_string())
        }
        0xC2 => {
            // 32비트 정수 (리틀 엔디안)
            if content.len() < 4 {
                return None;
            }
            let num = u32::from_le_bytes([content[0], content[1], content[2], content[3]]);
            Some(num.to_string())
        }
        size => {
            // 단순 문자열
            let size = size as usize;
            if content.len() < size {
                return None;
            }
            let string_bytes = &content[..size];
            match str::from_utf8(string_bytes) {
                Ok(s) => Some(s.to_string()),
                Err(_) => None, 
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 테스트 케이스를 실행하여 인코딩 및 디코딩을 검증합니다.
    fn test_encode_decode(input: &[u8], expected_encoded: Option<&[u8]>) {
        // 인코딩 테스트
        let encoded = string_encode(input);
        assert_eq!(
            encoded.as_deref(),
            expected_encoded,
            "Encoding mismatch for input '{:?}'",
            input
        );

        // 디코딩 테스트
        if let Some(encoded_data) = encoded {
            let decoded = string_decode(&encoded_data);
            // 입력 데이터가 유효한 UTF-8인지 확인
            if let Ok(original_str) = str::from_utf8(input) {
                if original_str.chars().all(|c| c.is_ascii_digit()) {
                    // 숫자 문자열인 경우, 디코딩된 문자열도 동일해야 함
                    let num_str = original_str.to_string();
                    assert_eq!(
                        decoded,
                        Some(num_str),
                        "Decoding mismatch for input '{:?}'",
                        input
                    );
                } else {
                    // 비숫자 데이터인 경우, 디코딩된 문자열이 원본과 동일해야 함
                    assert_eq!(
                        decoded,
                        Some(original_str.to_string()),
                        "Decoding mismatch for input '{:?}'",
                        input
                    );
                }
            } else {
                // 유효하지 않은 UTF-8인 경우 디코딩이 실패해야 함
                assert_eq!(
                    decoded, None,
                    "Decoding should fail for invalid UTF-8 input '{:?}'",
                    input
                );
            }
        } else {
            assert_eq!(expected_encoded, None, "Expected encoding to fail for input '{:?}'", input);
        }
    }

    /// 단순 문자열 인코딩 및 디코딩 테스트
    #[test]
    fn test_simple_string_encode_decode() {
        // "Hello, World!" -> [0x0D, 'H', 'e', 'l', 'l', 'o', ',', ' ', 'W', 'o', 'r', 'l', 'd', '!']
        let input = b"Hello, World!";
        let expected = Some(&[
            0x0D,
            0x48, 0x65, 0x6C, 0x6C, 0x6F, 0x2C, 0x20,
            0x57, 0x6F, 0x72, 0x6C, 0x64, 0x21,
        ][..]);
        test_encode_decode(input, expected);
    }

    /// 빈 문자열 인코딩 및 디코딩 테스트
    #[test]
    fn test_empty_string_encode_decode() {
        // "" -> [0x00]
        let input = b"";
        let expected = Some(&[0x00][..]);
        test_encode_decode(input, expected);
    }

    /// 최대 길이(255) 데이터 인코딩 및 디코딩 테스트
    #[test]
    fn test_max_length_encode_decode() {
        // 데이터 길이가 255인 경우
        let input = vec![0x41; 255]; // 'A' * 255
        let mut expected = Vec::with_capacity(256);
        expected.push(255);
        expected.extend_from_slice(&input);
        test_encode_decode(&input, Some(&expected[..]));
    }

    /// 초과 길이(256) 데이터 인코딩 실패 테스트
    #[test]
    fn test_over_max_length_encode() {
        // 데이터 길이가 256을 초과하면 인코딩 실패 (None 반환)
        let input = vec![0x61; 256]; // 'a' * 256
        let encoded = string_encode(&input);
        assert_eq!(encoded, None, "Encoding should fail for data length > 255");
    }

    /// 유효하지 않은 UTF-8 데이터 인코딩 및 디코딩 테스트
    #[test]
    fn test_non_utf8_data_encode_decode() {
        // 유효하지 않은 UTF-8 시퀀스
        let input = vec![0xFF, 0xFF, 0xFF];
        let expected = Some(&[0x03, 0xFF, 0xFF, 0xFF][..]);
        test_encode_decode(&input, expected);
    }

    /// 유효하지 않은 UTF-8 데이터 디코딩 실패 테스트
    #[test]
    fn test_decode_invalid_utf8() {
        // 인코딩된 데이터가 유효한 UTF-8이 아니면 디코딩 실패 (None 반환)
        let encoded = vec![0x03, 0xFF, 0xFF, 0xFF];
        let decoded = string_decode(&encoded);
        assert_eq!(decoded, None, "Decoding should fail for invalid UTF-8");
    }

    /// 불완전한 인코딩 데이터 디코딩 실패 테스트
    #[test]
    fn test_incomplete_encoding() {
        // 인코딩이 불완전한 경우 (예: 길이가 5인데 데이터가 3바이트만 있는 경우)
        let encoded = vec![0x05, 0x48, 0x65, 0x6C];
        let decoded = string_decode(&encoded);
        assert_eq!(decoded, None, "Decoding should fail for incomplete data");
    }

    /// 길이만 있고 데이터가 없는 경우 디코딩 실패 테스트
    #[test]
    fn test_only_length_byte() {
        // 길이만 있고 데이터가 없는 경우
        let encoded = vec![0x02];
        let decoded = string_decode(&encoded);
        assert_eq!(decoded, None, "Decoding should fail for missing data bytes");
    }

    /// 길이가 0인 경우 디코딩 테스트
    #[test]
    fn test_valid_utf8_with_length_zero() {
        // 길이가 0인 경우, 빈 문자열 반환
        let encoded = vec![0x00];
        let decoded = string_decode(&encoded);
        assert_eq!(decoded, Some(String::new()), "Decoding should return empty string");
    }

    /// 유니코드 문자열 인코딩 및 디코딩 테스트
    #[test]
    fn test_non_ascii_string_encode_decode() {
        // 유니코드 문자열 "こんにちは" (Hello in Japanese)
        let input = "こんにちは".as_bytes();
        let expected_length = input.len() as u8;
        let mut expected = Vec::with_capacity(1 + input.len());
        expected.push(expected_length);
        expected.extend_from_slice(input);
        test_encode_decode(input, Some(&expected[..]));
    }

    /// 정수 인코딩 데이터 디코딩 테스트
    #[test]
    fn test_integer_encoded_data_decode() {
        // 0xC0: 8-bit integer "123" -> [0xC0, 0x7B]
        let encoded_c0 = vec![0xC0, 0x7B];
        let decoded_c0 = string_decode(&encoded_c0);
        assert_eq!(decoded_c0, Some("123".to_string()), "Decoding mismatch for C0 encoded data");

        // 0xC1: 16-bit integer "12345" -> [0xC1, 0x39, 0x30]
        let encoded_c1 = vec![0xC1, 0x39, 0x30];
        let decoded_c1 = string_decode(&encoded_c1);
        assert_eq!(decoded_c1, Some("12345".to_string()), "Decoding mismatch for C1 encoded data");

        // 0xC2: 32-bit integer "1234567" -> [0xC2, 0x87, 0xD6, 0x12, 0x00]
        let encoded_c2 = vec![0xC2, 0x87, 0xD6, 0x12, 0x00];
        let decoded_c2 = string_decode(&encoded_c2);
        assert_eq!(decoded_c2, Some("1234567".to_string()), "Decoding mismatch for C2 encoded data");

        // 0xC0: 최대 8-bit 정수 "255" -> [0xC0, 0xFF]
        let encoded_c0_max = vec![0xC0, 0xFF];
        let decoded_c0_max = string_decode(&encoded_c0_max);
        assert_eq!(decoded_c0_max, Some("255".to_string()), "Decoding mismatch for C0 max encoded data");

        // 0xC1: 최대 16-bit 정수 "65535" -> [0xC1, 0xFF, 0xFF]
        let encoded_c1_max = vec![0xC1, 0xFF, 0xFF];
        let decoded_c1_max = string_decode(&encoded_c1_max);
        assert_eq!(decoded_c1_max, Some("65535".to_string()), "Decoding mismatch for C1 max encoded data");

        // 0xC2: 최대 32-bit 정수 "4294967295" -> [0xC2, 0xFF, 0xFF, 0xFF, 0xFF]
        let encoded_c2_max = vec![0xC2, 0xFF, 0xFF, 0xFF, 0xFF];
        let decoded_c2_max = string_decode(&encoded_c2_max);
        assert_eq!(decoded_c2_max, Some("4294967295".to_string()), "Decoding mismatch for C2 max encoded data");
    }

    /// 정수 인코딩에서 불완전한 데이터 디코딩 실패 테스트
    #[test]
    fn test_incomplete_integer_encoding() {
        // 0xC1: 16-bit 정수 인코딩 시 바이트가 부족한 경우
        let incomplete_c1 = vec![0xC1, 0x39];
        let decoded = string_decode(&incomplete_c1);
        assert_eq!(decoded, None, "Decoding should fail for incomplete C1 encoded data");

        // 0xC2: 32-bit 정수 인코딩 시 바이트가 부족한 경우
        let incomplete_c2 = vec![0xC2, 0x87, 0xD6];
        let decoded = string_decode(&incomplete_c2);
        assert_eq!(decoded, None, "Decoding should fail for incomplete C2 encoded data");
    }

    /// C0, C1, C2 이외의 접두사를 가진 데이터 디코딩 테스트
    #[test]
    fn test_decode_other_prefixes() {
        // [0x05, 'H', 'e', 'l', 'l', 'o'] -> "Hello"
        let encoded = vec![0x05, 0x48, 0x65, 0x6C, 0x6C, 0x6F];
        let decoded = string_decode(&encoded);
        assert_eq!(decoded, Some("Hello".to_string()), "Decoding mismatch for simple string");

        // 길이만큼의 데이터가 부족한 경우
        let incomplete = vec![0x05, 0x48, 0x65];
        let decoded = string_decode(&incomplete);
        assert_eq!(decoded, None, "Decoding should fail for incomplete simple string data");
    }
}
