use serde::{Serialize, de::DeserializeOwned};

use crate::api::{DurableTaskError, Result};

/// Default maximum JSON payload size (64 MiB).
#[cfg(test)]
pub const DEFAULT_MAX_JSON_SIZE: usize = 64 * 1024 * 1024;

/// Serialise a value to a JSON string, returning `Ok(None)` for unit `()` types.
pub fn to_json<T: Serialize>(value: &T) -> Result<Option<String>> {
    let json = serde_json::to_string(value)?;
    // serde_json serialises () as "null"
    if json == "null" {
        Ok(None)
    } else {
        Ok(Some(json))
    }
}

/// Check that a JSON string does not exceed the given size limit.
fn check_json_size(s: &str, max_size: usize) -> Result<()> {
    if s.len() > max_size {
        return Err(DurableTaskError::Other(format!(
            "JSON payload size {} exceeds maximum of {} bytes",
            s.len(),
            max_size
        )));
    }
    Ok(())
}

/// Deserialise from an optional JSON string.
///
/// If `json` is `None`, deserialises from `"null"` (which works for `Option<T>`, `()`, etc.).
pub fn from_json<T: DeserializeOwned>(json: Option<&str>, max_json_size: usize) -> Result<T> {
    let s = json.unwrap_or("null");
    check_json_size(s, max_json_size)?;
    let value = serde_json::from_str(s)?;
    Ok(value)
}

/// Deserialise from an optional JSON string, returning `Ok(None)` if input is `None`.
#[cfg(test)]
pub fn from_json_optional<T: DeserializeOwned>(
    json: Option<&str>,
    max_json_size: usize,
) -> Result<Option<T>> {
    match json {
        None => Ok(None),
        Some(s) => {
            check_json_size(s, max_json_size)?;
            let value = serde_json::from_str(s)?;
            Ok(Some(value))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_to_json_unit() {
        let result = to_json(&()).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_to_json_string() {
        let result = to_json(&"hello").unwrap();
        assert_eq!(result, Some("\"hello\"".to_string()));
    }

    #[test]
    fn test_to_json_struct() {
        #[derive(serde::Serialize)]
        struct Foo {
            x: i32,
        }
        let result = to_json(&Foo { x: 42 }).unwrap();
        assert_eq!(result, Some("{\"x\":42}".to_string()));
    }

    #[test]
    fn test_from_json_string() {
        let result: String = from_json(Some("\"hello\""), DEFAULT_MAX_JSON_SIZE).unwrap();
        assert_eq!(result, "hello");
    }

    #[test]
    fn test_from_json_none_to_unit() {
        let result: () = from_json(None, DEFAULT_MAX_JSON_SIZE).unwrap();
        assert_eq!(result, ());
    }

    #[test]
    fn test_from_json_optional_none() {
        let result: Option<String> =
            from_json_optional::<String>(None, DEFAULT_MAX_JSON_SIZE).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_from_json_optional_some() {
        let result: Option<i32> = from_json_optional(Some("42"), DEFAULT_MAX_JSON_SIZE).unwrap();
        assert_eq!(result, Some(42));
    }

    #[test]
    fn test_from_json_rejects_oversized_payload() {
        let big = "a".repeat(DEFAULT_MAX_JSON_SIZE + 1);
        let result: crate::api::Result<String> = from_json(Some(&big), DEFAULT_MAX_JSON_SIZE);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("exceeds maximum"));
    }

    #[test]
    fn test_from_json_optional_rejects_oversized_payload() {
        let big = "a".repeat(DEFAULT_MAX_JSON_SIZE + 1);
        let result: crate::api::Result<Option<String>> =
            from_json_optional(Some(&big), DEFAULT_MAX_JSON_SIZE);
        assert!(result.is_err());
    }

    #[test]
    fn test_from_json_custom_max_size() {
        let result: crate::api::Result<String> = from_json(Some("\"ab\""), 2);
        assert!(result.is_err());
        let result: crate::api::Result<String> = from_json(Some("\"a\""), 3);
        assert!(result.is_ok());
    }
}
