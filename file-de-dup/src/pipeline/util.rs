use std::ffi::OsString;

/// Get file type.
pub fn get_file_type(file_name: &OsString) -> u8 {
    if let Some(ext) = file_name
        .as_os_str()
        .to_str()
        .and_then(|name| name.rsplit_once('.'))
        .map(|(_, ext)| ext) {
            return get_file_type_from_extension(ext);
    }

    10
}

/// Get file type from extension. Uses an internal u8 value to distinguish between various
/// file types.
pub fn get_file_type_from_extension(ext: &str) -> u8 {
    match ext {
        "gif" => 0,
        "jpeg" => 1,
        "mpeg" => 2,
        "c" => 3,
        _ => 10,
    }
}