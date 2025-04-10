use std::ffi::OsString;

pub fn get_file_type(file_name: &OsString) -> u8 {
    if let Some(ext) = file_name
        .as_os_str()
        .to_str()
        .and_then(|name| name.rsplit_once('.'))
        .map(|(_, ext)| ext) {
            return get_file_type_from_extension(ext);
    }

    return 10;
}

pub fn get_file_type_from_extension(ext: &str) -> u8 {
    match ext {
        "gif" => return 0,
        "jpeg" => return 1,
        "mpeg" => return 2,
        "c" => return 3,
        _ => return 10,
    }
}