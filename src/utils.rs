//! Utilities for building command-line programs.

/// Returns the sha256 digest of the file at the given path *if it exists*.
/// If the file does _not_ exist it returns `Ok(None)`.
pub fn sha256_digest(contents: &[u8]) -> Option<String> {
    fn sha256<R: std::io::Read>(mut reader: R) -> ring::digest::Digest {
        let mut context = ring::digest::Context::new(&ring::digest::SHA256);
        let mut buffer = [0; 1024];

        loop {
            let count = reader.read(&mut buffer).unwrap();
            if count == 0 {
                break;
            }
            context.update(&buffer[..count]);
        }

        context.finish()
    }

    let reader = std::io::BufReader::new(contents);
    let digest = sha256(reader);
    Some(data_encoding::HEXUPPER.encode(digest.as_ref()))
}

/// Recursively get the files within a directory.
pub fn get_files(dir: impl AsRef<std::path::Path>) -> Vec<std::path::PathBuf> {
    log::info!("reading directory '{}'", dir.as_ref().display());
    if !(dir.as_ref().exists() && dir.as_ref().is_dir()) {
        log::error!(
            "'{}' does not exist, or is not a directory",
            dir.as_ref().display()
        );
        panic!("not a dir");
    }

    let mut files = vec![];
    for entry in std::fs::read_dir(dir).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        if path.is_file() {
            files.push(path);
        } else if path.is_dir() {
            files.extend(get_files(path));
        }
    }
    files
}
