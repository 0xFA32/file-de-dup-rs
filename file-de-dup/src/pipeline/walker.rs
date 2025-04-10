use super::executor::PipelineStage;
use crossbeam_channel::Sender;
use std::{ffi::OsString, fs, path::PathBuf, sync::Arc};
use rayon::prelude::*;

pub struct Walker {
    full_path: &'static str,
    recursive: bool,
    num_threads: usize,
    next_stage_channel: Sender<Arc<OsString>>
}

impl Walker {
    pub fn new(
        full_path: &'static str,
        recursive: bool,
        num_threads: usize,
        next_stage_channel: Sender<Arc<OsString>>,
    ) -> Walker {
        Self {
            full_path,
            recursive,
            num_threads,
            next_stage_channel
        }
    }

    fn walk_non_recursive(&self) {
        if let Ok(paths) = fs::read_dir(self.full_path) {
            for path in paths {
                if path.is_err() {
                    continue;
                }

                if let Ok(p) = path {
                    if let Ok(metadata) = fs::metadata(p.path()) {
                        if metadata.file_type().is_file() {
                            let res =
                            self
                                .next_stage_channel
                                .send(Arc::new(fs::canonicalize(p.path()).unwrap().into_os_string()));
                            if res.is_err() {
                                eprintln!("Error sending file name to next stage!");
                                return;
                            }
                        }
                    }
                }
            }
        } else {
            eprintln!("Error reading dir!");
        }
    }

    fn walk_recursive(&self, path_bufs: Vec<PathBuf>) {
        path_bufs.into_par_iter().for_each_with(self.next_stage_channel.clone(), |ch, p| {
            if let Ok(metadata) = fs::metadata(p.clone()) {
                if metadata.file_type().is_file() {
                    let res = ch.send(Arc::new(p.into_os_string()));
                    if res.is_err() {
                        eprintln!("Error sending file name to next stage!");
                    }
                } else {
                    if let Ok(paths) = fs::read_dir(p) {
                        let bufs: Vec<PathBuf> = paths
                            .filter_map(|entry| entry.ok())
                            .map(|entry| entry.path())
                            .collect();
    
                        self.walk_recursive(bufs);
                    }
                }
            }
        });
    }
}

impl PipelineStage for Walker {
    fn execute(&mut self) {
        // For simplicity just handle non-recursive case explicitly.
        if !self.recursive {
            Self::walk_non_recursive(&self);
            return;
        }

        let paths = fs::read_dir(self.full_path).unwrap();
        let path_bufs: Vec<PathBuf> = paths
            .filter_map(|entry| entry.ok())
            .map(|entry| entry.path())
            .collect();

        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(self.num_threads)
            .build()
            .unwrap();
        pool.install(|| Self::walk_recursive(&self, path_bufs));
    }
    
    fn is_completed(&self) -> bool {
        true
    }
}

mod tests {
    #[test]
    fn non_recursive_test() {

    }

    #[test]
    fn recursive_test() {
        
    }
}