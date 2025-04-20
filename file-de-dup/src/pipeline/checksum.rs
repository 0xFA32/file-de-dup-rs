use std::{ffi::OsString, fs::File, io::{BufReader, Read}, sync::{Arc, Mutex}};

use crossbeam_channel::{Receiver, Sender};
use dashmap::DashMap;
use rayon::iter::IntoParallelIterator;
use crc::{Crc, CRC_64_ECMA_182};
use crate::report::Report;
use rayon::prelude::*;
use super::executor::{AggregateFiles, AggregatedFilesChecksum, PipelineStage};

// Aggregate these many files before sending to the next stage.
// Set it to usize::MAX to aggregate all of the files before sending
// to the next stage.
#[allow(clippy::absurd_extreme_comparisons)]
const AGGREGATE_LIMIT: usize = usize::MAX;

pub struct Checksum {
    num_threads: usize,
    do_full_comparison: bool,
    prev_stage_channel: Receiver<Arc<AggregateFiles>>,
    next_stage_channel: Sender<Arc<AggregatedFilesChecksum>>,
    report: Arc<Mutex<Report>>,
    aggregated_files_checksum: DashMap<(u64, usize), Vec<Arc<OsString>>>,
}

impl Checksum {
    pub fn new(
        num_threads: usize,
        do_full_comparison: bool,
        prev_stage_channel: Receiver<Arc<AggregateFiles>>,
        next_stage_channel: Sender<Arc<AggregatedFilesChecksum>>,
        report: Arc<Mutex<Report>>,
    ) -> Checksum {
        Self {  
            num_threads,
            do_full_comparison,
            prev_stage_channel,
            next_stage_channel,
            report,
            aggregated_files_checksum: DashMap::new(),
        }
    }

    fn calculate_checksum(&mut self, aggregated_files: &Vec<Arc<AggregateFiles>>) {
        aggregated_files.into_par_iter().for_each(|aggregate_file| {
            aggregate_file.file_names.par_iter().for_each(|file| {
                let checksum = Self::calculate_checksum_file(file);
                if let Some(checksum) = checksum {
                    if AGGREGATE_LIMIT == usize::MAX || !self.do_full_comparison {
                        self.aggregated_files_checksum
                            .entry((checksum, aggregate_file.file_metdata.size))
                            .or_default()
                            .push(file.clone());
                    } else {
                        self.aggregated_files_checksum.entry((checksum, aggregate_file.file_metdata.size))
                            .and_modify(|v| {
                                if v.len() > AGGREGATE_LIMIT {
                                    let _ = self.next_stage_channel.send(Arc::new(AggregatedFilesChecksum { 
                                        checksum,
                                        file_names: std::mem::replace(v, vec![file.clone()]),
                                        file_size: aggregate_file.file_metdata.size,
                                    }));
                                } else {
                                    v.push(file.clone());
                                }
                            }).or_insert_with(|| vec![file.clone()]);
                    }
                }
            });
        });
    }

    fn calculate_checksum_file(file: &Arc<OsString>) -> Option<u64> {
        let file = File::open(file.as_os_str());
        if file.is_err() {
            return None;
        }

        let mut reader = BufReader::new(file.unwrap());
        let mut buffer = [0u8; 16384]; 

        let hasher = Crc::<u64>::new(&CRC_64_ECMA_182);
        let mut digest = hasher.digest();
        
        loop {
            match reader.read(&mut buffer) {
                Ok(0) => break,
                Ok(n) => digest.update(&buffer[..n]),
                Err(_) => break,
            }
        }

        Some(digest.finalize())
    }
}

impl PipelineStage for Checksum {
    fn execute(&mut self) {
        let aggregated_files: Vec<Arc<AggregateFiles>> = self.prev_stage_channel.try_iter().collect();
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(self.num_threads)
            .build()
            .unwrap();

        pool.install(|| Self::calculate_checksum(self, &aggregated_files));

        // Send remaining aggregated data to channel.
        let keys: Vec<(u64, usize)> = self
            .aggregated_files_checksum
            .iter()
            .map(|entry| *entry.key())
            .collect();

        for key in keys {
            if let Some((_, aggregated_files)) = self.aggregated_files_checksum.remove(&key) {
                if !self.do_full_comparison {
                    self.report.lock().unwrap().add(&aggregated_files);
                } else {
                    let _ = self.next_stage_channel.send(Arc::new(AggregatedFilesChecksum {
                        file_names: aggregated_files,
                        checksum: key.0,
                        file_size: key.1, 
                    }));
                }
            }
        }
    }
    
    fn is_completed(&self) -> bool {
        true
    } 
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::{Path, PathBuf};
    use crossbeam_channel::unbounded;
    use crate::pipeline::executor::{AggregateFiles, AggregatedFilesChecksum, FileMetadata, PipelineStage};
    use crate::report::Report;
    use crate::pipeline::util;
    use super::Checksum;
    use std::ffi::OsString;
    use std::sync::{Arc, Mutex};
    use std::collections::HashSet;

    #[test]
    fn checksum_same_file_test() {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("test_data/test_data");
        
        let file1 = root.join("test10.c");
        let file2 = root.join("test_5/test200.c");

        let file_type = util::get_file_type(&(file1.as_os_str().to_os_string()));
        let file_metadata = fs::metadata(&file1).unwrap();
        let report = Arc::new(Mutex::new(Report::new()));
        let (tx, rx) = unbounded::<Arc<AggregateFiles>>();
        let (ntx, nrx) = unbounded::<Arc<AggregatedFilesChecksum>>();

        let aggregated_files: AggregateFiles = AggregateFiles {
            file_metdata: Arc::new(FileMetadata { size: file_metadata.len() as usize, file_type: file_type }),
            file_names: vec![Arc::new(file1.as_os_str().to_os_string()), Arc::new(file2.as_os_str().to_os_string())]
        };

        let _ = tx.send(Arc::new(aggregated_files));

        let mut checksum = Checksum::new(1, true, rx, ntx, report);
        checksum.execute();

        let res: Vec<Arc<AggregatedFilesChecksum>> = nrx.try_iter().collect();
        assert_eq!(res.len(), 1);
        assert_eq!(res.get(0).unwrap().file_names.len(), 2);
        assert_eq!(res.get(0).unwrap().checksum, 5946100406913306610);
    }

    #[test]
    fn checksum_different_file_same_size_test() {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("test_data/test_data");
        
        let file1 = root.join("test10.c");
        let file2 = root.join("test.c");

        let file_type = util::get_file_type(&(file1.as_os_str().to_os_string()));
        let file_metadata = fs::metadata(&file1).unwrap();
        let report = Arc::new(Mutex::new(Report::new()));
        let (tx, rx) = unbounded::<Arc<AggregateFiles>>();
        let (ntx, nrx) = unbounded::<Arc<AggregatedFilesChecksum>>();

        let aggregated_files: AggregateFiles = AggregateFiles {
            file_metdata: Arc::new(FileMetadata { size: file_metadata.len() as usize, file_type: file_type }),
            file_names: vec![Arc::new(file1.as_os_str().to_os_string()), Arc::new(file2.as_os_str().to_os_string())]
        };

        let _ = tx.send(Arc::new(aggregated_files));

        let mut checksum = Checksum::new(1, true, rx, ntx, report);
        checksum.execute();

        let res: Vec<Arc<AggregatedFilesChecksum>> = nrx.try_iter().collect();
        assert_eq!(res.len(), 2);
        assert_eq!(res.get(0).unwrap().file_names.len(), 1);
        assert_eq!(res.get(1).unwrap().file_names.len(), 1);
        
        let checksums: Vec<u64> = res.iter().map(|f| f.checksum).collect();
        assert!(checksums.contains(&5946100406913306610));
        assert!(checksums.contains(&2608553799101011692));
    }

    #[test]
    fn checksum_different_file_different_size_test() {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("test_data/test_data");
        
        let file1 = root.join("test10.c");
        let file2 = root.join("test.c");

        let file_type = util::get_file_type(&(file1.as_os_str().to_os_string()));
        let file_metadata = fs::metadata(&file1).unwrap();
        let report = Arc::new(Mutex::new(Report::new()));
        let (tx, rx) = unbounded::<Arc<AggregateFiles>>();
        let (ntx, nrx) = unbounded::<Arc<AggregatedFilesChecksum>>();

        let aggregated_files: AggregateFiles = AggregateFiles {
            file_metdata: Arc::new(FileMetadata { size: file_metadata.len() as usize, file_type: file_type }),
            file_names: vec![Arc::new(file1.as_os_str().to_os_string())]
        };

        let aggregated_files1: AggregateFiles = AggregateFiles {
            file_metdata: Arc::new(FileMetadata { size: (file_metadata.len() as usize) + 100, file_type: file_type }),
            file_names: vec![Arc::new(file2.as_os_str().to_os_string())]
        };

        let _ = tx.send(Arc::new(aggregated_files));
        let _ = tx.send(Arc::new(aggregated_files1));

        let mut checksum = Checksum::new(1, true, rx, ntx, report);
        checksum.execute();

        let res: Vec<Arc<AggregatedFilesChecksum>> = nrx.try_iter().collect();
        assert_eq!(res.len(), 2);
        assert_eq!(res.get(0).unwrap().file_names.len(), 1);
        assert_eq!(res.get(1).unwrap().file_names.len(), 1);
        
        let checksums: Vec<u64> = res.iter().map(|f| f.checksum).collect();
        assert!(checksums.contains(&5946100406913306610));
        assert!(checksums.contains(&2608553799101011692));
    }    

    #[test]
    fn checksum_same_file_no_full_comparison_test() {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("test_data/test_data");
        
        let file1 = root.join("test10.c");
        let file2 = root.join("test_5/test200.c");

        let file_type = util::get_file_type(&(file1.as_os_str().to_os_string()));
        let file_metadata = fs::metadata(&file1).unwrap();
        let report = Arc::new(Mutex::new(Report::new()));
        let (tx, rx) = unbounded::<Arc<AggregateFiles>>();
        let (ntx, nrx) = unbounded::<Arc<AggregatedFilesChecksum>>();

        let aggregated_files: AggregateFiles = AggregateFiles {
            file_metdata: Arc::new(FileMetadata { size: file_metadata.len() as usize, file_type: file_type }),
            file_names: vec![Arc::new(file1.as_os_str().to_os_string()), Arc::new(file2.as_os_str().to_os_string())]
        };

        let _ = tx.send(Arc::new(aggregated_files));

        let mut checksum =
            Checksum::new(1, false, rx, ntx, report.clone());
        checksum.execute();

        let res: Vec<Arc<AggregatedFilesChecksum>> = nrx.try_iter().collect();
        assert_eq!(res.len(), 0);
        assert_eq!(report.lock().unwrap().get().len(), 1);
    }

    #[test]
    fn checksum_different_file_no_full_comparison_test() {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("test_data/test_data");
        
        let file1 = root.join("test10.c");
        let file2 = root.join("test_5/test200.c");

        let file_type = util::get_file_type(&(file1.as_os_str().to_os_string()));
        let file_metadata = fs::metadata(&file1).unwrap();
        let report = Arc::new(Mutex::new(Report::new()));
        let (tx, rx) = unbounded::<Arc<AggregateFiles>>();
        let (ntx, nrx) = unbounded::<Arc<AggregatedFilesChecksum>>();

        let aggregated_files: AggregateFiles = AggregateFiles {
            file_metdata: Arc::new(FileMetadata { size: file_metadata.len() as usize, file_type: file_type }),
            file_names: vec![Arc::new(file1.as_os_str().to_os_string()), Arc::new(file2.as_os_str().to_os_string())]
        };

        let _ = tx.send(Arc::new(aggregated_files));

        let mut checksum =
            Checksum::new(1, false, rx, ntx, report.clone());
        checksum.execute();

        let res: Vec<Arc<AggregatedFilesChecksum>> = nrx.try_iter().collect();
        assert_eq!(res.len(), 0);
        assert_eq!(report.lock().unwrap().get().len(), 1);
    }        
}