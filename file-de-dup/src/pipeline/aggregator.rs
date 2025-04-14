use std::{collections::HashSet, ffi::OsString, fs, sync::Arc};

use crossbeam_channel::{Receiver, Sender};
use dashmap::DashMap;
use rayon::prelude::*;
use super::{executor::{AggregateFiles, FileMetadata, PipelineStage}, util};

// Aggregate these many files before sending to the next stage.
// Set it to usize::MAX to aggregate all of the files before sending
// to the next stage.
const AGGREGATE_LIMIT: usize = 1;

pub struct Aggregator<'a> {
    filter_file_types: &'a Option<HashSet<u8>>,
    num_threads: usize,
    prev_stage_channel: Receiver<Arc<OsString>>,
    aggregated_files: DashMap<Arc<FileMetadata>, Vec<Arc<OsString>>>,
    next_stage_channel: Sender<Arc<AggregateFiles>>
}

impl<'a> Aggregator<'a> {
    pub fn new(
        filter_file_types: &'a Option<HashSet<u8>>,
        num_threads: usize,
        prev_stage_channel: Receiver<Arc<OsString>>,
        next_stage_channel: Sender<Arc<AggregateFiles>>,
    ) -> Aggregator<'a> {
        Self {
            filter_file_types,
            num_threads,
            prev_stage_channel,
            aggregated_files: DashMap::new(),
            next_stage_channel: next_stage_channel,
        }
    }

    fn aggregate(&mut self, paths: &Vec<Arc<OsString>>) {
        paths.into_par_iter().for_each(|p| {
            if let Ok(file_metadata) = fs::metadata(p.as_os_str()) {
                let file_type = util::get_file_type(p);

                // Skip files based on filter file type provided from user.
                if let Some(types) = self.filter_file_types {
                    if !types.contains(&file_type) {
                        return;    
                    }
                }

                let metadata = Arc::new(FileMetadata { 
                    size: file_metadata.len() as usize,
                    file_type: file_type,
                });
    
                if AGGREGATE_LIMIT == usize::MAX {
                    self.aggregated_files.entry(metadata)
                        .or_insert_with(Vec::new)
                        .push(p.clone());
                } else {
                    self.aggregated_files.entry(metadata.clone())
                        .and_modify(|v| {
                            if v.len() > AGGREGATE_LIMIT {
                                let _ = self.next_stage_channel.send(Arc::new(AggregateFiles { 
                                    file_metdata: metadata,
                                    file_names: std::mem::replace(v, vec![p.clone()])
                                }));
                            } else {
                                v.push(p.clone());
                            }
                        }).or_insert_with(|| vec![p.clone()]);
                }
            }
        });
    }
}

impl<'a> PipelineStage for Aggregator<'a> {
    fn execute(&mut self) {
        let files: Vec<Arc<OsString>> = self.prev_stage_channel.try_iter().collect();
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(self.num_threads)
            .build()
            .unwrap();
        pool.install(|| Self::aggregate(self, &files));

        // Send remaining aggregated files to next stage.
        let keys: Vec<Arc<FileMetadata>> = self
            .aggregated_files
            .iter()
            .map(|entry| entry.key().clone())
            .collect();

        for key in keys {
            if let Some((k, v)) = self.aggregated_files.remove(&key) {
                let _ = self.next_stage_channel.send(
                    Arc::new(AggregateFiles {
                        file_metdata: k,
                        file_names: v
                    }));
            }
        }
    }
    
    fn is_completed(&self) -> bool {
        true
    }
}

mod tests {

    use std::path::{Path, PathBuf};
    use crossbeam_channel::unbounded;
    use crate::pipeline::executor::{AggregateFiles, PipelineStage};
    use super::{Aggregator, util};
    use std::ffi::OsString;
    use std::sync::Arc;
    use std::collections::HashSet;

    #[test]
    fn aggregate_same_file_test() {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("test_data/test_data");
        
        let file1 = root.join("test10.c");
        let file2 = root.join("test_5/test200.c");

        
        let (tx, rx) = unbounded::<Arc<OsString>>();
        let (ntx, nrx) = unbounded::<Arc<AggregateFiles>>();
        let _ = tx.send(Arc::new(file1.as_os_str().to_os_string()));
        let _ = tx.send(Arc::new(file2.as_os_str().to_os_string()));

        let mut aggregator = Aggregator::new(&None, 1, rx, ntx);
        aggregator.execute();

        let res: Vec<Arc<AggregateFiles>> = nrx.try_iter().collect();
        assert_eq!(res.len(), 1);
        assert_eq!(res.get(0).unwrap().file_names.len(), 2);
    }

    #[test]
    fn aggregate_different_file_test() {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("test_data/test_data");
        
        let file1 = root.join("test10.c");
        let file2 = root.join("test.c");

        
        let (tx, rx) = unbounded::<Arc<OsString>>();
        let (ntx, nrx) = unbounded::<Arc<AggregateFiles>>();
        let _ = tx.send(Arc::new(file1.as_os_str().to_os_string()));
        let _ = tx.send(Arc::new(file2.as_os_str().to_os_string()));

        let mut aggregator = Aggregator::new(&None, 1, rx, ntx);
        aggregator.execute();

        let res: Vec<Arc<AggregateFiles>> = nrx.try_iter().collect();
        assert_eq!(res.len(), 2);
        assert_eq!(res.get(0).unwrap().file_names.len(), 1);
        assert_eq!(res.get(1).unwrap().file_names.len(), 1);
    }

    #[test]
    fn aggregate_different_file_diff_dir_test() {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("test_data/test_data");
        
        let file1 = root.join("test10.c");
        let file2 = root.join("test_5/test19.c");

        
        let (tx, rx) = unbounded::<Arc<OsString>>();
        let (ntx, nrx) = unbounded::<Arc<AggregateFiles>>();
        let _ = tx.send(Arc::new(file1.as_os_str().to_os_string()));
        let _ = tx.send(Arc::new(file2.as_os_str().to_os_string()));

        let mut aggregator = Aggregator::new(&None, 1, rx, ntx);
        aggregator.execute();

        let res: Vec<Arc<AggregateFiles>> = nrx.try_iter().collect();
        assert_eq!(res.len(), 2);
        assert_eq!(res.get(0).unwrap().file_names.len(), 1);
        assert_eq!(res.get(1).unwrap().file_names.len(), 1);
    }

    #[test]
    fn aggregate_file_filter_match_test() {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("test_data/test_data");
        
        let file1 = root.join("test10.c");
        let file2 = root.join("test_5/test200.c");

        
        let (tx, rx) = unbounded::<Arc<OsString>>();
        let (ntx, nrx) = unbounded::<Arc<AggregateFiles>>();
        let _ = tx.send(Arc::new(file1.as_os_str().to_os_string()));
        let _ = tx.send(Arc::new(file2.as_os_str().to_os_string()));

        let mut filter_types = HashSet::new();
        filter_types.insert(util::get_file_type_from_extension("c"));
        let mut filter_types = Some(filter_types);
        let mut aggregator = Aggregator::new(&filter_types, 1, rx, ntx);
        aggregator.execute();

        let res: Vec<Arc<AggregateFiles>> = nrx.try_iter().collect();
        assert_eq!(res.len(), 1);
        assert_eq!(res.get(0).unwrap().file_names.len(), 2);
    }

    #[test]
    fn aggregate_file_filter_no_match_test() {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("test_data/test_data");
        
        let file1 = root.join("test10.c");
        let file2 = root.join("test_5/test200.c");

        
        let (tx, rx) = unbounded::<Arc<OsString>>();
        let (ntx, nrx) = unbounded::<Arc<AggregateFiles>>();
        let _ = tx.send(Arc::new(file1.as_os_str().to_os_string()));
        let _ = tx.send(Arc::new(file2.as_os_str().to_os_string()));

        let mut filter_types = HashSet::new();
        filter_types.insert(util::get_file_type_from_extension("cpp"));
        let mut filter_types = Some(filter_types);
        let mut aggregator = Aggregator::new(&filter_types, 1, rx, ntx);
        aggregator.execute();

        let res: Vec<Arc<AggregateFiles>> = nrx.try_iter().collect();
        assert_eq!(res.len(), 0);
    }    
}