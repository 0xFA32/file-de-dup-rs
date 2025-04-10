use std::{collections::HashSet, ffi::OsString, fs, sync::Arc};

use crossbeam_channel::{Receiver, Sender};
use dashmap::DashMap;
use rayon::prelude::*;
use super::{executor::{AggregateFiles, FileMetadata, PipelineStage}, util};

// Aggregate these many files before sending to the next stage.
// Set it to usize::MAX to aggregate all of the files before sending
// to the next stage.
const AGGREGATE_LIMIT: usize = usize::MAX;

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

    fn aggregate(&mut self, paths: &Vec<Arc<OsString>>) -> Result<(), &'static str> {
        paths.into_par_iter().try_for_each(|p| {
            if let Ok(file_metadata) = fs::metadata(p.as_os_str()) {
                let file_type = util::get_file_type(p);

                // Skip files based on filter file type provided from user.
                if let Some(types) = self.filter_file_types {
                    if !types.contains(&file_type) {
                        return Ok(());    
                    }
                }

                let metadata = Arc::new(FileMetadata { 
                    size: file_metadata.len(),
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
                                    file_metdata: metadata.clone(),
                                    file_names: std::mem::replace(v, vec![p.clone()])
                                }));
                            } else {
                                v.push(p.clone());
                            }
                        });
                    
                }
            }

            Ok(())
        })
    }
}

impl<'a> PipelineStage for Aggregator<'a> {
    fn execute(&mut self) {
        let files: Vec<Arc<OsString>> = self.prev_stage_channel.try_iter().collect();
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(self.num_threads)
            .build()
            .unwrap();
        let res = pool.install(|| Self::aggregate(self, &files));

        if res.is_err() {
            eprintln!("Error in generating the aggregate files");
            return;
        }

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