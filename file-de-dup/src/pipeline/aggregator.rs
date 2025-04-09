use std::{ffi::OsString, fs};

use crossbeam_channel::{Receiver, Sender};
use dashmap::DashMap;
use rayon::prelude::*;
use super::executor::{AggregateFiles, FileMetadata, PipelineStage};

// Aggregate these many files before sending to the next stage.
// Set it to usize::MAX to aggregate all of the files before sending
// to the next stage.
const AGGREGATE_LIMIT: usize = usize::MAX;

pub struct Aggregator<'a> {
    filter_file_types: &'a Option<Vec<String>>,
    num_threads: usize,
    prev_stage_channel: Receiver<OsString>,
    aggregated_files: DashMap<FileMetadata, Vec<OsString>>,
    next_stage_channel: Sender<AggregateFiles>
}

impl<'a> Aggregator<'a> {
    pub fn new(
        filter_file_types: &'a Option<Vec<String>>,
        num_threads: usize,
        prev_stage_channel: Receiver<OsString>,
        next_stage_channel: Sender<AggregateFiles>,
    ) -> Aggregator<'a> {
        Self {
            filter_file_types,
            num_threads,
            prev_stage_channel,
            aggregated_files: DashMap::new(),
            next_stage_channel: next_stage_channel,
        }
    }

    fn aggregate(&mut self, paths: &Vec<OsString>) -> Result<(), &'static str> {
        paths.into_par_iter().try_for_each(|p| {
            let file_metadata = fs::metadata(p).unwrap();
            let file_type = Self::get_file_type(p);

            // TODO: Filter by file type provided in the cli.
            let metadata = FileMetadata { 
                size: file_metadata.len(),
                file_type: file_type,
            };

            if AGGREGATE_LIMIT == usize::MAX {
                self.aggregated_files.entry(metadata)
                    .or_insert_with(Vec::new)
                    .push(p.clone());
            } else {
                self.aggregated_files.entry(metadata.clone())
                    .and_modify(|v| {
                        if v.len() > AGGREGATE_LIMIT {
                            self.next_stage_channel.send(AggregateFiles { 
                                file_metdata: metadata,
                                file_names: std::mem::replace(v, vec![p.clone()])
                            });
                        } else {
                            v.push(p.clone());
                        }
                    });
                
            }

            Ok(())
        })
    }

    fn get_file_type(p: &OsString) -> u8 {
        // TODO: Categorize by different file types.
        return 0;
    }
}

impl<'a> PipelineStage for Aggregator<'a> {
    fn execute(&mut self) {
        let files: Vec<OsString> = self.prev_stage_channel.try_iter().collect();
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(self.num_threads)
            .build()
            .unwrap();
        let res = pool.install(|| Self::aggregate(self, &files));

        if res.is_err() {
            eprintln!("Error in generating the aggregate files");
            return;
        }

        let keys: Vec<FileMetadata> = self
            .aggregated_files
            .iter()
            .map(|entry| entry.key().clone())
            .collect();

        for key in keys {
            if let Some((k, v)) = self.aggregated_files.remove(&key) {
                let _ = self.next_stage_channel.send(
                    AggregateFiles {
                        file_metdata: k,
                        file_names: v
                    });
            }
        }
    }
    
    fn is_completed(&self) -> bool {
        true
    }
}