use std::{ffi::OsString, fs};

use crossbeam_channel::Receiver;
use dashmap::DashMap;
use rayon::prelude::*;
use super::executor::PipelineStage;

pub struct Aggregator<'a> {
    filter_file_types: &'a Option<Vec<String>>,
    num_threads: usize,
    prev_stage_channel: Receiver<OsString>,
    pub aggregated_files: DashMap<FileMetadata, Vec<OsString>>,
}

#[derive(PartialEq, Eq, Hash)]
pub struct FileMetadata {
    size: u64,
    file_type: u8,
}

impl<'a> Aggregator<'a> {
    pub fn new(
        filter_file_types: &'a Option<Vec<String>>,
        num_threads: usize,
        prev_stage_channel: Receiver<OsString>,
    ) -> Aggregator<'a> {
        Self {
            filter_file_types,
            num_threads,
            prev_stage_channel,
            aggregated_files: DashMap::new(),
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

            self.aggregated_files.entry(metadata)
                .or_insert_with(Vec::new)
                .push(p.clone());

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
        }
    }
    
    fn is_completed(&self) -> bool {
        true
    }
}