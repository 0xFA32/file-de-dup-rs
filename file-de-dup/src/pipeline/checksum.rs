use std::{ffi::OsString, fs::File, io::BufReader, sync::Arc};

use crossbeam_channel::{Receiver, Sender};
use dashmap::DashMap;
use rayon::iter::IntoParallelIterator;
use sha2::{Sha256, Digest};
use crate::report::Report;
use rayon::prelude::*;
use super::executor::{AggregateFiles, AggregatedFilesChecksum, PipelineStage};

pub struct Checksum {
    num_threads: usize,
    do_full_comparison: bool,
    prev_stage_channel: Receiver<Arc<AggregateFiles>>,
    next_stage_channel: Sender<Arc<AggregatedFilesChecksum>>,
    report: Report,
    aggregated_files_checksum: DashMap<u64, Vec<Arc<OsString>>>,
}

impl Checksum {
    pub fn new(
        num_threads: usize,
        do_full_comparison: bool,
        prev_stage_channel: Receiver<Arc<AggregateFiles>>,
        next_stage_channel: Sender<Arc<AggregatedFilesChecksum>>,
        report: Report,
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

    fn calculate_checksum(&mut self, aggregated_files: &Vec<Arc<AggregateFiles>>) -> Result<(), &'static str> {
        aggregated_files.into_par_iter().try_for_each(|aggregate_file| {

            aggregate_file.file_names.into_par_iter().try_for_each(|file| {

            })
            Ok(())
        })
    }

    fn calculate_checksum_file(file: Arc<OsString>) -> u64 {
        let file = File::open(file.as_os_str());
        if file.is_err() {
            // TODO: For now return 0;
            return 0;
        }

        let mut reader = BufReader::new(file.unwrap());
        let mut buffer = [0u8, 16384]; 

        let mut hasher = Sha256::new();
        reader.read(&mut buffer);
        hasher.update(data);
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
    }
    
    fn is_completed(&self) -> bool {
        true
    } 
}