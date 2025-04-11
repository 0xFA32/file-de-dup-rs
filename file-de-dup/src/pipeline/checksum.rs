use std::{ffi::OsString, fs::File, io::{BufReader, Read}, sync::Arc};

use crossbeam_channel::{Receiver, Sender};
use dashmap::DashMap;
use rayon::iter::IntoParallelIterator;
use crc::{Crc, CRC_64_ECMA_182};
use crate::report::Report;
use rayon::prelude::*;
use super::executor::{AggregateFiles, AggregatedFilesChecksum, PipelineStage};

pub struct Checksum<'a> {
    num_threads: usize,
    do_full_comparison: bool,
    prev_stage_channel: Receiver<Arc<AggregateFiles>>,
    next_stage_channel: Sender<Arc<AggregatedFilesChecksum>>,
    report: &'a mut Report,
    aggregated_files_checksum: DashMap<u64, Vec<Arc<OsString>>>,
}

impl<'a> Checksum<'a> {
    pub fn new(
        num_threads: usize,
        do_full_comparison: bool,
        prev_stage_channel: Receiver<Arc<AggregateFiles>>,
        next_stage_channel: Sender<Arc<AggregatedFilesChecksum>>,
        report: &'a mut Report,
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
                    self.aggregated_files_checksum
                        .entry(checksum)
                        .or_insert_with(Vec::new)
                        .push(file.clone());
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

impl<'a> PipelineStage for Checksum<'a> {
    fn execute(&mut self) {
        let aggregated_files: Vec<Arc<AggregateFiles>> = self.prev_stage_channel.try_iter().collect();
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(self.num_threads)
            .build()
            .unwrap();

        pool.install(|| Self::calculate_checksum(self, &aggregated_files));

        let keys: Vec<u64> = self
            .aggregated_files_checksum
            .iter()
            .map(|entry| entry.key().clone())
            .collect();

        for key in keys {
            if let Some((k, v)) = self.aggregated_files_checksum.remove(&key) {
                if !self.do_full_comparison {
                    self.report.add(&v);
                } else {
                    self.next_stage_channel.send(Arc::new(AggregatedFilesChecksum {
                        file_names: v,
                        checksum: k 
                    }));
                }
            }
        }
    }
    
    fn is_completed(&self) -> bool {
        true
    } 
}