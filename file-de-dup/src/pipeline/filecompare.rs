use std::{sync::Arc, thread};

use crossbeam_channel::{unbounded, Receiver};

use crate::report::Report;

use super::executor::{AggregatedFilesChecksum, PipelineStage};
use rayon::prelude::*;

pub struct FileCompare<'a> {
    num_threads: usize,
    prev_stage_channel: Receiver<Arc<AggregatedFilesChecksum>>,
    report: &'a mut Report,
}

impl<'a> FileCompare<'a> {
    pub fn new(
        num_threads: usize,
        prev_stage_channel: Receiver<Arc<AggregatedFilesChecksum>>,
        report: &'a mut Report,
    ) -> FileCompare {
        Self {
            num_threads,
            prev_stage_channel,
            report,
        }
    }

    fn file_compare(&mut self, files: &Vec<Arc<AggregatedFilesChecksum>>) {
        files.into_par_iter().for_each(|file| {

        });
    }
}

impl<'a> PipelineStage for FileCompare<'a> {
    fn execute(&mut self) {
        let files: Vec<Arc<AggregatedFilesChecksum>> = self.prev_stage_channel.try_iter().collect();
        let (mut file_compare_sender_chan, mut file_compare_receiver_chan) =
            unbounded::<AggregatedFilesOffset>();
        for file in files {
            let _ = file_compare_sender_chan.send(AggregatedFilesOffset { files: file, offset: 0 });
        }

        for id in 0..self.num_threads {
            let r_chan = file_compare_receiver_chan.clone();
            let s_chan = file_compare_sender_chan.clone();
            thread::spawn(move || {
                let buffer = [0u8; 4096];
                for aggregate_file in r_chan {
                    let mut base_offset = aggregate_file.offset;
                    for file in aggregate_file.files.file_names.iter() {

                    }
                }
            });
        }

    }
    
    fn is_completed(&self) -> bool {
        true
    }
}

struct AggregatedFilesOffset {
    files: Arc<AggregatedFilesChecksum>,
    offset: u64,
}