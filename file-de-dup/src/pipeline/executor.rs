use std::ffi::OsString;

/// Pipelined execution of various stages to find duplicates of files.
/// 
///   ┌─────────────┐       ┌─────────────┐       ┌─────────────┐       ┌─────────────┐
///   │    Walk     │------>│  Aggregate  │------>│ Checksum    │------>│  Full comp  │
///   │             │       │             │       │             │       │             │
///   └─────────────┘       └─────────────┘       └─────────────┘       └─────────────┘
///                                                     |                    |
///                                                     └────────────────────└─────────> Report
/// 
/// Stage 1: Walk
///     - Walk the directory recursively (if specified) and generated the set of files.
/// Stage 2: Aggregate
///     - Aggregate the files by various parameters like file size, type etc.
/// Stage 3: Checksum
///     - For same set of files then calculate the checksum.
/// Stage 4: Byte by Byte comparison
///     - If the checksums are the same for some set of files then do a byte-by-byte comparison.
/// 
/// In the initial version we can have each stage execute serially. In the later stages we can look 
/// into executing them parallely.

use crate::{pipeline::aggregator::Aggregator, report::Report};
use super::{checksum::Checksum, filecompare::FileCompare, walker::Walker};
use crossbeam_channel::unbounded;

pub struct Executor {
    full_path: &'static str,
    recursive: bool,
    filter_file_types: Option<Vec<String>>,
    num_threads: usize,
    do_full_comparison: bool,
}

/// Trait to be implemented by a stage of a pipeline.
pub trait PipelineStage {
    /// Execute the stage of the pipeline.
    fn execute(&mut self);
    
    /// Returns true if the stage is completed. False, otherwise.
    fn is_completed(&self) -> bool;
}

impl Executor {
    pub fn new(
        full_path: &'static str,
        recursive: bool,
        filter_file_types: Option<Vec<String>>,
        num_threads: usize,
        do_full_comparison: bool,
    ) -> Executor {
        Self {
            full_path,
            recursive,
            filter_file_types,
            num_threads,
            do_full_comparison,
        }
    }

    pub fn execute(&self) {
        let mut report = Report::new();
        let (mut s1, mut r1) = unbounded::<OsString>();
        let mut walker = Walker::new(self.full_path, self.recursive, self.num_threads, s1);
        walker.execute();
        let mut agg = Aggregator::new(&self.filter_file_types, self.num_threads, r1);
        agg.execute();
        let iterator = agg.aggregated_files.iter();
        for it in iterator {
            let files = it.value();

            report.add(files);
            /*
            if files.len() == 1 {
                report.add(files);
            } else {

            }*/
        }

        report.display();
        /*
        let mut agg = Aggregator::new(&self.filter_file_types, self.num_threads);
        let mut checksum = Checksum::new(self.num_threads, self.do_full_comparison);
        let mut file_compare = FileCompare::new(self.num_threads);

        walker.execute();
        println!("Current progress = {}", walker.progress());
        
        agg.execute();
        println!("Current progress = {}", agg.progress());

        checksum.execute();
        println!("Current progress = {}", checksum.progress());

        file_compare.execute();
        println!("Current progress = {}", file_compare.progress());*/
    }
}