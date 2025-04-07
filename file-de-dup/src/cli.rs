use clap::Parser;
use std::{path::Path, process::exit};

mod pipeline;
mod report;

use crate::pipeline::executor::Executor;

/// Arguments which can be passed to the tool to find de-dup of files.
#[derive(Parser, Debug)]
#[command(version, about)]
struct Args {
    /// Full path under which the files would be checked for de-dup.
    #[arg(short = 'f', long)]
    full_path: String,

    /// Check for files recursively. Default value is false.
    #[arg(short = 'r', long, default_value_t = false)]
    recursive: bool,

    /// Filter for given file types. If set to None then the tool runs for all of the
    /// file types.
    #[arg(short = 't', long, num_args = 1..)]
    filter_file_types: Option<Vec<String>>,

    /// Number of threads the tool should use to run the tool. If no value provided then
    /// we would use 3x number of cores.
    ///     TODO: For now use 3x number of cores but run tests to finalize on this.
    #[arg(short = 'n', long, default_value_t = 0)]
    num_threads: usize,

    /// Do a full comparison for the file which would be a byte by byte comparison.
    /// By default the tool would do a byte-by-byte comparison but if specified otherwise
    /// then the tool can use the checksum to evaluate the duplicate of files. Useful in cases
    /// we want to do a quick check of the files and we are aware of the content of the files by
    /// the name.
    #[arg(short = 'c', long, default_value_t = true)]
    do_full_comparison: bool,
}

/// A cli application to find duplicates of files in a given directory
/// and printing out a simple report. 
/// Eg: 
///     Consider a directory `/foo` with following files
///         - /foo/f1
///         - /foo/f2
///         - /foo/bar/f1
///         - /foo/bar/foo/f1
///         - /foo/bar/f2
///
///     Command 1: ./file-de-dup --full-path "/foo" --recursive then it returns
///             File1
///                 - /foo/f1
///                 - /foo/bar/f1
///                 - /foo/bar/foo/f1
///             
///             File2
///                 - /foo/f2
///                 - /foo/bar/f2
/// 
///     Command 1: ./file-de-dup --full-path "/foo/bar" --recursive then it returns
///             File1
///                 - /foo/bar/f1
///                 - /foo/bar/foo/f1
///             
///             File2
///                 - /foo/bar/f2
/// 
/// Note: File names can be different but the tool looks at actual content so File1 and File2
/// are arbitary names used to denote f1 and f2 respectively.
fn main() {
    let args = Args::parse();

    if !Path::new(&args.full_path).exists() {
        eprintln!("Invalid path name = {}", args.full_path);
        exit(1);
    }

    let num_threads = if args.num_threads == 0 {
        3*num_cpus::get()
    } else {
        args.num_threads
    };

    let executor = Executor::new(
        Box::leak(args.full_path.into_boxed_str()),
        args.recursive,
        args.filter_file_types,
        num_threads,
        args.do_full_comparison);
    
    executor.execute();
}
