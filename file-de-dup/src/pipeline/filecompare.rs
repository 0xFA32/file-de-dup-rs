use std::{cmp::min, collections::HashMap, ffi::OsString, fs::File, sync::{Arc, Mutex}, thread, time::Duration};

use crossbeam_channel::{unbounded, Receiver, Sender};
use memmap2::{Mmap, MmapOptions};

use crate::report::Report;

use super::executor::{AggregatedFilesChecksum, PipelineStage};

const CHUNK_SIZE: usize = 4096;
const TIMEOUT_MILLIS: u64 = 10;

pub struct FileCompare {
    num_threads: usize,
    prev_stage_channel: Receiver<Arc<AggregatedFilesChecksum>>,
    report: Arc<Mutex<Report>>,
}

impl FileCompare {
    pub fn new(
        num_threads: usize,
        prev_stage_channel: Receiver<Arc<AggregatedFilesChecksum>>,
        report: Arc<Mutex<Report>>,
    ) -> FileCompare {
        Self {
            num_threads,
            prev_stage_channel,
            report,
        }
    }

    fn get_total_work(files: &[Arc<AggregatedFilesChecksum>]) -> u64 {
        let mut total_work: u64 = 0;
        for file in files {
            total_work += file.file_names.len() as u64;
        }

        return total_work;
    }

    fn add_to_report(
        report: Arc<Mutex<Report>>,
        total_work: Arc<Mutex<u64>>,
        aggregated_files: &AggregatedFilesOffset,
    ) -> u64 {
        let mut data = report.lock().unwrap();
        data.add(&aggregated_files.files.iter().map(|f| f.file_name.clone()).collect());
        drop(data);

        let mut num = total_work.lock().unwrap();
        *num -= aggregated_files.files.len() as u64;
        let ret = *num;
        drop(num);

        return ret;
    }

    fn do_work(
        &mut self,
        file_compare_receiver_chan: Receiver<AggregatedFilesOffset>,
        file_compare_sender_chan: Sender<AggregatedFilesOffset>,
        work_done: Arc<Mutex<u64>>,
    ) {
        let mut threads = Vec::new();

        // Sent all work to channel. Going to start working on it.
        for _ in 0..self.num_threads {
            let r_chan = file_compare_receiver_chan.clone();
            let s_chan = file_compare_sender_chan.clone();
            let report = self.report.clone();
            let total_work = work_done.clone();
            let t= thread::spawn(move || {
                loop {
                    match r_chan.recv_timeout(Duration::from_millis(TIMEOUT_MILLIS)) {
                        Ok(aggregate_file) => {
                            let mut offset = aggregate_file.offset as usize;
                            if offset == aggregate_file.file_size - 1 || aggregate_file.files.len() <= 1 {
                                let remaining_work = Self::add_to_report(
                                    report.clone(),
                                    total_work.clone(),
                                    &aggregate_file);
                                
                                match remaining_work {
                                    0 => break,
                                    _ => continue,
                                }
                            }

                            while offset < aggregate_file.file_size - 1 {
                                let end = min(offset + CHUNK_SIZE, aggregate_file.file_size - 1);
                                let reference = &aggregate_file
                                    .files
                                    .get(0)
                                    .unwrap()
                                    .file_ptr[offset..end];

                                let mut observed_difference = false;
        
                                for index in 0..aggregate_file.files.len() {
                                    let ptr = &aggregate_file
                                        .files
                                        .get(index)
                                        .unwrap()
                                        .file_ptr[offset..end];

                                    if reference != ptr {
                                        observed_difference = true;
                                        break;
                                    }
                                }
        
                                if observed_difference {

                                    let mut byte_comparison: HashMap<&[u8], Vec<Arc<FilePtr>>> = HashMap::new();
                                    for index in 0..aggregate_file.files.len() {
                                        let file_ptr = aggregate_file.files.get(index).unwrap();
                                        let ptr = &file_ptr.file_ptr[offset..end];
                                        byte_comparison.entry(ptr)
                                            .or_insert_with(Vec::new)
                                            .push(file_ptr.clone());
                                    }
        
                                    for (_, value) in byte_comparison.into_iter() {
                                        let _ = s_chan.send(AggregatedFilesOffset {
                                            files: value,
                                            offset: offset,
                                            file_size: aggregate_file.file_size,
                                        });
                                    }
        
                                    break;
                                }
        
                                offset = end;
        
                            }

                            if offset == aggregate_file.file_size - 1 {
                                // If all the files are same then we can add it to the report.
                                let remaining_work = Self::add_to_report(
                                    report.clone(),
                                    total_work.clone(),
                                    &aggregate_file);
                                
                                match remaining_work {
                                    0 => break,
                                    _ => continue,
                                }
                            }
                        }
                        Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                            let num = total_work.lock().unwrap();
                            if *num == 0 {
                                drop(num);
                                break;
                            }

                            drop(num);
                            // Continue to receive work from the channel. Technically we might
                            // be running the thread even though we might not expect more work
                            // but that is fine as it is going to be in the end of the loop.

                        }
                        Err(crossbeam_channel::RecvTimeoutError::Disconnected) => {
                            println!("Channel closed?");
                            break;
                        }
                    }

                }
            });

            threads.push(t);
        }

        for thread in threads {
            thread.join().unwrap();
        }        
    }

    fn send_work(&mut self, chan: Sender<AggregatedFilesOffset>, files: &[Arc<AggregatedFilesChecksum>]) {
        for aggregated_file in files {
            let mut file_ptrs: Vec<Arc<FilePtr>> = Vec::new();
            for i in 0..aggregated_file.file_names.len() {
                let file = aggregated_file.file_names.get(i).unwrap();
                let f = File::open(file.as_os_str()).unwrap();
                file_ptrs.push(Arc::new(FilePtr {
                    file_ptr: unsafe { MmapOptions::new().map(&f).unwrap() },
                    file_name: file.clone(),
                }));
            }
            
            let _ = chan.send(AggregatedFilesOffset {
                files: file_ptrs,
                offset: 0,
                file_size: aggregated_file.file_size as usize
            });
        }        
    }
}

impl PipelineStage for FileCompare {
    fn execute(&mut self) {
        let files: Vec<Arc<AggregatedFilesChecksum>> = self.prev_stage_channel.try_iter().collect();

        for chunked_aggregated_files in files.chunks(10_000).into_iter() {
            let total_work = Self::get_total_work(chunked_aggregated_files);

            let work_done = Arc::new(Mutex::new(0u64));
            let mut num = work_done.lock().unwrap();
            *num = total_work;
    
            drop(num);
    
            let (file_compare_sender_chan, file_compare_receiver_chan) =
                unbounded::<AggregatedFilesOffset>();

            Self::send_work(self, file_compare_sender_chan.clone(), chunked_aggregated_files);

            Self::do_work(self, file_compare_receiver_chan, file_compare_sender_chan, work_done);
        }
    }
    
    fn is_completed(&self) -> bool {
        true
    }
}

struct AggregatedFilesOffset {
    files: Vec<Arc<FilePtr>>,
    offset: usize,
    file_size: usize,
}

struct FilePtr {
    file_ptr: Mmap,
    file_name: Arc<OsString>,
}

mod tests {
    use std::fs;
    use std::path::{Path, PathBuf};
    use crossbeam_channel::unbounded;
    use crate::pipeline::executor::{AggregateFiles, AggregatedFilesChecksum, FileMetadata, PipelineStage};
    use crate::report::Report;
    use crate::pipeline::util;
    use super::FileCompare;
    use std::ffi::OsString;
    use std::sync::{Arc, Mutex};
    use std::collections::HashSet;

    #[test]
    fn file_compare_same_file_test() {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("test_data/test_data");
        
        let file1 = root.join("test10.c");
        let file2 = root.join("test_5/test200.c");
        let file_metadata = fs::metadata(&file1).unwrap();

        let report = Arc::new(Mutex::new(Report::new()));
        let (tx, rx) = unbounded::<Arc<AggregatedFilesChecksum>>();
        let aggregate_file = AggregatedFilesChecksum {
            file_names: vec![Arc::new(file1.as_os_str().to_os_string()), Arc::new(file2.as_os_str().to_os_string())],
            checksum: 100,
            file_size: file_metadata.len() as usize,
        };

        let _ = tx.send(Arc::new(aggregate_file));

        let mut file_compare = FileCompare::new(1, rx, report.clone());
        file_compare.execute();

        assert_eq!(report.lock().unwrap().get().len(), 1);
        assert_eq!(report.lock().unwrap().get().get(0).unwrap().len(), 2);
    }

    #[test]
    fn file_compare_different_file_test() {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("test_data/test_data");
        
        let file1 = root.join("test1.c");
        let file2 = root.join("test.c");
        let file_metadata = fs::metadata(&file1).unwrap();

        let report = Arc::new(Mutex::new(Report::new()));
        let (tx, rx) = unbounded::<Arc<AggregatedFilesChecksum>>();
        let aggregate_file = AggregatedFilesChecksum {
            file_names: vec![Arc::new(file1.as_os_str().to_os_string()), Arc::new(file2.as_os_str().to_os_string())],
            checksum: 100,
            file_size: file_metadata.len() as usize,
        };

        let _ = tx.send(Arc::new(aggregate_file));

        let mut file_compare = FileCompare::new(1, rx, report.clone());
        file_compare.execute();

        assert_eq!(report.lock().unwrap().get().len(), 2);
        assert_eq!(report.lock().unwrap().get().get(0).unwrap().len(), 1);
        assert_eq!(report.lock().unwrap().get().get(1).unwrap().len(), 1);
    }

    #[test]
    fn file_compare_different_file_different_size_test() {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("test_data/test_data");
        
        let file1 = root.join("test1.c");
        let file2 = root.join("test10.c");
        let file_metadata = fs::metadata(&file1).unwrap();

        let report = Arc::new(Mutex::new(Report::new()));
        let (tx, rx) = unbounded::<Arc<AggregatedFilesChecksum>>();
        let aggregate_file = AggregatedFilesChecksum {
            file_names: vec![Arc::new(file1.as_os_str().to_os_string())],
            checksum: 100,
            file_size: file_metadata.len() as usize,
        };

        let aggregate_file1 = AggregatedFilesChecksum {
            file_names: vec![Arc::new(file2.as_os_str().to_os_string())],
            checksum: 100,
            file_size: fs::metadata(&file2).unwrap().len() as usize,
        };

        let _ = tx.send(Arc::new(aggregate_file));
        let _ = tx.send(Arc::new(aggregate_file1));

        let mut file_compare = FileCompare::new(1, rx, report.clone());
        file_compare.execute();

        assert_eq!(report.lock().unwrap().get().len(), 2);
        assert_eq!(report.lock().unwrap().get().get(0).unwrap().len(), 1);
        assert_eq!(report.lock().unwrap().get().get(1).unwrap().len(), 1);
    }
    
    #[test]
    fn file_compare_different_file_same_checksum_2_test() {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("test_data/test_data");
        
        let file1 = root.join("test1.c");
        let file2 = root.join("test.c");
        let file_metadata = fs::metadata(&file1).unwrap();

        let report = Arc::new(Mutex::new(Report::new()));
        let (tx, rx) = unbounded::<Arc<AggregatedFilesChecksum>>();
        let aggregate_file = AggregatedFilesChecksum {
            file_names: vec![Arc::new(file1.as_os_str().to_os_string()), Arc::new(file2.as_os_str().to_os_string())],
            checksum: 100,
            file_size: file_metadata.len() as usize,
        };

        let _ = tx.send(Arc::new(aggregate_file));

        let mut file_compare = FileCompare::new(1, rx, report.clone());
        file_compare.execute();

        assert_eq!(report.lock().unwrap().get().len(), 2);
        assert_eq!(report.lock().unwrap().get().get(0).unwrap().len(), 1);
        assert_eq!(report.lock().unwrap().get().get(1).unwrap().len(), 1);
    }

    #[test]
    fn file_compare_different_file_same_checksum_3_test() {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("test_data/test_data");
        
        let file1 = root.join("test1.c");
        let file2 = root.join("test.c");
        let file3 = root.join("test11.c");
        let file_metadata = fs::metadata(&file1).unwrap();

        let report = Arc::new(Mutex::new(Report::new()));
        let (tx, rx) = unbounded::<Arc<AggregatedFilesChecksum>>();
        let aggregate_file = AggregatedFilesChecksum {
            file_names: vec![
                Arc::new(file1.as_os_str().to_os_string()),
                Arc::new(file2.as_os_str().to_os_string()),
                Arc::new(file3.as_os_str().to_os_string())
            ],
            checksum: 100,
            file_size: file_metadata.len() as usize,
        };

        let _ = tx.send(Arc::new(aggregate_file));

        let mut file_compare = FileCompare::new(1, rx, report.clone());
        file_compare.execute();

        assert_eq!(report.lock().unwrap().get().len(), 3);
    }
}