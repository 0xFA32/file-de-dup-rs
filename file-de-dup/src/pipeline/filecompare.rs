use super::executor::PipelineStage;

pub struct FileCompare {
    // Add channel details.
    num_threads: usize,
}

impl FileCompare {
    pub fn new(num_threads: usize) -> FileCompare {
        Self {
            num_threads,
        }
    }
}

impl PipelineStage for FileCompare {
    fn execute(&mut self) {
        
    }
    
    fn is_completed(&self) -> bool {
        true
    }

    fn progress(&self) -> String {
        "hello".to_string()
    }    
}