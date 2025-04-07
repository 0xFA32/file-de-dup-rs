use super::executor::PipelineStage;

pub struct Checksum {
    // Channel related stuff.
    num_threads: usize,
    do_full_comparison: bool,
}

impl Checksum {
    pub fn new(num_threads: usize, do_full_comparison: bool) -> Checksum {
        Self {  
            num_threads,
            do_full_comparison,
        }
    }
}

impl PipelineStage for Checksum {
    fn execute(&mut self) {
        
    }
    
    fn is_completed(&self) -> bool {
        true
    }

    fn progress(&self) -> String {
        "hello".to_string()
    }    
}