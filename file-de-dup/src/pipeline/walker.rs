use super::executor::PipelineStage;

pub struct Walker {
    full_path: &'static str,
    recursive: bool,
    num_threads: usize,
}

impl Walker {
    pub fn new(full_path: &'static str, recursive: bool, num_threads: usize) -> Walker {
        Self {
            full_path,
            recursive,
            num_threads,
        }
    }
}

impl PipelineStage for Walker {
    fn execute(&mut self) {
        
    }
    
    fn is_completed(&self) -> bool {
        true
    }

    fn progress(&self) -> String {
        "hello".to_string()
    }
}