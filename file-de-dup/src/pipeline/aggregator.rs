use super::executor::PipelineStage;

pub struct Aggregator<'a> {
    filter_file_types: &'a Option<Vec<String>>,
    num_threads: usize,
}

impl<'a> Aggregator<'a> {
    pub fn new(filter_file_types: &'a Option<Vec<String>>, num_threads: usize) -> Aggregator<'a> {
        Self {
            filter_file_types,
            num_threads,
        }
    }
}

impl<'a> PipelineStage for Aggregator<'a> {
    fn execute(&mut self) {
        
    }
    
    fn is_completed(&self) -> bool {
        true
    }

    fn progress(&self) -> String {
        "hello".to_string()
    }
}