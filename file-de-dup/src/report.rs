pub struct Report {
    de_duped_files: Vec<Vec<String>>,
}

impl Report {
    pub fn new() -> Report {
        Self {
            de_duped_files: Vec::new(),
        }
    }

    pub fn display(&self) {
        let mut file_num = 1;
        for i in 0..self.de_duped_files.len() {
            println!("Duplicates of file{}:", file_num);
            if let Some(duped_files) = self.de_duped_files.get(i) {
                for file in duped_files {
                    println!("  - {}", file);
                }
            }
        }
    }
}

