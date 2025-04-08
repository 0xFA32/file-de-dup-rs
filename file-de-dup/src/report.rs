use std::ffi::OsString;

pub struct Report {
    de_duped_files: Vec<Vec<OsString>>,
}

impl Report {
    pub fn new() -> Report {
        Self {
            de_duped_files: Vec::new(),
        }
    }

    pub fn add(&mut self, files: &Vec<OsString>) {
        self.de_duped_files.push(files.to_vec());
    }

    pub fn display(&self) {
        let mut file_num = 1;
        for i in 0..self.de_duped_files.len() {
            println!("Duplicates of file{}:", file_num);
            if let Some(duped_files) = self.de_duped_files.get(i) {
                for file in duped_files {
                    println!("  - {:?}", file);
                }
            }

            file_num += 1;
        }
    }
}

