use std::{ffi::OsString, ops::{Deref, DerefMut}, sync::Arc};

pub struct Report {
    de_duped_files: Vec<Vec<Arc<OsString>>>,
}

impl Report {
    pub fn new() -> Report {
        Self {
            de_duped_files: Vec::new(),
        }
    }

    pub fn add(&mut self, files: &Vec<Arc<OsString>>) {
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

    #[cfg(test)]
    pub fn get(&self) -> &Vec<Vec<Arc<OsString>>> {
        &self.de_duped_files
    }
}

impl Deref for Report {
    type Target = Vec<Vec<Arc<OsString>>>;

    fn deref(&self) -> &Self::Target {
        &self.de_duped_files
    }
}

impl DerefMut for Report {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.de_duped_files
    }
}