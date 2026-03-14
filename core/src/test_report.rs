#![allow(missing_docs)]

use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, BufWriter};
use std::path::Path;
use std::time::SystemTime;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TestResult {
    pub name: String,
    pub module: String,
    pub status: String, // "pass", "fail"
    pub error: Option<String>,
    pub duration_ms: u128,
    pub timestamp: SystemTime,
}

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct TestReport {
    pub results: Vec<TestResult>,
}

impl TestReport {
    pub fn new() -> Self {
        Self {
            results: Vec::new(),
        }
    }

    pub fn add_result(&mut self, result: TestResult) {
        self.results.push(result);
        // Sort by module (lexical), then by status (fail before pass), then by name
        self.results.sort_by(|a, b| {
            a.module
                .cmp(&b.module)
                .then_with(|| b.status.cmp(&a.status))
                .then_with(|| a.name.cmp(&b.name))
        });
    }

    pub fn save_to_file<P: AsRef<Path>>(&self, path: P) -> io::Result<()> {
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path)?;
        let writer = BufWriter::new(file);
        serde_json::to_writer_pretty(writer, &self)?;
        Ok(())
    }

    pub fn load_from_file<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let file = File::open(path)?;
        let reader = BufReader::new(file);
        let report = serde_json::from_reader(reader)?;
        Ok(report)
    }

    pub fn print_summary(&self) {
        println!("\nTest Report Summary:");
        let mut by_module: std::collections::BTreeMap<&str, Vec<&TestResult>> =
            std::collections::BTreeMap::new();
        for result in &self.results {
            by_module.entry(&result.module).or_default().push(result);
        }
        for (module, results) in by_module.iter() {
            println!("\nModule: {}", module);
            for res in results {
                println!(
                    "  [{}] {} ({} ms){}",
                    res.status,
                    res.name,
                    res.duration_ms,
                    res.error
                        .as_ref()
                        .map(|e| format!(" - {}", e))
                        .unwrap_or_default()
                );
            }
        }
    }
}
