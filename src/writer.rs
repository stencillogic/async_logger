use std::path::PathBuf;
use std::fs::File;
use std::time::SystemTime;
use std::thread::JoinHandle;
use crate::buf::DoubleBuf;
use std::io::Write;
use std::sync::{Arc, Mutex, atomic::AtomicBool, atomic::Ordering};
use super::{Writer, Error, ErrorRepr, ErrorKind};


/// Writer to a file.
pub struct FileWriter {
    f:  File,
    file_size: usize,
    log_dir: String,
    written_size: usize,
}


impl FileWriter {

    /// Creates a `FileWriter` which will write to a file located in `log_dir` directory.
    /// The file name has form `log_<unix_timestamp_seconds>.txt`.
    /// When the file size grows beyound `file_size` bytes the file is closed and a new file is
    /// opened.
    ///
    /// # Errors
    ///
    /// `Err` is returend if opening of log file was not successful.
    pub fn new(log_dir: &str, file_size: usize) -> Result<FileWriter, Error> {

        let f = FileWriter::open_log_file(log_dir)?;

        Ok(FileWriter {
            f,
            file_size,
            log_dir: log_dir.to_owned(),
            written_size: 0,
        })
    }

    fn open_log_file(log_dir: &str) -> Result<File, Error> {

        let epoch_secs = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map_err(|e| { Error::new(ErrorKind::TimeError, ErrorRepr::TimeError(e)) })?
            .as_secs();

        let mut file_name = format!("log_{}.txt", epoch_secs);
        let mut file_path;
        let mut index = 2;

        loop {
            file_path = PathBuf::new();

            file_path.push(log_dir);
            file_path.push(&file_name);
            if file_path.exists() {
                file_name = format!("log_{}_{}.txt", epoch_secs, index);
                index += 1;
            } else {
                break;
            }
        }

        let file_path = file_path
            .to_str()
            .ok_or(Error::new(ErrorKind::PathToStrConversionError, ErrorRepr::Simple))?;

        std::fs::OpenOptions::new()
            .append(true)
            .create_new(true)
            .open(file_path)
            .map_err(|e| { Error::new(ErrorKind::IoError, ErrorRepr::IoError(e)) })
    }

    fn rotate_log(&mut self) {
        match FileWriter::open_log_file(&self.log_dir) {
            Ok(f) => {
                self.f = f;
                self.written_size = 0;
            }
            Err(e) => eprintln!("Failed to perform log rotation: {}", e),
        };
    }
}


impl Writer<u8> for FileWriter {

    fn process_slice(&mut self, slice: &[u8]) {

        if self.written_size > self.file_size {
            self.rotate_log();
        }

        if let Err(e) = self.f.write_all(slice) {
            eprintln!("Write to log failed: {}", e);
        }

        self.written_size += slice.len();
    }

    fn flush(&mut self) {

        let _ret = self.f.flush();
    }
}


impl Writer<Box<String>> for FileWriter {

    fn process_slice(&mut self, slice: &[Box<String>]) {

        if self.written_size > self.file_size {
            self.rotate_log();
        }

        for item in slice {
            let bytes = item.as_bytes();

            if let Err(e) = self.f.write_all(bytes) {
                eprintln!("Write to log failed: {}", e);
            }

            self.written_size += bytes.len();
        }
    }

    fn flush(&mut self) {

        let _ret = self.f.flush();
    }
}


/// Writer thread.
pub struct ThreadedWriter {
    writer_thread:  JoinHandle<()>,
    terminate:      Arc<AtomicBool>,
}


impl ThreadedWriter {

    /// Create a new instance: spawn a thread, pass a `writer` there, and do writes of log messages
    /// using the `writer`.
    pub fn new<T: Send + 'static>(writer: Arc<Mutex<Box<dyn Writer<T>>>>, db: &DoubleBuf<T>) -> ThreadedWriter {

        let terminate = Arc::new(AtomicBool::new(false));

        let terminate2 = terminate.clone();

        let db2 = (*db).clone();

        let writer_thread = std::thread::spawn(move || {

            Self::write_log_loop(writer, db2, terminate2);
        });

        ThreadedWriter {
            writer_thread,
            terminate,
        }
    }


    pub fn request_stop(&self) {

        self.terminate.store(true, Ordering::Relaxed);
    }
        
    pub fn wait_termination(self) {
        
        self.writer_thread.join().unwrap();
    }


    /// enters the loop of reading from buffer and writing to a file
    fn write_log_loop<T: Send + 'static>(writer: Arc<Mutex<Box<dyn Writer<T>>>>, buf: DoubleBuf<T>, terminate: Arc<AtomicBool>) {

        let mut terminated_cnt = 0;

        let mut buf_id = 0;

        loop {

            let slice: &mut [T] = buf.reserve_for_reaed(buf_id);

            let mut guard = writer.lock().unwrap();

            guard.process_slice(slice);

            let ptr = slice.as_mut_ptr();
            for i in 0..slice.len() {
                unsafe { std::ptr::drop_in_place(ptr.offset(i as isize)) }
            }

            if terminate.load(Ordering::Relaxed) {

                buf.set_buf_terminated(buf_id);

                terminated_cnt += 1;

                if terminated_cnt == buf.get_buf_cnt() {

                    guard.flush();
                    break;
                }
 
            } else {

                buf.set_buf_appendable(buf_id);
            }

            buf_id = 1 - buf_id;
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use std::path::Path;

    fn prepare(log_dir: &str) {
        if Path::new(log_dir).exists() {
            cleanup(log_dir);
        }

        std::fs::create_dir(log_dir).expect("Failed to create test dir");
    }

    fn cleanup(log_dir: &str) {
        std::fs::remove_dir_all(log_dir).expect("Failed to delete test dir on cleanup");
    }

    fn check_result(log_dir: &str, expected_file_size: usize) -> PathBuf {
        let expected_file_cnt = 1;
        let mut file_cnt = 0;
        let mut file = None;
        for entry in Path::new(log_dir).read_dir().expect("Failed to list files in test directory") {
            let ent = entry.expect("Failed to get entry inside test directory");
            let meta = ent.metadata().expect("Failed to extract log file metadata");
            assert!(meta.is_file());
            assert_eq!(expected_file_size, meta.len() as usize, "Unexpected log file size");
            file_cnt += 1;
            file = Some(ent);
        }

        assert_eq!(expected_file_cnt, file_cnt, "Expected to find just {} log file in {}", expected_file_cnt, log_dir);
        file.expect("Log file is not found").path()
    }

    #[test]
    fn test_rotation() {
        let file_size = 1024;
        let slice = "12345678".as_bytes();
        let log_dir = "/tmp/AsyncLoggerNBTest_458702014905836";

        prepare(log_dir);

        let mut fw = FileWriter::new(log_dir, file_size).expect("Failed to create file writer");

        for _ in 0..file_size / slice.len() {
            fw.process_slice(slice);
        };

        Writer::<u8>::flush(&mut fw);

        let file = check_result(log_dir, file_size);

        fw.process_slice(slice);
        Writer::<u8>::flush(&mut fw);
        std::fs::remove_file(file).expect("Failed to delete log file after rotation");

        for _ in 0..file_size / slice.len() {
            fw.process_slice(slice);
        };

        Writer::<u8>::flush(&mut fw);

        let file = check_result(log_dir, file_size);

        fw.process_slice(slice);
        Writer::<u8>::flush(&mut fw);
        std::fs::remove_file(file).expect("Failed to delete log file after rotation");

        fw.process_slice(slice);
        Writer::<u8>::flush(&mut fw);

        check_result(log_dir, slice.len());

        cleanup(log_dir);
    }
}
