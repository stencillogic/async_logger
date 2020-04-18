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
}


impl FileWriter {

    /// Creates a `FileWriter` which will write to a file located in `log_dir` directory.
    /// The file name has form `log_<unix_timestamp_seconds>.txt`.
    pub fn new(log_dir: &str) -> Result<FileWriter, Error> {

        let epoch_secs = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map_err(|e| { Error::new(ErrorKind::TimeError, ErrorRepr::TimeError(e)) })?
            .as_secs();

        let file_name = format!("log_{}.txt", epoch_secs);

        let mut file_path = PathBuf::new();

        file_path.push(log_dir);
        file_path.push(file_name);

        let file_path = file_path
            .to_str()
            .ok_or(Error::new(ErrorKind::PathToStrConversionError, ErrorRepr::Simple))?;

        let f = std::fs::OpenOptions::new()
            .append(true)
            .create_new(true)
            .open(file_path)
            .map_err(|e| { Error::new(ErrorKind::IoError, ErrorRepr::IoError(e)) })?;

        Ok(FileWriter {
            f,
        })
    }
}


impl Writer<u8> for FileWriter {

    fn process_slice(&mut self, slice: &[u8]) {

        let _ret = self.f.write_all(&slice);
    }

    fn flush(&mut self) {

        let _ret = self.f.flush();
    }
}


impl Writer<Box<String>> for FileWriter {

    fn process_slice(&mut self, slice: &[Box<String>]) {

        for item in slice {
            let _ret = self.f.write_all(item.as_bytes());
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
