//! Asyncronous logger.
//!
//! `AsyncLoggerNB` is implementation of asyncronous logger that allows writing arbitrary `u8`
//! slices to a memory buffer, and then send the buffer to a writer. 
//!
//! NB at the end of `AsyncLoggerNB` stands for non-blocking. Implementation allows adding messages to a buffer without locking
//! the buffer which prevents other threads from writing. `AsyncLoggerNB` uses pair of fixed size buffers; 
//! while one buffer is being written by the multiple threads, the second is being proccessed by the
//! single writer thread. Blocking appears when buffers change their roles.
//! Thus, this implementation is applicable in situation of small (compared to the size of buffer) writes
//! by multiple threads running on multiple cpu cores with high concurrency of writes.
//!
//! `AsyncLoggerNB` can accept any writer implementation of `Writer` trait. This package includes
//! `FileWriter` that writes data to a file. You can create your own implementation of the `Writer`
//! trait as well.
//!
//! # Examples
//!
//!     use async_logger::FileWriter;
//!     use async_logger::AsyncLoggerNB;
//!     use std::{thread, sync::Arc};
//!
//!     let writer = FileWriter::new("/tmp").expect("Failed to create file writer");
//!
//!     let logger = Arc::new(AsyncLoggerNB::new(Box::new(writer), 1024).expect("Failed to create new async logger"));
//!
//!     let write_line = "Hello, world!\n";
//!     
//!     let logger_c = logger.clone();
//!
//!     let handle = thread::spawn(move || {
//!
//!         logger_c.write_slice(write_line.as_bytes()).unwrap();
//!         logger_c.write_slice(write_line.as_bytes()).unwrap();
//!         logger_c.flush();
//!
//!         logger_c.write_slice(write_line.as_bytes()).unwrap();
//!
//!     });
//!
//!     handle.join().expect("Failed on thread join");
//!
//!     match Arc::try_unwrap(logger) {
//!         Ok(logger) => logger.terminate(),
//!         Err(_) => panic!("Failed to terminate logger because it is still in use"),
//!     };
//!
//!


mod buf;
mod writer;


use buf::DoubleBuf;
use writer::ThreadedWriter;
use std::sync::{Mutex, Arc};
pub use writer::FileWriter;



/// Writer performs data processing of a fully filled buffer.
pub trait Writer: Send {

    /// Logger calls this function when there is data to be processed.
    /// This function is guaranteed to be called sequentially; no internal syncronization is
    /// required by default.
    fn process_slice(&mut self, slice: &[u8]);

    /// Flush the remining data, and finalize writer. 
    /// This function is called only on writer thread termination.
    fn flush(&mut self);
}



/// Logger with non-blocking async processing.
pub struct AsyncLoggerNB {
    buf:    DoubleBuf,
    tw:     ThreadedWriter,
    size:   usize,
    writer: Arc<Mutex<Box<dyn Writer>>>,
    terminated: Arc<Mutex<bool>>,
}


impl AsyncLoggerNB {

    /// Create a new AsyncLoggerNB instance with buffer of buf_size.
    ///
    /// # Errors
    ///
    /// `Err` is returend if `buf_sz` is greater than `std::isize::MAX` or `buf_sz` is zero, or 
    /// if memory allocation has failed for some reason (e.g. OOM).
    ///
    /// # Panics
    ///
    /// Panics of OS fails to create thread.
    pub fn new(writer: Box<dyn Writer>, buf_sz: usize) -> Result<AsyncLoggerNB, ()> {

        let buf = DoubleBuf::new(buf_sz)?;

        let writer = Arc::new(Mutex::new(writer));

        let writer2 = writer.clone();

        let tw = ThreadedWriter::new(writer2, &buf);

        let terminated = Arc::new(Mutex::new(false));

        Ok(AsyncLoggerNB {
            buf,
            tw,
            size: buf_sz,
            writer,
            terminated,
        })
    }

    /// Flush underlying buffers, and wait until writer thread terminates. 
    /// Further attempts to write to buffers will return error.
    ///
    /// # Panics
    ///
    /// Panics if some of the internal mutexes is poisoned, or when writer thread paniced.
    pub fn terminate(self) {

        let mut guard = self.terminated.lock().unwrap();

        if ! *guard {

            self.tw.request_stop();

            self.buf.seal_buffers();

            self.tw.wait_termination();

            *guard = true;
        }
    }

    /// Write a slice of `u8`. If the size of slice is larger than buffer size then buffer is
    /// bypassed, and slice is handed directly to writer. Note, in this case message can appear
    /// out-of-order.
    /// Function blocks if there is not enough free space in buffer available.
    ///
    /// # Errors
    ///
    /// `Err` is returned when the function tries to put slice in buffer after `terminate` was called. 
    /// This is normally not expected, because `terminate` takes ownership on logger instance.
    ///
    /// # Panics
    ///
    /// This function panics if some of the internal mutexes is poisoned or when writer panics.
    pub fn write_slice(&self, slice: &[u8]) -> Result<(),()> {

        if slice.len() > self.size {

            let mut guard = self.writer.lock().unwrap();

            guard.process_slice(slice);

        } else {

            if let Ok(mut dst) = self.buf.reserve_for_write(slice.len()) {

                unsafe {
                    std::ptr::copy(slice.as_ptr(), dst.as_mut_ptr(), dst.len());
                }

            } else {

                return Err(());
            }
        }

        Ok(())
    }

    /// Mark not yet full buffer as ready for writer.
    /// This function doesn't call `Writer::flush.
    /// THis function doesn't wait while writer process all the previously written data.
    ///
    /// # Panics
    ///
    /// Panics if some of the internal mutexes is poisoned.
    pub fn flush(&self) {

        self.buf.flush();
    }
}


#[cfg(test)]
mod tests {


    use super::*;
    use std::path::Path;
    use std::io::{BufRead, BufReader};
    use std::fs::File;
    use std::thread;
    use std::sync::{Once, MutexGuard, atomic::AtomicU64, atomic::Ordering};
    use std::mem::MaybeUninit;


    const LOG_DIR: &str = "/tmp/AsyncLoggerNBTest_45870201463983";

    static mut TEST_MUTEX: MaybeUninit<Mutex<()>> = MaybeUninit::uninit();

    static INIT_MUTEX: Once = Once::new();


    fn prepare<'a>() -> MutexGuard<'a, ()> {

        INIT_MUTEX.call_once(|| {
            unsafe { TEST_MUTEX = MaybeUninit::new(Mutex::new(())) };
        });

        let mtx: &Mutex<()> = unsafe { TEST_MUTEX.as_ptr().as_ref().expect("Test mutex is not initialized") };
        let guard = mtx.lock().expect("Test mutex is poisoned");

        if Path::new(LOG_DIR).exists() {

            cleanup();
        }

        std::fs::create_dir(LOG_DIR).expect("Failed to create test dir");

        guard
    }


    fn cleanup() {

        std::fs::remove_dir_all(LOG_DIR).expect("Failed to delete test dir on cleanup");
    }


    fn get_resulting_file_path() -> String {

        String::from(Path::new(LOG_DIR)
            .read_dir()
            .expect("Failed to list files in test directory")
            .next()
            .expect("No files found in test directory")
            .expect("Failed to get entry inside test directory")
            .path()
            .to_str()
            .expect("Failed to get file path as str"))
    }


    fn spawn_threads(logger: &Arc<AsyncLoggerNB>, test_strings: &[&str], cnt: usize, flush_cnt: usize) {

        let mut handles = vec![];

        for i in 0..test_strings.len() {

            let s = String::from(test_strings[i]);

            let logger_c = logger.clone();

            let handle = thread::spawn(move || {

                for i in 0..cnt {
                    logger_c.write_slice(s.as_bytes()).unwrap();

                    if i % flush_cnt == 0 {
                        logger_c.flush();
                    }
                }
            });

            handles.push(handle);
        }

        for handle in handles {
            handle.join().expect("Failed on thread join");
        }
    }


    #[test]
    fn test_async_logger_single_thread() {

        let _guard = prepare();

        let writer = FileWriter::new(LOG_DIR).expect("Failed to create file writer");

        let writer_obj: Box<dyn Writer> = Box::new(writer);

        let buf_sz = 64;
        
        let logger = AsyncLoggerNB::new(writer_obj, buf_sz).expect("Failed to create new async logger");

        let mut cnt = 10000;

        let write_line = "Hello, world!\n";
        
        for _ in 0..cnt {
            logger.write_slice(write_line.as_bytes()).unwrap();
        }

        logger.terminate();

        let out_file = get_resulting_file_path();

        let mut reader = BufReader::new(File::open(out_file).expect("Failed to open resulting file"));

        let mut line = String::new();

        loop {

            let len = reader.read_line(&mut line).expect("Failed to read line from the reslting file");

            if len == 0 {

                break;
            }

            assert_eq!(write_line, line);

            line.clear();

            cnt -= 1;
        }
        
        cleanup();
    }


    fn run_threaded_test(test_strings: &[&str], buf_sz: usize, iter_cnt: usize, flush_cnt: usize) {

        let writer = FileWriter::new(LOG_DIR).expect("Failed to create file writer");

        let writer_obj: Box<dyn Writer> = Box::new(writer);

        let logger = Arc::new(AsyncLoggerNB::new(writer_obj, buf_sz).expect("Failed to create new async logger"));

        spawn_threads(&logger, &test_strings, iter_cnt, flush_cnt);

        match Arc::try_unwrap(logger) {
            Ok(logger) => logger.terminate(),
            Err(_) => panic!("Failed to terminate logger because it is still used"),
        };

        let out_file = get_resulting_file_path();

        let mut reader = BufReader::new(File::open(out_file).expect("Failed to open resulting file"));

        let mut line = String::new();

        let mut test_strings_hm = std::collections::HashMap::new();

        for x in test_strings.iter() { test_strings_hm.insert(String::from(*x), 0); };

        loop {

            let len = reader.read_line(&mut line).expect("Failed to read line from the reslting file");

            if len == 0 {

                break;
            }

            *test_strings_hm.get_mut(&line).expect(&format!("The line is not recognized: {}", line)) += 1;

            line.clear();
        }

        test_strings_hm.iter().for_each( |(line, cnt)| {
            assert_eq!(*cnt, iter_cnt, "Resulting file contains {} lines \"{}\", but expected {}", cnt, line, iter_cnt);
        });
    }


    #[test]
    fn test_async_logger_multiple_threads() {

        let _guard = prepare();

        let test_strings = [
            "aAaAaA AaAa 0\n",
            "bBbBbB BbBbB 1\n",
            "CcCcCcC cCcCcC 2\n",
            "DdDdD dDDDdDdDd 3\n",
            "eEeEeEe eEeEeEe E 4\n",
            "FfFf FfFf FfFfFfFf 5\n",
            "gGgGg GgGgG gGgGgGg 6\n",
            "HhHhHhHhHhH hHhHhHhHh 7\n",
            "IiIiIiI IiIiIiI iIiIiI 8\n",
            "jJjJ jJjJjJ jJjJjJjJjjJ 9\n",
        ];

        let buf_sz = 64;

        let iter_cnt = 1000;
        
        run_threaded_test(&test_strings, buf_sz, iter_cnt, iter_cnt + 1);
      
        cleanup();
    }


    #[test]
    fn test_async_logger_large_msg() {

        let _guard = prepare();

        let test_strings = [
            "aAaAaA AaAa 0\n",
            "bBbBbB BbBbB 1\n",
            "CcCcCcC cCcCcC 2\n",
            "DdDdD dDDDdDdDd 3\n",
            "eEeEeEe eEeEeEe E 4 eEeEeEe eEeEeEe E 4 eEeEeEe eEeEeEe E 4 eEeEeEe eEeEeEe E 4\n",
            "FfFf FfFf FfFfFfFf 5\n",
            "gGgGg GgGgG gGgGgGg 6\n",
            "HhHhHhHhHhH hHhHhHhHh 7\n",
            "IiIiIiI IiIiIiI iIiIiI 8\n",
            "jJjJ jJjJjJ jJjJjJjJjjJ 9 jJjJ jJjJjJ jJjJjJjJjjJ 9 jJjJ jJjJjJ jJjJjJjJjjJ 9\n",
        ];

        let buf_sz = 64;

        let iter_cnt = 1000;

        run_threaded_test(&test_strings, buf_sz, iter_cnt, iter_cnt + 1);

        cleanup();
    }

    #[test]
    fn test_flush() {

        let _guard = prepare();

        let test_strings = [
            "aAaAaA AaAa 0\n",
            "bBbBbB BbBbB 1\n",
            "CcCcCcC cCcCcC 2\n",
            "DdDdD dDDDdDdDd 3\n",
            "eEeEeEe eEeEeEe E 4\n",
            "FfFf FfFf FfFfFfFf 5\n",
            "gGgGg GgGgG gGgGgGg 6\n",
            "HhHhHhHhHhH hHhHhHhHh 7\n",
            "IiIiIiI IiIiIiI iIiIiI 8\n",
            "jJjJ jJjJjJ jJjJjJjJjjJ 9\n",
        ];

        let buf_sz = 64;

        let iter_cnt = 1000;
        
        run_threaded_test(&test_strings, buf_sz, iter_cnt, iter_cnt / 20);
      
        cleanup();
    }

    struct WriterTest {
        flush_cnt: Arc<AtomicU64>,
        slice_cnt: Arc<AtomicU64>,
    }

    impl Writer for WriterTest {

        fn process_slice(&mut self, _slice: &[u8]) {
            self.slice_cnt.fetch_add(1, Ordering::Relaxed);
        }

        fn flush(&mut self) {
            self.flush_cnt.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[test]
    fn test_flush2() {

        let write_line = "abc";

        let buf_sz = 1024;
        let flush_cnt = Arc::new(AtomicU64::new(0));
        let slice_cnt = Arc::new(AtomicU64::new(0));

        let writer = WriterTest {
            flush_cnt: flush_cnt.clone(),
            slice_cnt: slice_cnt.clone(),
        };

        let writer_obj: Box<dyn Writer> = Box::new(writer);

        let logger = Arc::new(AsyncLoggerNB::new(writer_obj, buf_sz).expect("Failed to create new async logger"));

        logger.write_slice(write_line.as_bytes()).unwrap();
        logger.write_slice(write_line.as_bytes()).unwrap();
        logger.flush();

        logger.write_slice(write_line.as_bytes()).unwrap();
        logger.write_slice(write_line.as_bytes()).unwrap();
        logger.flush();

        match Arc::try_unwrap(logger) {
            Ok(logger) => logger.terminate(),
            Err(_) => panic!("Failed to terminate logger because it is still used"),
        };

        assert_eq!(1, flush_cnt.load(Ordering::Relaxed), "Flush count doesnt match");

        let slice_cnt = slice_cnt.load(Ordering::Relaxed);
        assert!(2 <= slice_cnt && 4 >= slice_cnt, "Slice count has unexpected value {}", slice_cnt);
    }
}
