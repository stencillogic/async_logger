//! Asynchronous logger.
//!
//! `AsyncLoggerNB` is implementation of asynchronous logger that allows writing arbitrary 
//! slices or individual values to a memory buffer, and then send the buffer to a writer. 
//!
//! NB at the end of `AsyncLoggerNB` stands for non-blocking. Implementation allows adding messages to a buffer without locking
//! the buffer which prevents other threads from waiting. `AsyncLoggerNB` uses pair of fixed size buffers; 
//! while one buffer is being written by the multiple threads, the second is being proccessed by the
//! single writer thread. Blocking appears when buffers change their roles.
//! Thus, this implementation is applicable in situation of small (compared to the size of buffer) writes
//! by multiple threads running on multiple cpu cores with high concurrency of writes.
//!
//! `AsyncLoggerNB` can process serialized data (stream of bytes) or custom complex data structures, references to objects.
//!
//! `AsyncLoggerNB` can accept any writer implementation of `Writer` trait. This package includes
//! `FileWriter` that writes data to a file. You can create your own implementation of the `Writer`
//! trait as well.
//!
//! Implementation of [log](https://docs.rs/log) facade based on this asynchronous logger is available as separate crate
//! [async_logger_log](https://docs.rs/async_logger_log). Please refer to `async_logger_log` crate documentation for more info and examples.
//!
//! # Examples
//!
//! ```
//! use async_logger::FileWriter;
//! use async_logger::AsyncLoggerNB;
//! use std::{thread, sync::Arc};
//!
//! let writer = FileWriter::new("/tmp", 10*1024*1024).expect("Failed to create file writer");
//!
//! let logger = Arc::new(AsyncLoggerNB::new(Box::new(writer), 8192)
//!     .expect("Failed to create new async logger"));
//!
//! let write_line = "Hello, world!\n";
//! 
//! let logger_c = logger.clone();
//!
//! let handle = thread::spawn(move || {
//!
//!     logger_c.write_slice(write_line.as_bytes()).unwrap();
//!     logger_c.write_slice(write_line.as_bytes()).unwrap();
//!     logger_c.flush();
//!
//!     logger_c.write_slice(write_line.as_bytes()).unwrap();
//!
//! });
//!
//! handle.join().expect("Failed on thread join");
//!
//! match Arc::try_unwrap(logger) {
//!     Ok(logger) => logger.terminate(),
//!     Err(_) => panic!("Failed to terminate logger because it is still in use"),
//! };
//! ```
//!
//! When you know size of data to be written in beforehand it may be more efficient to write data
//! directly to the logger buffer. For that case `AsyncLoggerNB::reserve_slice` can be used:
//!
//! ```
//! use async_logger::{FileWriter, AsyncLoggerNB, Writer};
//!
//! // implement some custom writer along the way
//! struct Stub {}
//! impl Writer<u8> for Stub {
//!     fn process_slice(&mut self, slice: &[u8]) {
//!         for item in slice {
//!             println!("{}", item);
//!         }
//!     }
//!     fn flush(&mut self) {}
//! }
//!
//! let logger = AsyncLoggerNB::new(Box::new(Stub {}), 8192)
//!     .expect("Failed to create new async logger");
//!
//! // getting slice for writing
//! let mut slice = logger.reserve_slice(10).unwrap();
//!
//! assert_eq!(10, slice.len());
//!
//! // write to the logger buffer directly
//! for i in 0..10 {
//!     slice[i] = (i*i) as u8;
//! }
//!
//! drop(slice);    // release the buffer
//!
//! ```
//!
//! Sometimes it is more efficient to write a pointer to some existing instance of struct instead
//! of copying the complete object. This can be achieved by moving boxed reference to an object to
//! `AsyncLoggerNB::write_value`. See the documentation of the function 
//! [write_value](struct.AsyncLoggerNB.html#method.write_value) for details and example.
//!
//! # Performance
//!
//! Recommended buffer size is to let holding from tens to hundreds of
//! messages. Choosing too small size leads to performance degradation. And choosing too big size
//! doesn't increase performance significantly but leads to resource waste. 
//!
//! 65536 bytes is typical size.
//!
//! ### Performance tests
//!
//! Tests show that this non-blocking implementation is at least not slower than comparable
//! implementation with mutex, and can be two times faster under highly competitive load.
//!
//! ### Metrics
//!
//! `AsyncLoggerNB` collects total time spent by threads waiting for free buffer space in nanoseconds,
//! and total count of wait events. 
//! Metrics collection is enabled at compile time with feature `metrics`.
//! After enabling metrics you can use `AsyncLoggerNB::get_metrics` to get the current metrics values.
//! Note, the metrics values can wrap around after significant amount of time of running without
//! interruption.
//!
//! # Notes
//!
//! Attempting to get several instances of `Byte` struct at the same time in the same thread can cause deadlock.

mod buf;
mod writer;


use buf::DoubleBuf;
use writer::ThreadedWriter;
use std::sync::{Mutex, Arc};
pub use writer::FileWriter;
pub use buf::Metrics;
pub use buf::Slice;


/// Writer performs data processing of a fully filled buffer.
pub trait Writer<T: Send + 'static>: Send {

    /// Logger calls this function when there is data to be processed.
    /// This function is guaranteed to be called sequentially; no internal synchronization is
    /// required by default.
    fn process_slice(&mut self, slice: &[T]);

    /// Flush the remining data, and finalize writer. 
    /// This function is called only on writer thread termination.
    fn flush(&mut self);
}



/// Logger with non-blocking async processing.
pub struct AsyncLoggerNB<T: Send + 'static> {
    buf:    DoubleBuf<T>,
    tw:     ThreadedWriter,
    writer: Arc<Mutex<Box<dyn Writer<T>>>>,
    terminated: Arc<Mutex<bool>>,
    threshold:  usize,
}


impl<T: Send + 'static> AsyncLoggerNB<T> {

    /// Create a new AsyncLoggerNB instance with buffer of buf_size items.
    ///
    /// # Errors
    ///
    /// `Err` is returend if `buf_sz` is greater than `std::isize::MAX` or `buf_sz` is zero or when
    /// `T` has size of zero, or when memory allocation has failed for some reason (e.g. OOM).
    ///
    /// # Panics
    ///
    /// Panics of OS fails to create thread.
    pub fn new(writer: Box<dyn Writer<T>>, buf_sz: usize) -> Result<AsyncLoggerNB<T>, Error> {

        let buf = DoubleBuf::<T>::new(buf_sz)?;

        let writer = Arc::new(Mutex::new(writer));

        let writer2 = writer.clone();

        let tw = ThreadedWriter::new(writer2, &buf);

        let terminated = Arc::new(Mutex::new(false));

        let threshold = buf_sz - buf_sz / 5;

        Ok(AsyncLoggerNB {
            buf,
            tw,
            writer,
            terminated,
            threshold,
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

    /// Write a slice of `<T>`. If the size of slice is larger or equal to 0.8 * buffer_size then buffer is
    /// bypassed, and slice is handed directly to writer. Note, in this case message can appear
    /// out-of-order.
    /// Function blocks if message size is less than 0.8 * buffer_size, and there is not enough free space in any of buffers. 
    /// As soon as there is free space larger than 0.8 * buffer_size available slice is written and function returns.
    ///
    /// # Errors
    ///
    /// `Err` is returned when the function tries to put slice in buffer after `terminate` was called. 
    /// This is normally not expected, because `terminate` takes ownership on logger instance.
    ///
    /// # Panics
    ///
    /// This function panics if some of the internal mutexes is poisoned or when writer thread panics.
    pub fn write_slice(&self, slice: &[T]) -> Result<(),()> where T: Copy {

        if slice.len() >= self.threshold {

            let mut guard = self.writer.lock().unwrap();

            guard.process_slice(slice);

        } else {

            self.buf.write_slice(slice)?;
        }

        Ok(())
    }


    /// This function is similar to `write_slice` but instead of pushing some slice to buffer it allows
    /// reserving some space for writing directly in the underlying destination buffer. This way excessive
    /// copy operation from the slice to the internal buffer can be avoided.
    /// Thus, this function is more preferable than `write_slice` but is applicable only when you know the size of the slice you need
    /// beforehand. When the size of the slice doesn't matter use `reserve_slice_relaxed`.
    ///
    /// The function returns `Slice` struct that can be dereferenced as mutable slice of `<T>`. The
    /// client code can use the dereferenced slice to write to it. The client code holds the buffer
    /// until `Slice` instance goes out of scope or is explicitly dropped with `drop`. That
    /// means client's code must take care of not holding the returned `Slice` instance for too long
    /// because it can block other threads.
    /// 
    /// # Errors
    ///
    /// If the `reserve_size` is larger or equal to 0.8 * buffer_size then `Err` is returned with
    /// `ErrorKind::RequestedSizeIsTooLong`.
    ///
    /// `Err` is also returned when the function is called after `terminate` was called, but
    /// this is normally not expected, because `terminate` takes ownership on logger instance.
    ///
    /// # Panics
    ///
    /// This function panics if some of the internal mutexes is poisoned or when writer thread panics.
    pub fn reserve_slice(&self, reserve_size: usize) -> Result<Slice<T>,Error> where T: Copy {

        if reserve_size >= self.threshold {
            return Err(Error::new(ErrorKind::RequestedSizeIsTooLong, ErrorRepr::Simple));
        } else {
            return self.buf.reserve_slice(reserve_size, false);
        }
    }


    /// This function is similar to `reserve_slice` but returned `Slice` struct can have length  
    /// from 1 item, and up to `reserve_size` items.
    ///
    /// # Errors
    ///
    /// `Err` is returned when the function is called after `terminate` was called.
    /// This is normally not expected, because `terminate` takes ownership on logger instance.
    ///
    /// # Panics
    ///
    /// This function panics if some of the internal mutexes is poisoned or when writer thread panics.
    #[inline]
    pub fn reserve_slice_relaxed(&self, reserve_size: usize) -> Result<Slice<T>,()> {

        return self.buf.reserve_slice(reserve_size, true).map_err(|_| {()});
    }

    /// Write a value of type `<T>`. This method can be used for writing values that do not
    /// implement `Copy` trait, e.g. `String`, or pointer to a string `Box<String>`. The function
    /// takes ownership of the argument. After the argument is processed by writer `drop` for it is
    /// called automatically.
    /// 
    /// Function blocks if there is not enough free space in any of buffers. 
    /// As soon as there is free space available value is written and function returns.
    ///
    /// # Errors
    ///
    /// `Err` is returned when the function tries to put value in buffer after `terminate` was called. 
    /// This is normally not expected, because `terminate` takes ownership on logger instance.
    ///
    /// # Panics
    ///
    /// This function panics if some of the internal mutexes is poisoned or when writer thread panics.
    ///
    /// # Examples
    ///
    /// ```
    /// use async_logger::{FileWriter, AsyncLoggerNB, Writer};
    ///
    /// // implement some custom writer along the way
    /// struct Stub {}
    /// impl Writer<Box<String>> for Stub {
    ///     fn process_slice(&mut self, slice: &[Box<String>]) {}
    ///     fn flush(&mut self) {}
    /// }
    ///
    /// let writer_obj: Box<dyn Writer<Box<String>>> = Box::new(Stub {});
    ///
    /// let logger = AsyncLoggerNB::new(Box::new(Stub {}), 8192)
    ///     .expect("Failed to create new async logger");
    ///
    /// let string_ptr = Box::new("test message".to_owned());
    /// logger.write_value(string_ptr).unwrap();
    ///
    /// ```
    pub fn write_value(&self, value: T) -> Result<(),()> {
        let slice = [value];
        self.buf.write_slice(&slice)?;
        std::mem::forget(slice);
        Ok(())
    }

    /// Mark not yet full buffer as ready for writer.
    /// This function doesn't call `Writer::flush`.
    /// This function doesn't wait while writer process all the previously written data.
    ///
    /// # Panics
    ///
    /// Panics if some of the internal mutexes is poisoned.
    pub fn flush(&self) {

        self.buf.flush();
    }


    /// Return current values of performance metrics, e.g. wait event information.
    pub fn get_metrics(&self) -> Metrics {
        self.buf.get_metrics()
    }
}

/// Errors returned by the crate functions.
#[derive(Debug)]
pub struct Error {
    kind: ErrorKind,
    repr: ErrorRepr
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_string())
    }
}

impl Error {

    fn new(kind: ErrorKind, repr: ErrorRepr) -> Error {
        Error {
            kind,
            repr
        }
    }

    /// For kind IoError return associated io error.
    pub fn io_err(self) -> Option<std::io::Error> {
        match self.repr {
            ErrorRepr::IoError(e) => Some(e),
            _ => None
        }
    }

    /// For kind TimeError return associated time error.
    pub fn time_err(self) -> Option<std::time::SystemTimeError> {
        match self.repr {
            ErrorRepr::TimeError(e) => Some(e),
            _ => None
        }
    }

    /// For kind MemoryLayoutError return associated memory layout error.
    pub fn layout_err(self) -> Option<std::alloc::LayoutErr> {
        match self.repr {
            ErrorRepr::MemoryLayoutError(e) => Some(e),
            _ => None
        }
    }

    /// Returns kind of error.
    pub fn kind(&self) -> ErrorKind {
        self.kind
    }
}

impl std::error::Error for Error { }


/// Error kinds.
#[derive(Debug, PartialEq, Copy, Clone)]
pub enum ErrorKind {
    PathToStrConversionError,
    TimeError,
    IoError,
    IncorrectBufferSize,
    AllocFailure,
    MemoryLayoutError,
    LoggerIsTerminated,
    RequestedSizeIsTooLong,
}

#[derive(Debug)]
enum ErrorRepr {
    Simple,
    IoError(std::io::Error),
    TimeError(std::time::SystemTimeError),
    MemoryLayoutError(std::alloc::LayoutErr),
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
    use std::collections::HashMap;


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


    fn spawn_threads<T: Send + Sync + Clone + Copy + 'static>(logger: &Arc<AsyncLoggerNB<T>>, test_strings: &[&'static [T]], cnt: usize, flush_cnt: usize) {

        let mut handles = vec![];

        for i in 0..test_strings.len() {

            let s = test_strings[i];

            let logger_c = logger.clone();

            let handle = thread::spawn(move || {

                for i in 1..cnt+1 {
                    if i & 0x1 == 0 {
                        logger_c.write_slice(&s).unwrap();
                    } else {
                        match logger_c.reserve_slice(s.len()) {
                            Ok(mut bytes) => {
                                let dst = &mut bytes;
                                dst.copy_from_slice(&s);
                                drop(bytes);
                            },
                            Err(e) => {
                                if e.kind() == ErrorKind::RequestedSizeIsTooLong {
                                    logger_c.write_slice(&s).unwrap();
                                } else {
                                    panic!("Unexpected error: {:?}", e);
                                }
                            }
                        }
                    }

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

    ///
    /// tests for u8 slices
    ///

    #[test]
    fn test_async_logger_single_thread() {

        let _guard = prepare();

        let writer = FileWriter::new(LOG_DIR, std::usize::MAX).expect("Failed to create file writer");

        let writer_obj: Box<dyn Writer<u8>> = Box::new(writer);

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


    fn run_threaded_test(test_strings: &'static [&[u8]], buf_sz: usize, iter_cnt: usize, flush_cnt: usize) {

        let writer = FileWriter::new(LOG_DIR, std::usize::MAX).expect("Failed to create file writer");

        let writer_obj: Box<dyn Writer<u8>> = Box::new(writer);

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

        for x in test_strings.iter() { test_strings_hm.insert(std::str::from_utf8(*x).unwrap().to_owned(), 0); };

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

        static TEST_STRINGS: [&[u8]; 10] = [
            b"aAaAaA AaAa 0\n",
            b"bBbBbB BbBbB 1\n",
            b"CcCcCcC cCcCcC 2\n",
            b"DdDdD dDDDdDdDd 3\n",
            b"eEeEeEe eEeEeEe E 4\n",
            b"FfFf FfFf FfFfFfFf 5\n",
            b"gGgGg GgGgG gGgGgGg 6\n",
            b"HhHhHhHhHhH hHhHhHhHh 7\n",
            b"IiIiIiI IiIiIiI iIiIiI 8\n",
            b"jJjJ jJjJjJ jJjJjJjJjjJ 9\n",
        ];

        let buf_sz = 64;

        let iter_cnt = 1000;
        
        run_threaded_test(&TEST_STRINGS, buf_sz, iter_cnt, iter_cnt + 1);
      
        cleanup();
    }


    #[test]
    fn test_async_logger_large_msg() {

        let _guard = prepare();

        static TEST_STRINGS: [&[u8]; 10] = [
            b"aAaAaA AaAa 0\n",
            b"bBbBbB BbBbB 1\n",
            b"CcCcCcC cCcCcC 2\n",
            b"DdDdD dDDDdDdDd 3\n",
            b"eEeEeEe eEeEeEe E 4 eEeEeEe eEeEeEe E 4 eEeEeEe eEeEeEe E 4 eEeEeEe eEeEeEe E 4\n",
            b"FfFf FfFf FfFfFfFf 5\n",
            b"gGgGg GgGgG gGgGgGg 6\n",
            b"HhHhHhHhHhH hHhHhHhHh 7\n",
            b"IiIiIiI IiIiIiI iIiIiI 8\n",
            b"jJjJ jJjJjJ jJjJjJjJjjJ 9 jJjJ jJjJjJ jJjJjJjJjjJ 9 jJjJ jJjJjJ jJjJjJjJjjJ 9\n",
        ];

        let buf_sz = 64;

        let iter_cnt = 1000;

        run_threaded_test(&TEST_STRINGS, buf_sz, iter_cnt, iter_cnt + 1);

        cleanup();
    }

    #[test]
    fn test_flush() {

        let _guard = prepare();

        static TEST_STRINGS: [&[u8]; 10] = [
            b"aAaAaA AaAa 0\n",
            b"bBbBbB BbBbB 1\n",
            b"CcCcCcC cCcCcC 2\n",
            b"DdDdD dDDDdDdDd 3\n",
            b"eEeEeEe eEeEeEe E 4\n",
            b"FfFf FfFf FfFfFfFf 5\n",
            b"gGgGg GgGgG gGgGgGg 6\n",
            b"HhHhHhHhHhH hHhHhHhHh 7\n",
            b"IiIiIiI IiIiIiI iIiIiI 8\n",
            b"jJjJ jJjJjJ jJjJjJjJjjJ 9\n",
        ];

        let buf_sz = 64;

        let iter_cnt = 1000;
        
        run_threaded_test(&TEST_STRINGS, buf_sz, iter_cnt, iter_cnt / 20);
      
        cleanup();
    }

    struct WriterTest {
        flush_cnt: Arc<AtomicU64>,
        slice_cnt: Arc<AtomicU64>,
    }

    impl<T: Send + Clone + 'static> Writer<T> for WriterTest {

        fn process_slice(&mut self, _slice: &[T]) {
            self.slice_cnt.fetch_add(1, Ordering::Relaxed);
        }

        fn flush(&mut self) {
            self.flush_cnt.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn test_flush2<T: Send + Clone + Copy + 'static>(write_line: &[T]) {

        let buf_sz = 1024;
        let flush_cnt = Arc::new(AtomicU64::new(0));
        let slice_cnt = Arc::new(AtomicU64::new(0));

        let writer = WriterTest {
            flush_cnt: flush_cnt.clone(),
            slice_cnt: slice_cnt.clone(),
        };

        let writer_obj: Box<dyn Writer<T>> = Box::new(writer);

        let logger = Arc::new(AsyncLoggerNB::new(writer_obj, buf_sz).expect("Failed to create new async logger"));

        logger.write_slice(write_line).unwrap();
        logger.write_slice(write_line).unwrap();
        logger.flush();

        logger.write_slice(write_line).unwrap();
        logger.write_slice(write_line).unwrap();
        logger.flush();

        match Arc::try_unwrap(logger) {
            Ok(logger) => logger.terminate(),
            Err(_) => panic!("Failed to terminate logger because it is still used"),
        };

        assert_eq!(1, flush_cnt.load(Ordering::Relaxed), "Flush count doesnt match");

        let slice_cnt = slice_cnt.load(Ordering::Relaxed);
        assert!(2 <= slice_cnt && 4 >= slice_cnt, "Slice count has unexpected value {}", slice_cnt);
    }

    #[test]
    fn test_flush2_u8() {
        let write_line: &[u8] = b"abc";
        test_flush2(write_line);
    }

    ///
    /// Heavy concurrency test
    ///

    struct StubWriter {
        counters: [u64; 4],
        lengths: [usize; 4],
    }

    impl Writer<u8> for StubWriter {
        fn process_slice(&mut self, slice: &[u8]) {
            let mut p = 0;
            while p<slice.len() {
                let l = (slice[p] - 49) as usize;
                if l > 3 {
                    println!("l = {}, p = {}, slice = {}", l, p, String::from_utf8_lossy(slice));
                }
                self.counters[l] += 1;
                p += self.lengths[l];
            }
        }

        fn flush(&mut self) {
            for i in 0..self.counters.len() {
                println!("counter {}: {}", i, self.counters[i]);
            }
        }
    }

    #[ignore]
    #[test]
    fn heavy_concurrency_test() {

        let test_strings: &[&[u8]] = &[
            b"1[INFO module_x]: testing message, thread #",
            b"2[INFO module_y]: testing message for thread #",
            b"3[INFO module_z]: another one message for thread #",
            b"4[INFO module_o]: a long long long long long long long long long long long long message for therad #",
        ];

        let lengths = [
            test_strings[0].len(),
            test_strings[1].len(),
            test_strings[2].len(),
            test_strings[3].len(),
        ];

        let buf_sz = 8192 * 8;

        let iter_cnt = 10000000;

        let writer_obj: Box<dyn Writer<u8>> = Box::new(StubWriter {counters: [0u64;4], lengths});

        let logger = Arc::new(AsyncLoggerNB::new(writer_obj, buf_sz).expect("Failed to create new async logger"));

        for i in 1..25+1 {
            spawn_threads(&logger, &test_strings, iter_cnt, iter_cnt/100);
            println!("{:?}", logger.get_metrics());
            println!("{}", i);
        }

        match Arc::try_unwrap(logger) {
            Ok(logger) => logger.terminate(),
            Err(_) => panic!("Failed to terminate logger because it is still used"),
        };
    }


    ///
    /// tests for u32 and u64 and str slices
    ///

    #[test]
    fn test_flush2_u64() {
        static WRITE_LINE: [u64; 10] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        test_flush2(&WRITE_LINE);
    }
    struct IntWriter<T> {
        pub counters: Arc<HashMap<T, AtomicU64>>,
        pub lengths: HashMap<T, usize>,
    }

    impl<T: Clone + Sync + Send + Copy + 'static + Eq + std::hash::Hash> Writer<T> for IntWriter<T> {
        fn process_slice(&mut self, slice: &[T]) {
            let mut p = 0;
            while p<slice.len() {
                let l = slice[p];
                (*self.counters).get(&l).unwrap().fetch_add(1, Ordering::Relaxed);
                p += self.lengths.get(&l).unwrap();
            }
        }

        fn flush(&mut self) { }
    }


    fn test_async_logger_param<T: Sync + Clone + Copy + Send + 'static + Eq + std::hash::Hash>(test_strings: &[&'static [T]]) {

        let mut lengths = HashMap::new();
        for i in 0..4 {
            lengths.insert(test_strings[i][0], test_strings[i].len());
        }

        let buf_sz = 1024;

        let iter_cnt = 10000;

        let mut counters = HashMap::new();

        for i in 0..4 {
            counters.insert(test_strings[i][0], AtomicU64::new(0));
        }

        let counters = Arc::new(counters);

        let writer_obj: Box<dyn Writer<T>> = Box::new(IntWriter {counters: counters.clone(), lengths});

        let logger = Arc::new(AsyncLoggerNB::new(writer_obj, buf_sz).expect("Failed to create new async logger"));

        for _ in 1..10 {
            spawn_threads(&logger, test_strings, iter_cnt, iter_cnt/100);
        }

        match Arc::try_unwrap(logger) {
            Ok(logger) => logger.terminate(),
            Err(_) => panic!("Failed to terminate logger because it is still used"),
        };
    }

    #[test]
    fn test_async_logger_u32() {
        static TEST_STRINGS: &[&[u32]] = &[
            &[1, 502, 504, 5, 6, 101, 102, 103, 65536, 1000000000],
            &[2, 7, 8, 9, 10, 11, 12, 13, 14, std::u32::MAX-2, 60, 61, 62, 63, 64, 65],
            &[3, std::u32::MAX-3, 16, 17, 18, std::u32::MAX-3, 20],
            &[4, 21, 22, 23, 24, 25, std::u32::MAX-4],
        ];

        test_async_logger_param(TEST_STRINGS);
    }


    #[test]
    fn test_async_logger_u64() {
        static TEST_STRINGS: &[&[u64]] = &[
            &[1, 502, 504, 5, 6, 101, 102, 103, 65536, 5000000000],
            &[2, 7, 8, 9, 10, 11, 12, 13, 14, std::u64::MAX-2, 60, 61, 62, 63, 64, 65],
            &[3, std::u64::MAX-3, 16, 17, 18, std::u64::MAX-3, 20],
            &[4, 21, 22, 23, 24, 25, std::u64::MAX-4],
        ];

        test_async_logger_param(TEST_STRINGS);
    }

    #[test]
    fn test_async_logger_str() {
        static TEST_STRINGS: &[&[&str]] = &[
            &["1", "test"],
            &["2",],
            &["3", "test 3", "test test 3", "test 3 tst", ""],
            &["4", "verdurenoj", "propergertulopus"],
        ];

        test_async_logger_param(TEST_STRINGS);
    }

    ///
    /// Writing boxed strings
    /// 
    struct StringWriter {
        pub counters: Arc<HashMap<String, AtomicU64>>,
    }

    impl Writer<Box<String>> for StringWriter {
        fn process_slice(&mut self, slice: &[Box<String>]) {
            let mut p = 0;
            while p<slice.len() {
                let l: &String = &(slice[p]);
                match (*self.counters).get(l) {
                    Some(c) => { c.fetch_add(1, Ordering::Relaxed); },
                    None => panic!("wrong val {}, {}, {:?}", l, p, slice)
                };
                p += 1;
            }
        }

        fn flush(&mut self) { }
    }

    fn write_complete_slice_boxed(logger_c: &Arc<AsyncLoggerNB<Box<String>>>, s: &[&str]) {

        for j in 0..s.len() {
            logger_c.write_value(Box::new(s[j].to_owned())).unwrap();
        }
    }

    fn spawn_threads_string(logger: &Arc<AsyncLoggerNB<Box<String>>>, test_strings: &'static [&'static [&str]], cnt: usize, flush_cnt: usize) {

        let mut handles = vec![];

        for i in 0..test_strings.len() {

            let s = test_strings[i];

            let logger_c = logger.clone();

            let handle = thread::spawn(move || {

                for l in 1..cnt+1 {

                    write_complete_slice_boxed(&logger_c, s);

                    if l % flush_cnt == 0 {
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


    fn test_async_logger_boxed(test_strings: &'static [&'static [&str]]) {

        let buf_sz = 1024;

        let iter_cnt = 10000;

        let mut counters = HashMap::new();

        for i in 0..test_strings.len() {
            for j in 0..test_strings[i].len() {
                counters.insert(String::from(test_strings[i][j]), AtomicU64::new(0));
            }
        }

        let counters = Arc::new(counters);

        let writer_obj: Box<dyn Writer<Box<String>>> = Box::new(StringWriter {counters: counters.clone()});

        let logger = Arc::new(AsyncLoggerNB::new(writer_obj, buf_sz).expect("Failed to create new async logger"));

        for _ in 1..10+1 {
            spawn_threads_string(&logger, test_strings, iter_cnt, iter_cnt/100);
        }

        match Arc::try_unwrap(logger) {
            Ok(logger) => logger.terminate(),
            Err(_) => panic!("Failed to terminate logger because it is still used"),
        };

        for (k,v) in counters.iter() {
            assert_eq!(iter_cnt*10, v.load(Ordering::Relaxed) as usize, "Counter for value {} doesn't match", k);
        }
    }

    #[test]
    fn test_async_logger_box() {
        static TEST_STRINGS: &[&[&str]] = &[
            &["line 1", "test"],
            &["line 2",],
            &["line 3", "test 3", "test test 3", "test 3 tst", ""],
            &["line 4", "verdurenoj", "propergertulopus"],
        ];

        test_async_logger_boxed(TEST_STRINGS);
    }
}
