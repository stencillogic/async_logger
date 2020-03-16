# async_logger

![Rust](https://github.com/stencillogic/async_logger/workflows/Rust/badge.svg)

Asyncronous logger allows writing arbitrary u8 slices to a memory buffer, that then processed by a writer in it's own thread.
See [crate documentation](https://docs.rs/async_logger/) for more info.

## Intro

`AsyncLoggerNB` is implementation of asyncronous logger that allows writing arbitrary `u8`
slices to a memory buffer, and then send the buffer to a writer. 

NB at the end of `AsyncLoggerNB` stands for non-blocking. Implementation allows adding messages to a buffer without locking
the buffer which prevents other threads from writing. `AsyncLoggerNB` uses pair of fixed size buffers; 
while one buffer is being written by the multiple threads, the second is being proccessed by the
single writer thread. Blocking appears when buffers change their roles.
Thus, this implementation is applicable in situation of small (compared to the size of buffer) writes
by multiple threads running on multiple cpu cores with high concurrency of writes.

`AsyncLoggerNB` can accept any writer implementation of `Writer` trait. This package includes
`FileWriter` that writes data to a file. You can create your own implementation of the `Writer`
trait as well.

Implementation of [log](https://docs.rs/log) facade based on this asyncronous logger is available as separate crate
[async_logger_log](https://docs.rs/async_logger_log). Please refer to `async_logger_log` crate documentation for more info and examples.

## Examples

```
use async_logger::FileWriter;
use async_logger::AsyncLoggerNB;
use std::{thread, sync::Arc};

let writer = FileWriter::new("/tmp").expect("Failed to create file writer");

let logger = Arc::new(AsyncLoggerNB::new(Box::new(writer), 1024)
    .expect("Failed to create new async logger"));

let write_line = "Hello, world!\n";

let logger_c = logger.clone();

let handle = thread::spawn(move || {

    logger_c.write_slice(write_line.as_bytes()).unwrap();
    logger_c.write_slice(write_line.as_bytes()).unwrap();
    logger_c.flush();

    logger_c.write_slice(write_line.as_bytes()).unwrap();

});

handle.join().expect("Failed on thread join");

match Arc::try_unwrap(logger) {
    Ok(logger) => logger.terminate(),
    Err(_) => panic!("Failed to terminate logger because it is still in use"),
};
```

