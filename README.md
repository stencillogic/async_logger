# async_logger

![Rust](https://github.com/stencillogic/async_logger/workflows/Rust/badge.svg)

Asynchronous logger allows fast writing of arbitrary data to a fixed size memory buffer. 
The data from the memory buffer then can be processed in a separate thread.
This crate was created initially for logging purposes but also can be used as a queue anywhere else.
See [crate documentation](https://docs.rs/async_logger/) for more info.

Implementation of [log](https://docs.rs/log) facade based on this crate is available as separate crate
[async_logger_log](https://docs.rs/async_logger_log).

## Intro

`AsyncLoggerNB` is implementation of asynchronous logger/queue that allows writing arbitrary slices to a memory buffer, 
and then send the buffer to a processing thread. 

`AsyncLoggerNB` uses pair of fixed size buffers; 
while one buffer is being written by the multiple threads, the second is being proccessed by the
single "writer" thread. Writing to a buffers is lock-free operation.
Blocking appears only at the moment when buffers change their roles.
This makes `AsyncLoggerNB` realy fast, and at the same time allows it be bounded.
It can be effectively used in mutlithreaded mutlicore environment with high level of concurrent writes 
when you don't want to drop messages or run out of memory but still want to keep lock-free writes.

`AsyncLoggerNB` can process serialized data (stream of bytes) or custom complex data structures, and also references to objects.

`AsyncLoggerNB` can accept any "writer" as soon as it implements `Writer` trait. This package includes
`FileWriter` that writes data to a file.

## Examples

```
use async_logger::FileWriter;
use async_logger::AsyncLoggerNB;
use std::{thread, sync::Arc};

let writer = FileWriter::new("/tmp", 10*1024*1024).expect("Failed to create file writer");

let logger = Arc::new(AsyncLoggerNB::new(Box::new(writer), 8192)
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

## Notes

1. This crate was tested on Linux x86_64. Rust version 1.42.
