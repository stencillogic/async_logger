//! Buffer management

use std::sync::atomic::{AtomicUsize, Ordering, compiler_fence};
use std::sync::{Mutex, Condvar, Arc};
use std::cell::RefCell;
use super::{Error, ErrorRepr, ErrorKind};


// for optimization: storing last writable buf id optimistically expecting it will be writable the next
// time we try to request a writable slice
thread_local! {
    static CUR_BUF: RefCell<usize> = RefCell::new(0);
}


#[repr(align(64))]
struct CacheAligned<T> (T);


/// Shared buffer which allows reserving mutable areas of memory concurrently.
/// # Safety
/// This structure is internal, and can't be considered safe by itself.
pub struct Buf {
    acquire_size:   CacheAligned<AtomicUsize>,
    done_size:      CacheAligned<AtomicUsize>,
    used_size:      AtomicUsize,
    ptr:            *mut u8,
    size:           usize,
}

impl Buf {

    /// Create a new instance with allocation of `size` bytes. The memory is never deallocated
    /// after the allocation.
    /// `size` must be > 0 and <= std::isize::MAX.
    /// # Errors
    /// Returns `Err` if allocation has failed.
    fn new(size: usize) -> Result<Buf, Error> {

        if size > (std::isize::MAX as usize) || size < 1 {

            return Err(Error::new(ErrorKind::IncorrectBufferSize, ErrorRepr::Simple));
        }

        let ptr: *mut u8;

        unsafe {

            let align = std::mem::align_of::<u8>();
            ptr = std::alloc::alloc(
                std::alloc::Layout::from_size_align(size, align)
                .map_err(|e| { Error::new(ErrorKind::MemoryLayoutError, ErrorRepr::MemoryLayoutError(e)) })?
            );
        }

        if ptr.is_null() {

            Err(Error::new(ErrorKind::AllocFailure, ErrorRepr::Simple))

        } else {

            Ok(Buf {
                acquire_size: CacheAligned(AtomicUsize::new(0)),
                done_size: CacheAligned(AtomicUsize::new(0)),
                used_size: AtomicUsize::new(0),
                ptr,
                size,
            })
        }
    }


    /// Sets used space count to zero.
    fn reset(&self) {

        self.used_size.store(0, Ordering::Relaxed);
        self.done_size.0.store(0, Ordering::Relaxed);

        compiler_fence(Ordering::SeqCst);

        self.acquire_size.0.store(0, Ordering::Relaxed);
    }


    /// Returns tuple of `bool` and `bool`: "data is written", and "notify writer"
    fn write_slice(&self, slice: &[u8]) -> (bool, bool) {

        let reserve_size = slice.len();

        if reserve_size == 0 {

            return (true, false);
        }

        if reserve_size > self.size || reserve_size > std::usize::MAX - self.size {

            return (false, false);
        }

        let mut prev_acq_size = self.acquire_size.0.load(Ordering::Relaxed);

        loop {

            if prev_acq_size > self.size {

                return (false, false);
            }

            let cur_acq_size = self.acquire_size.0.compare_and_swap(
                prev_acq_size,
                prev_acq_size + reserve_size,
                Ordering::Relaxed,
            );

            if cur_acq_size == prev_acq_size {

                if cur_acq_size + reserve_size > self.size {

                    if self.size > cur_acq_size {
                        let done_size = self.size - cur_acq_size;
                        let total_done = self.done_size.0.fetch_add(done_size, Ordering::Relaxed) + done_size;
                        return (false, total_done == self.size);
                    }

                    return (false, false);

                } else {

                    self.used_size.fetch_add(reserve_size, Ordering::Relaxed);

                    unsafe {

                        std::ptr::copy(slice.as_ptr(), 
                                       self.ptr.offset(cur_acq_size as isize), 
                                       reserve_size);
                    }

                    let total_done = self.done_size.0.fetch_add(reserve_size, Ordering::Relaxed) + reserve_size;
                    return (true, total_done == self.size);
                }

            } else {

                prev_acq_size = cur_acq_size;
            }
        }
    }

    
    /// try to take up all remaining space, return true if to notify writer
    fn reserve_rest(&self) -> bool {
        
        let reserve_size = self.size + 1;

        let mut prev_acq_size = self.acquire_size.0.load(Ordering::Relaxed);

        loop {

            if prev_acq_size > self.size {

                return false;
            }

            let cur_acq_size = self.acquire_size.0.compare_and_swap(
                prev_acq_size,
                prev_acq_size + reserve_size,
                Ordering::Relaxed,
            );

            if cur_acq_size == prev_acq_size {

                if self.size > cur_acq_size {

                    let done_size = self.size - cur_acq_size;
                    let total_done = self.done_size.0.fetch_add(done_size, Ordering::Relaxed) + done_size;

                    return total_done == self.size;
                }

                return false;

            } else {

                prev_acq_size = cur_acq_size;
            }
        }
    }

    /// Returns buffer for reading.
    fn acquire_for_read(&self) -> &[u8] {

        let total_written = self.used_size.load(Ordering::Relaxed);

        let ret;

        unsafe {

            ret = std::slice::from_raw_parts(self.ptr, total_written);
        };

        ret
    }
}


impl Drop for Buf {

    fn drop(&mut self) {

        let align = std::mem::align_of::<u8>();

        unsafe {

            std::alloc::dealloc(self.ptr, std::alloc::Layout::from_size_align(self.size, align).unwrap());
        }
    }
}


unsafe impl Sync for Buf {}
unsafe impl Send for Buf {}



/// Doubled Buf instances (flip-flop buffer)
#[derive(Clone)]
pub struct DoubleBuf {
    bufs: Arc<Vec<Buf>>,
    buf_state: Arc<(Mutex<[BufState; 2]>, Condvar, Condvar)>,
    size: usize,
}


impl DoubleBuf {


    /// Create an instance of buffer pair, each of size `sz`.
    pub fn new(sz: usize) -> Result<DoubleBuf, Error> {

        let bufs = Arc::new(vec![Buf::new(sz)?, Buf::new(sz)?]);

        let buf_state = Arc::new((Mutex::new([BufState::Appendable, BufState::Appendable]), Condvar::new(), Condvar::new()));

        Ok(DoubleBuf {
            bufs,
            buf_state,
            size: sz,
        })
    }


    /// return number of buffers
    #[inline]
    pub fn get_buf_cnt(&self) -> usize {

        self.bufs.len()
    }

    fn try_write(&self, buf_id: usize, slice: &[u8]) -> bool {

        let (is_written, notify_writer) = self.bufs[buf_id].write_slice(slice);

        if notify_writer {

            self.set_buf_readable(buf_id);
        }

        if is_written {

            CUR_BUF.with( |v| {
                *v.borrow_mut() = buf_id; 
            });
        }

        return is_written;
    }

    /// Write data.
    /// `Err` is retured if buffer is terminated
    pub fn write_slice(&self, slice: &[u8]) -> Result<(), ()> {

        let mut cur_buf = 0;

        CUR_BUF.with( |v| {
            cur_buf = *v.borrow(); 
        });

        let mut appendable = 0;

        loop {

            if self.try_write(cur_buf, slice) {

                return Ok(());

            } else if self.try_write(1 - cur_buf, slice) {

                return Ok(());

            } else {

                if appendable > 0 {

                    std::thread::yield_now();

                    if appendable > 10000 {
                        std::thread::sleep(std::time::Duration::new(0,10_000_000));
                        appendable = 0;
                    }
                }

                let (buf_id, state) = self.wait_for(BufState::Appendable as u32 | BufState::Terminated as u32);

                if state == BufState::Terminated {

                    return Err(());
                }

                cur_buf = buf_id;

                appendable += 1;
            }
        }
    }


    /// Return buffer slice for buffer `buf_id` when buffer is ready to be processed by the writer.
    /// If the buffer is not full the method blocks until buffer is ready for processing.
    pub fn reserve_for_reaed(&self, buf_id: usize) -> &[u8] {

        self.wait_for_buf(BufState::Readable as u32, buf_id);

        return self.bufs[buf_id].acquire_for_read();
    }


    /// Wait until one of the buffers has certain `state`.
    /// Return buffer id with that state.
    fn wait_for(&self, state: u32) -> (usize, BufState) {

        let (ref lock, ref cvar_a, ref cvar_r) = *self.buf_state;

        let mut cur_state = lock.lock().unwrap();

        let cvar = DoubleBuf::determine_cvar(state, cvar_a, cvar_r);

        loop {

            for i in 0..cur_state.len() {

                if 0 != (cur_state[i] as u32 & state) {

                    return (i, cur_state[i]);
                }
            }

            cur_state = cvar.wait_timeout(cur_state, std::time::Duration::new(1, 0)).unwrap().0;
        }
    }


    /// Wait until i'th buffer has certain `state`.
    fn wait_for_buf(&self, state: u32, buf_id: usize) -> BufState {

        let (ref lock, ref cvar_a, ref cvar_r) = *self.buf_state;

        let mut cur_state = lock.lock().unwrap();

        let cvar = DoubleBuf::determine_cvar(state, cvar_a, cvar_r);

        loop {

            if 0 != (cur_state[buf_id] as u32 & state) {

                return cur_state[buf_id];
            }

            cur_state = cvar.wait_timeout(cur_state, std::time::Duration::new(1, 0)).unwrap().0;
        }
    }

    fn set_buf_readable(&self, buf_id: usize) {

        let (ref lock, ref _cvar_a, ref cvar_r) = *self.buf_state;

        let mut cur_state = lock.lock().unwrap();

        cur_state[buf_id] = BufState::Readable;

        cvar_r.notify_all();
    }


    pub fn set_buf_terminated(&self, buf_id: usize) {

        let (ref lock, ref cvar_a, ref _cvar_r) = *self.buf_state;

        let mut cur_state = lock.lock().unwrap();

        cur_state[buf_id] = BufState::Terminated;

        cvar_a.notify_all();
    }


    pub fn set_buf_appendable(&self, buf_id: usize) {

        let (ref lock, ref cvar_a, ref _cvar_r) = *self.buf_state;

        let mut cur_state = lock.lock().unwrap();

        compiler_fence(Ordering::SeqCst);
        self.bufs[buf_id].reset();
        compiler_fence(Ordering::SeqCst);

        cur_state[buf_id] = BufState::Appendable;

        cvar_a.notify_all();
    }


    #[inline]
    fn determine_cvar<'a>(state: u32, cvar_a: &'a Condvar, cvar_r: &'a Condvar) -> &'a Condvar {

        if 0 != state & (BufState::Readable as u32) { cvar_r } else { cvar_a }
    }


    /// Flush the last used in current thread buffer
    pub fn flush(&self) {

        let mut buf_id = 0;

        CUR_BUF.with( |v| {
            buf_id = *v.borrow(); 
        });

        self.flush_buf(buf_id);

        CUR_BUF.with( |v| {
            *v.borrow_mut() = 1 - buf_id;
        });
    }

    fn flush_buf(&self, buf_id: usize) {

        if self.bufs[buf_id].reserve_rest() {

            self.set_buf_readable(buf_id);
        }
    }

    /// Prevent buffers from writing
    pub fn seal_buffers(&self) {

        let mut sealed = [false; 2];

        loop {

            for buf_id in 0..2 {

                if ! sealed[buf_id] {

                    self.flush_buf(buf_id);
                }
            }

            for buf_id in 0..2 {

                if ! sealed[buf_id] {

                    let state = self.wait_for_buf(BufState::Terminated as u32 | BufState::Appendable as u32, buf_id);

                    sealed[buf_id] = state == BufState::Terminated;
                }
            }

            if sealed[0] && sealed[1] {

                break;

            } else {

                std::thread::sleep(std::time::Duration::new(0,10_000_000));
            }
        }
    }
}



/// Buffer states for buffer tracking
#[derive(Copy, Clone, PartialEq, Debug)]
enum BufState {
    Appendable = 0b001,
    Readable = 0b010,
    Terminated = 0b100,
}



#[cfg(test)]
mod tests {
    use super::*;
    use std;

    #[test]
    fn too_big_size() {
        if let Ok(_) = Buf::new(std::isize::MAX as usize) {
            panic!("Buf::new takes size value more than expected");
        }
    }

    #[test]
    fn zero_size() {
        if let Ok(_) = Buf::new(0) {
            panic!("Buf::new takes zero size");
        }
    }

}
