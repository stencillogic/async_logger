//! Buffer management

use std::sync::atomic::{AtomicUsize, AtomicU64, Ordering, compiler_fence};
use std::sync::{Mutex, Condvar, Arc};
use std::ops::{Deref, DerefMut};
use std::cell::RefCell;


// for optimization: storing last writable buf id optimistically expecting it will be writable the next
// time we try to request a writable slice
thread_local! {
    static CUR_BUF: RefCell<usize> = RefCell::new(0);
}


/// Shared buffer which allows reserving mutable areas of memory concurrently.
/// # Safety
/// This structure is internal, and can't be considered safe by itself.
pub struct Buf {
    ptr:            *mut u8,
    size:           usize,
    used_size:      AtomicUsize,
    acquire_size:   AtomicUsize,
    use_counter:    AtomicU64,
}

impl Buf {

    /// Create a new instance with allocation of `size` bytes. The memory is never deallocated
    /// after the allocation.
    /// `size` must be > 0 and <= std::isize::MAX.
    /// # Errors
    /// Returns `Err` if allocation has failed.
    fn new(size: usize) -> Result<Buf, ()> {

        if size > (std::isize::MAX as usize) || size < 1 {

            return Err(());
        }

        let ptr: *mut u8;

        unsafe {

            let align = std::mem::align_of::<u8>();
            ptr = std::alloc::alloc(std::alloc::Layout::from_size_align(size, align).unwrap());
        }

        if ptr.is_null() {

            Err(())

        } else {

            Ok(Buf {
                ptr,
                size,
                used_size: AtomicUsize::new(0),
                acquire_size: AtomicUsize::new(0),
                use_counter: AtomicU64::new(0),
            })
        }
    }


    /// Sets used space count to zero.
    fn reset(&self) {

        self.used_size.store(0, Ordering::Relaxed);

        compiler_fence(Ordering::Release);

        self.acquire_size.store(0, Ordering::Relaxed);
    }


    /// Returns buffer area of size `reserve_size` for writing.
    /// Returns tuple of `Option` and `bool`. 
    /// `None` if there is not enough space for requested `reserve_size`.
    /// `true` is returned if `release_after_reserve` is required.
    fn reserve_bytes(&self, reserve_size: usize) -> (Option<&mut [u8]>, bool) {

        let mut prev_acq_size = self.acquire_size.load(Ordering::Relaxed);

        // avoid excessive writer notifications
        if prev_acq_size > self.size {

            return (None, false);
        }

        compiler_fence(Ordering::SeqCst);

        // argument sanity check is optimistically after the buffer space avilability check 
        if reserve_size > self.size + 1 || reserve_size > std::usize::MAX - self.size {

            return (None, false);
        }

        if reserve_size == 0 {

            return (Some(&mut []), false);
        }

        compiler_fence(Ordering::SeqCst);

        self.use_counter.fetch_add(1, Ordering::Relaxed);

        compiler_fence(Ordering::SeqCst);

        loop {

            let cur_acq_size = self.acquire_size.compare_and_swap(
                prev_acq_size,
                prev_acq_size + reserve_size,
                Ordering::Relaxed,
            );

            if cur_acq_size == prev_acq_size {

                if cur_acq_size + reserve_size > self.size {

                    return (None, true);

                } else {

                    self.used_size.fetch_add(reserve_size, Ordering::Relaxed);

                    let slice;

                    unsafe {

                        slice = std::slice::from_raw_parts_mut(
                            self.ptr.offset(cur_acq_size as isize),
                            reserve_size);
                    }

                    return (Some(slice), true);
                }

            } else {

                prev_acq_size = cur_acq_size;
            }
            
            if prev_acq_size > self.size {

                return (None, true);
            }

        }
    }


    /// Release bytes previously reserved with `reserve_bytes`.
    /// Return `true` if writer notification is required, `false` otherwise.
    fn release_after_reserve(&self) -> bool {

        self.use_counter.fetch_sub(1, Ordering::Relaxed) == 1

            && self.acquire_size.load(Ordering::Relaxed) > self.size
    }


    /// Returns buffer for reading.
    fn acquire_for_read(&self) -> Option<&[u8]> {

        // it can happen the buffer is not full 
        if self.acquire_size.load(Ordering::Relaxed) <= self.size {

            return None;
        }

        let total_written = self.used_size.load(Ordering::Relaxed);

        let ret;

        unsafe {

            ret = std::slice::from_raw_parts(self.ptr, total_written);
        };

        Some(ret)
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
    pub fn new(sz: usize) -> Result<DoubleBuf, ()> {

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


    /// Return a slice of `u8` of `reserve_size` length for writing.
    /// If there is no free space the method blocks until there is a free buffer space available.
    /// Err is returned when buffer is terminated.
    pub fn reserve_for_write(&self, reserve_size: usize) -> Result<Bytes, ()> {

        let mut cur_buf = 0;

        CUR_BUF.with( |v| {
            cur_buf = *v.borrow(); 
        });

        fn try_get(slf: &DoubleBuf, buf_id: usize, reserve_size: usize) -> Option<Bytes> {

            match slf.bufs[buf_id].reserve_bytes(reserve_size) {

                (Some(slice), release) => {

                    CUR_BUF.with( |v| {
                        *v.borrow_mut() = buf_id; 
                    });

                    return Some(Bytes {
                        slice,
                        parent: slf,
                        buf_id: buf_id,
                        release,
                    })
                },

                (None, release) => {

                    if release {

                        if slf.bufs[buf_id].release_after_reserve() {

                            slf.set_buf_readable(buf_id);
                        }
                    }

                    None
                }
            }
        }

        let mut appendable = false;

        loop {

            if let Some(bytes) = try_get(self, cur_buf, reserve_size) {

                return Ok(bytes);

            } else if let Some(bytes) = try_get(self, 1 - cur_buf, reserve_size) {

                return Ok(bytes);

            } else {

                if appendable {
                    std::thread::yield_now();
                }

                let (buf_id, state) = self.wait_for(BufState::Appendable as u32 | BufState::Terminated as u32);

                if state == BufState::Terminated {

                    return Err(());
                }

                cur_buf = buf_id;

                appendable = true;
            }
        }
    }


    /// Return buffer slice for buffer `buf_id` when buffer is ready to be processed by the writer.
    /// If the buffer is not full the method blocks until buffer is ready for processing.
    pub fn reserve_for_reaed(&self, buf_id: usize) -> &[u8] {

        loop {

            self.wait_for_buf(BufState::Readable as u32, buf_id);

            // must be just single writer thread to avoid race !
            
            if let Some(slice) = self.bufs[buf_id].acquire_for_read() {

                return slice;
            }
        }
    }


    /// Wait until one of the buffers has certain `state`.
    /// Return buffer id with that state.
    fn wait_for(&self, state: u32) -> (usize, BufState) {

        let (ref lock, ref cvar_a, ref cvar_r) = *self.buf_state;

        let mut cur_state = lock.lock().unwrap();

        let cvar = DoubleBuf::determine_cvar(state, cvar_a, cvar_r);

        loop {

            for i in 0..cur_state.len() {

                if 0 != cur_state[i] as u32 & state {

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

            if 0 != cur_state[buf_id] as u32 & state {

                return cur_state[buf_id];
            }

            cur_state = cvar.wait_timeout(cur_state, std::time::Duration::new(1, 0)).unwrap().0;
        }
    }


    #[inline]
    fn set_buf_readable(&self, buf_id: usize) {

        self.set_buf(buf_id, BufState::Readable)
    }


    #[inline]
    pub fn set_buf_terminated(&self, buf_id: usize) {

        self.set_buf(buf_id, BufState::Terminated)
    }


    #[inline]
    pub fn set_buf_appendable(&self, buf_id: usize) {

        self.set_buf(buf_id, BufState::Appendable);

        self.bufs[buf_id].reset();
    }


    #[inline]
    fn determine_cvar<'a>(state: u32, cvar_a: &'a Condvar, cvar_r: &'a Condvar) -> &'a Condvar {

        if 0 != state & (BufState::Readable as u32) { cvar_r } else { cvar_a }
    }


    /// Set state of buffer `buf_id` to value of `state`.
    fn set_buf(&self, buf_id: usize, state: BufState) {

        let (ref lock, ref cvar_a, ref cvar_r) = *self.buf_state;

        let mut cur_state = lock.lock().unwrap();

        let cvar = DoubleBuf::determine_cvar(state as u32, cvar_a, cvar_r);

        if cur_state[buf_id] != state {

            cur_state[buf_id] = state;
            
            cvar.notify_all();
        }
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

        match self.bufs[buf_id].reserve_bytes(self.size + 1) {

            (Some(_), _) => {

                // we can't get slice larger than buffer size
                panic!("Unexpected state detected!");
            },

            (None, release) => {

                if release {

                    if self.bufs[buf_id].release_after_reserve() {

                        // notify that buffer is ready to be processed by writer
                        self.set_buf_readable(buf_id);
                    }
                }
            },
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



/// Wrapper for writable slice of [u8]
pub struct Bytes<'a> {
    slice:  &'a mut [u8],
    parent: &'a DoubleBuf,
    buf_id: usize,
    release: bool,
}


impl<'a> Deref for Bytes<'a> {

    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.slice
    }
}

impl<'a> DerefMut for Bytes<'a> {

    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.slice
    }
}

impl<'a> Drop for Bytes<'a> {

    fn drop(&mut self) {

        if self.release {

            if self.parent.bufs[self.buf_id].release_after_reserve() {

                // notify that buffer is ready for the writer
                self.parent.set_buf_readable(self.buf_id);
            }
        }
    }
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
