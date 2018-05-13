#![feature(fnbox)]

#[macro_use]
extern crate quick_error;
extern crate num_cpus;
extern crate boxfnonce;

use std::sync::{Arc,Mutex, Condvar};
use std::sync::atomic::{AtomicUsize, AtomicBool, Ordering};
use std::collections::VecDeque;
//TODO - fix later when boxing of FnOnce is resolved
//use std::boxed::FnBox;   
use std::thread::{Builder as ThreadBuilder};
use std::time::{Duration};
use boxfnonce::{SendBoxFnOnce};


quick_error! {
    #[derive(Debug, PartialEq, Eq)]
    pub enum Error {
        PoolIsTerminating {}
        PoolIsFull {}
    }
}


struct Queue<T> {
    items: Arc<Mutex<VecDeque<T>>>,
    cond: Arc<Condvar>,
    timeout: Option<Duration>
}

impl <T> Clone for Queue<T> {
    fn clone(&self) -> Self {
        Queue {
            items: self.items.clone(),
            cond: self.cond.clone(),
            timeout: self.timeout
        }
    }
}

impl <T> Queue<T> {
    #[allow(dead_code)]
    fn new() -> Self {
        Queue {
            items: Arc::new(Mutex::new(VecDeque::new())),
            cond: Arc::new(Condvar::new()),
            timeout: None
        }
    }

    fn with_timeout(timeout: Duration) -> Self {
        Queue {
            items: Arc::new(Mutex::new(VecDeque::new())),
            cond: Arc::new(Condvar::new()),
            timeout: Some(timeout)
        }
    }

    fn put(&mut self, m:T) {
        let mut unlocked_items = self.items.lock().unwrap();
        unlocked_items.push_front(m);
        self.cond.notify_one()

    }

    fn get(&mut self) -> Option<T> {
        let mut unlocked_items = self.items.lock().unwrap();
        #[allow(while_true)]
        // todo - later check for wait spurious breaks, now it's not so bit issue 
        // let wait_until = match self.timeout_millis {
        //     None => Instant::now(),
        //     Some(to) => Instant::now() + Duration::from_millis(to)
        // };
        while true {
            if let Some(item) = unlocked_items.pop_back()  {
                return Some(item)
            }
            unlocked_items = match self.timeout {
                None => self.cond.wait(unlocked_items).unwrap(),
                Some(to) => {
                    let res = self.cond.wait_timeout(unlocked_items, 
                    to).unwrap();
                    if res.1.timed_out() {
                        return None
                    }
                    res.0
                }
            }
        }
        unreachable!()
    }

    fn len(&self) -> usize {
        self.items.lock().unwrap().len()
    }
}


enum Message {
    End,
    Run(SendBoxFnOnce<'static,()>)
}

#[derive(Clone)]
struct Worker 
{
    queue: Queue<Message>,
    pool: Arc<PoolState>

}

#[derive(Clone)]
pub struct Pool {
    queue: Queue<Message>,
    state: Arc<PoolState>,
    params: PoolParams
}

struct PoolState {
    total_workers: AtomicUsize,
    workers: Mutex<usize>,
    active: AtomicUsize,
    terminating: AtomicBool,
    terminated: Mutex<bool>,
    cond_terminated: Condvar,
    min_threads: usize,
}

#[derive(Clone)]
struct PoolParams {
    max_threads: usize,
    max_queue: usize,
    thread_idle_time: Duration
}

#[derive(Debug)]
pub struct Builder {
    min_threads: usize,
    max_threads: usize,
    max_queue: usize,
    thread_idle_time: Duration
}
const DEFAULT_MAX_QUEUE: usize = 100;
const DEFAULT_THREAD_IDLE_TIME: Duration = Duration::from_secs(60);

impl Builder {
    pub fn new() -> Self {
        let machine_cpus = num_cpus::get();
        Builder {
            min_threads: machine_cpus,
            max_threads: machine_cpus*2,
            max_queue: DEFAULT_MAX_QUEUE,
            thread_idle_time: DEFAULT_THREAD_IDLE_TIME 
        }
    }

    pub fn set_min_threads(&mut self, min_threads:usize) -> &mut Self {
        self.min_threads = min_threads;
        self
    }

    pub fn set_max_threads(&mut self, max_threads:usize) -> &mut Self {
        self.max_threads = max_threads;
        self
    }

    pub fn set_max_queue(&mut self, max_queue:usize) -> &mut Self {
        self.max_queue = max_queue;
        self
    }

    pub fn set_thread_idle_time(&mut self, thread_idle_time: Duration) -> &mut Self {
        self.thread_idle_time = thread_idle_time;
        self
    }

    pub fn build(&self) -> Pool {
        let pool = Pool {
            queue: Queue::with_timeout(self.thread_idle_time),
            state: Arc::new(PoolState{
                total_workers: AtomicUsize::new(0),
                active: AtomicUsize::new(0),
                workers: Mutex::new(self.min_threads),
                terminating: AtomicBool::new(false),
                terminated: Mutex::new(false),
                cond_terminated: Condvar::new(),
                min_threads: self.min_threads,
            }),
            params: PoolParams {
                max_threads: self.max_threads,
                max_queue: self.max_queue,
                thread_idle_time: self.thread_idle_time
            }
        };
        
        
        for _i in 0..pool.state.min_threads {
           pool.add_worker()
        }
        pool
    }

}




impl Worker {
   fn run(&mut self) {
       #[allow(while_true)]
       while true {
           match self.queue.get() {
               Some(msg) => {
                   match msg {
               Message::End => break,
               Message::Run(f) => {
                 f.call();
                 self.pool.active.fetch_sub(1, Ordering::SeqCst);
               }
               }
               },
               None => {
                   let mut  workers = self.pool.workers.lock().unwrap();
                   if *workers > self.pool.min_threads {
                       *workers-=1;
                       return
                   }
               }
           }
          
       }
       {
        let mut terminated = self.pool.terminated.lock().unwrap();
        let mut workers = self.pool.workers.lock().unwrap();
        *workers-=1;
        if self.pool.terminating.load(Ordering::SeqCst) &&
            *workers == 0 {
            //No threads are running, terminated
            *terminated = true;
            self.pool.cond_terminated.notify_all();
        }
       }
   }
}

impl Pool {

    pub fn new() -> Self {
       Builder::new().build()
    }

    pub fn queue_size(&self) -> usize {
        self.queue.len()
    }

    fn add_worker(&self) {
        
        let mut w = Worker {
        queue: self.queue.clone(),
        pool: self.state.clone()
        };
        let builder = ThreadBuilder::new().name(format!("Worker {}", 
        self.state.total_workers.fetch_add(1,Ordering::Relaxed)));
        builder.spawn(move || {
            w.run()
        }).unwrap();
        
    }

    pub fn spawn<F>(&mut self, f: F) -> Result<(),Error>
    where F: FnOnce() -> () + Send + 'static
    {
        if self.state.terminating.load(Ordering::SeqCst) {
            return Err(Error::PoolIsTerminating)
        }
        if self.queue.len() >= self.params.max_queue {
            return Err(Error::PoolIsFull)
        }
        {
            let mut workers_now = self.state.workers.lock().unwrap();
            if *workers_now < self.state.min_threads || self.queue.len()> *workers_now 
            && *workers_now < self.params.max_threads {
            self.add_worker();
            *workers_now+=1
        }
        }
        self.state.active.fetch_add(1, Ordering::SeqCst);
        self.queue.put(Message::Run(SendBoxFnOnce::new(f)));
        Ok(())
    }
    // TODO
    // pub fn wait_till_idle(&self) {

    // }

    pub fn terminate(&mut self, ) {
        let num_workers = self.state.workers.lock().unwrap();
        self.state.terminating.store(true, Ordering::SeqCst);
        for _i in 0..*num_workers {
            self.queue.put(Message::End)
        }
    }

    pub fn join(self) {
        let mut terminated = self.state.terminated.lock().unwrap();
        while !*terminated  {
            terminated = self.state.cond_terminated.wait(terminated).unwrap()
        } 
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use std::thread::{sleep, self};
    use std::time::Duration;

    #[test]
    fn test_queue() {

        let q = Queue::<usize>::new();

        for i in 1..11 {
            let mut lc = q.clone();
            thread::spawn( move || {
                sleep(Duration::from_millis(i*10));
                lc.put(i as usize);
                });
        }

        let res: usize = (1..11)
        .map(|_| q.clone())
        .map(|mut qq| thread::spawn(move || {qq.get().unwrap()}))
        .map(|j| j.join().unwrap())
        .sum();

        assert_eq!(55, res);
    }

    

    #[test]
    fn it_works() {
    
    let x = Arc::new(AtomicUsize::new(0));

    let mut p = Pool::new();
    
    for _i in 1..5 {
        let c = x.clone();
        p.spawn(move || {c.fetch_add(1, Ordering::SeqCst);}).unwrap()
    }   
    
    p.terminate();
    //sleep(Duration::from_secs(1));
    p.join();
    
    //assert_eq!(0, p.state.workers.load(Ordering::SeqCst));
    assert_eq!(4, x.load(Ordering::SeqCst));
    }

    #[test]
    fn test_full_queue() {
        let mut pool = Builder::new()
            .set_max_queue(2)
            .set_min_threads(2)
            .set_max_threads(2)
            .build();

        fn work() {
            sleep(Duration::from_millis(1000))
        }

        pool.spawn(work).unwrap();
        pool.spawn(work).unwrap();
        sleep(Duration::from_millis(100));
        pool.spawn(work).unwrap();
        pool.spawn(work).unwrap();

        let res = pool.spawn(work);
        assert_eq!(Err(Error::PoolIsFull), res);

    }

    #[test]
    fn test_add_workers() {
        let mut pool = Builder::new()
            .set_max_queue(20)
            .set_min_threads(2)
            .set_max_threads(4)
            .build();

        fn work() {
            sleep(Duration::from_millis(100))
        }

        for _i in 0..10 {
            pool.spawn(work).unwrap()
        }
        sleep(Duration::from_millis(10));
        {
            let num_workers = pool.state.workers.lock().unwrap();
            assert_eq!(4, *num_workers )
        }
    }

    #[test]
    fn test_removes_workers() {
        let mut pool = Builder::new()
            .set_max_queue(20)
            .set_min_threads(2)
            .set_max_threads(4)
            .set_thread_idle_time(Duration::from_millis(500))
            .build();

        fn work() {
            sleep(Duration::from_millis(100))
        }

        for _i in 0..10 {
            pool.spawn(work).unwrap()
        }
        sleep(Duration::from_millis(1000));
        {
            let num_workers = pool.state.workers.lock().unwrap();
            assert_eq!(2, *num_workers )
        }
        // now still can normally proceed
        let x = Arc::new(AtomicUsize::new(0));
        for _i in 1..5 {
        let c = x.clone();
        pool.spawn(move || {c.fetch_add(1, Ordering::SeqCst);}).unwrap()
    }   
    
    pool.terminate();
    pool.join();
    assert_eq!(4, x.load(Ordering::SeqCst));

    }


}

