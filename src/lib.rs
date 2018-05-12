#![feature(fnbox)]

use std::sync::{Arc,Mutex, Condvar};
use std::sync::atomic::{AtomicUsize, AtomicBool, Ordering};
use std::collections::VecDeque;
use std::boxed::FnBox;
use std::thread::{Builder};


struct Queue<T> {
    items: Arc<Mutex<VecDeque<T>>>,
    cond: Arc<Condvar>
}

impl <T> Clone for Queue<T> {
    fn clone(&self) -> Self {
        Queue {
            items: self.items.clone(),
            cond: self.cond.clone()
        }
    }
}

impl <T> Queue<T> {
    fn new() -> Self {
        Queue {
            items: Arc::new(Mutex::new(VecDeque::new())),
            cond: Arc::new(Condvar::new())
        }
    }

    fn put(&mut self, m:T) {
        let mut unlocked_items = self.items.lock().unwrap();
        unlocked_items.push_front(m);
        self.cond.notify_one()

    }

    fn get(&mut self) -> T {
        let mut unlocked_items = self.items.lock().unwrap();
        #[allow(while_true)]
        while true {
            if let Some(item) = unlocked_items.pop_back()  {
                return item
            }
            unlocked_items = self.cond.wait(unlocked_items).unwrap();
        }
        unreachable!()
    }
}


enum Message {
    End,
    Run(Box<FnBox() -> () + Send + 'static>)
}

#[derive(Clone)]
struct Worker 
{
    queue: Queue<Message>,
    counter: Arc<AtomicUsize>

}

#[derive(Clone)]
pub struct Pool {
    queue: Queue<Message>,
    workers: Arc<AtomicUsize>,
    active: Arc<AtomicUsize>,
    //terminating: Arc

}


impl Worker {
   fn run(&mut self) {
       #[allow(while_true)]
       while true {
           match self.queue.get() {
               Message::End => break,
               Message::Run(f) => {
                 f();
                 let remains=self.counter.fetch_sub(1, Ordering::SeqCst);
               }
           }
          
       }
   }
}

impl Pool {

    pub fn new() -> Self {
        let q = Queue::new();
        let counter = Arc::new(AtomicUsize::new(0));
        
        let workers = AtomicUsize::new(0);
        for i in 0..4 {
           let mut w = Worker {
            queue: q.clone(),
            counter: counter.clone()
            };
            let builder = Builder::new().name(format!("Worker {}", i));
            builder.spawn(move || {
                w.run()
            }).unwrap();
           
            workers.fetch_add(1, Ordering::SeqCst);
        }
        Pool{
            queue: q,
            workers: Arc::new(workers),
            active: counter
        }
    }

    pub fn spawn<F>(&mut self, f: F) 
    where F: FnOnce() -> () + Send + 'static
    {
        self.active.fetch_add(1, Ordering::SeqCst);
        self.queue.put(Message::Run(Box::new(f)))
    }

    pub fn wait_till_idle(&self) {

    }

    pub fn terminate(&mut self, ) {
        
        for _i in 0..self.workers.load(Ordering::SeqCst) {
            self.queue.put(Message::End)
        }
    }

    pub fn join(&mut self) {
        
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
        .map(|mut qq| thread::spawn(move || {qq.get()}))
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
        p.spawn(move || {c.fetch_add(1, Ordering::SeqCst);})
    }   
    
    p.terminate();
    sleep(Duration::from_secs(1));
    //p.join();
    assert_eq!(4, x.load(Ordering::SeqCst));
    }
}
