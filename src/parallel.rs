use std::cmp::Reverse;
use std::collections::BTreeMap;
use std::sync::{mpsc::sync_channel, Arc, Condvar, Mutex};

// allows producing data in parallel while still consuming it in the main thread
// in order
pub fn parallel_process<Iter, Item, Context, Consumer, ContextCreator, Data, Error, Producer>(
    iter: Iter,
    create_thread_context: ContextCreator,
    produce: Producer,
    mut consume: Consumer,
) -> Result<(), Error>
where
    Iter: ExactSizeIterator<Item = Item> + Send + 'static,
    Context: Send + 'static,
    ContextCreator: Fn() -> Result<Context, Error>,
    Data: Send + 'static,
    Producer: Fn(&mut Context, Item) -> Data + Clone + Send + 'static,
    Consumer: FnMut(Data) -> Result<(), Error>,
{
    let num_threads = rayon::current_num_threads();
    let iter = Arc::new(Mutex::new(iter.enumerate()));
    let next = Arc::new((Mutex::new(2 * num_threads), Condvar::new()));
    let (sender, receiver) = sync_channel(2 * rayon::current_num_threads());
    for _ in 0..num_threads {
        let mut context = create_thread_context()?;
        let iter = iter.clone();
        let next = next.clone();
        let sender = sender.clone();
        let produce = produce.clone();
        rayon::spawn(move || loop {
            let idx = iter.lock().unwrap().next();
            let idx = match idx {
                Some(x) => x,
                None => break,
            };

            let result = produce(&mut context, idx.1);

            let mut guard = next.0.lock().unwrap();
            while *guard <= idx.0 {
                guard = next.1.wait(guard).unwrap();
            }
            sender.send((idx.0, result)).unwrap();
        });
    }
    drop(sender); // drop to make sure iteration will finish once all senders are out of scope
    let mut pending = BTreeMap::new();
    let mut next_idx = 0;
    for result in receiver {
        pending.insert(Reverse(result.0), result.1);
        while let Some(data) = pending.remove(&Reverse(next_idx)) {
            let mut guard = next.0.lock().unwrap();
            *guard += 1;
            next.1.notify_all();

            next_idx += 1;
            consume(data)?;
        }
    }
    Ok(())
}
