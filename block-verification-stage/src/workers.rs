pub(crate) mod entry_hash_verification;
pub(crate) mod signature_verification;
pub(crate) mod transaction_check;
pub(crate) mod transaction_execution;

use {
    crate::join_thread,
    crossbeam_channel::{Receiver, Sender, bounded},
    shaq::{error::WaitError, mpmc, spsc},
    std::{
        fmt::Debug,
        num::NonZeroUsize,
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        thread::{self, JoinHandle},
        time::Duration,
    },
};

/// The timeout used on waiting for requests before checking whether to exit
const EXIT_POLL_INTERVAL: Duration = Duration::from_millis(10);

/// The scheduler's end of the queues to a pool of verification workers, bounding the requests in
/// flight by the queue size.
pub(crate) struct VerificationQueue<Request, Response> {
    request_producer: mpmc::Producer<Request>,
    response_receiver: Receiver<Response>,
    requests_in_flight: usize,
    queue_size: usize,
}

impl<Request: Send + Debug, Response> VerificationQueue<Request, Response> {
    /// Returns the queue along with the workers' ends of it.
    pub(crate) fn new(
        queue_size: NonZeroUsize,
    ) -> (Self, mpmc::Consumer<Request>, Sender<Response>) {
        let (request_producer, request_consumer) = mpmc::pair(queue_size.get())
            .expect("mpmc shaq channel can be allocated with the config");
        let (response_sender, response_receiver) = bounded(queue_size.get());

        let verification_queue = Self {
            request_producer,
            response_receiver,
            requests_in_flight: 0,
            queue_size: queue_size.get(),
        };

        (verification_queue, request_consumer, response_sender)
    }

    pub(crate) fn response_receiver(&self) -> &Receiver<Response> {
        &self.response_receiver
    }

    /// Checks that the pool has capacity to take a new request with [`Self::send`]
    pub(crate) fn can_send_request(&self) -> bool {
        self.requests_in_flight < self.queue_size
    }

    /// [`Self::can_send_request`] must always be checked before calling this function,
    /// otherwise this function can panic if there is no capacity on the queue
    /// to send a new request.
    pub(crate) fn send(&mut self, request: Request) {
        self.request_producer
            .try_write(request)
            .expect("the scheduler only sends requests while the queue has room for one");

        self.requests_in_flight = self
            .requests_in_flight
            .checked_add(1)
            .expect("requests in flight are bounded by the request queue size");
    }

    /// Sends the request if the pool has capacity for it
    pub(crate) fn try_send(&mut self, request: Request) -> Result<(), Request> {
        if !self.can_send_request() {
            return Err(request);
        }

        self.send(request);
        Ok(())
    }

    /// Records that the scheduler handled the response to a request in flight.
    ///
    /// Panics if called more times than [`Self::send`].
    pub(crate) fn complete_request(&mut self) {
        self.requests_in_flight = self
            .requests_in_flight
            .checked_sub(1)
            .expect("each response answers a request that was counted as in flight when sent");
    }

    #[cfg(test)]
    pub(crate) fn requests_in_flight(&self) -> usize {
        self.requests_in_flight
    }
}

/// Like [`VerificationQueue`], but with a request queue for each worker.
pub(crate) struct PerWorkerQueues<Request, Response> {
    request_producers: Vec<spsc::Producer<Request>>,
    response_receiver: Receiver<Response>,
    requests_in_flight: Vec<usize>,
    queue_size: usize,
}

impl<Request: Send + Debug, Response> PerWorkerQueues<Request, Response> {
    /// Returns the queues along with the workers' ends of them, one request consumer per worker.
    pub(crate) fn new(
        num_workers: NonZeroUsize,
        queue_size: NonZeroUsize,
    ) -> (Self, Vec<spsc::Consumer<Request>>, Sender<Response>) {
        let (request_producers, request_consumers) = (0..num_workers.get())
            .map(|_| {
                spsc::pair(queue_size.get())
                    .expect("spsc shaq channel can be allocated with the config")
            })
            .unzip();
        let max_requests_in_flight = num_workers
            .checked_mul(queue_size)
            .expect("the number of workers and the queue size are small");
        let (response_sender, response_receiver) = bounded(max_requests_in_flight.get());

        let per_worker_queues = Self {
            request_producers,
            response_receiver,
            requests_in_flight: vec![0; num_workers.get()],
            queue_size: queue_size.get(),
        };

        (per_worker_queues, request_consumers, response_sender)
    }

    pub(crate) fn num_workers(&self) -> usize {
        self.request_producers.len()
    }

    pub(crate) fn response_receiver(&self) -> &Receiver<Response> {
        &self.response_receiver
    }

    pub(crate) fn requests_in_flight(&self, worker_index: usize) -> usize {
        self.requests_in_flight[worker_index]
    }

    /// Checks that the worker has capacity to take a new request with [`Self::send`]
    pub(crate) fn can_send_request(&self, worker_index: usize) -> bool {
        self.requests_in_flight(worker_index) < self.queue_size
    }

    /// [`Self::can_send_request`] must always be checked before calling this function,
    /// otherwise this function can panic if there is no capacity on the worker's queue
    /// to send a new request.
    pub(crate) fn send(&mut self, worker_index: usize, request: Request) {
        self.request_producers[worker_index]
            .try_write(request)
            .expect("the scheduler only sends requests while the queue has room for one");

        let requests_in_flight = &mut self.requests_in_flight[worker_index];
        *requests_in_flight = requests_in_flight
            .checked_add(1)
            .expect("requests in flight are bounded by the request queue size");
    }

    /// Records that the scheduler handled the response to a request in flight to the worker.
    ///
    /// Panics if called more times than [`Self::send`] for the worker.
    pub(crate) fn complete_request(&mut self, worker_index: usize) {
        let requests_in_flight = &mut self.requests_in_flight[worker_index];
        *requests_in_flight = requests_in_flight
            .checked_sub(1)
            .expect("each response answers a request that was counted as in flight when sent");
    }
}

/// A pool of worker threads answering requests, from a shared queue or one queue per worker.
pub(crate) struct VerificationWorkers {
    worker_handles: Vec<JoinHandle<()>>,
}

impl VerificationWorkers {
    pub(crate) fn spawn<Request: Send + 'static, Response: Send + 'static>(
        thread_name_prefix: &str,
        shutdown_signal: Arc<AtomicBool>,
        num_workers: NonZeroUsize,
        request_consumer: mpmc::Consumer<Request>,
        response_sender: Sender<Response>,
        verify: impl Fn(Request) -> Response + Clone + Send + 'static,
    ) -> Self {
        let worker_handles = (0..num_workers.get())
            .map(|index| {
                let request_consumer = request_consumer.clone();
                let response_sender = response_sender.clone();
                let exit = shutdown_signal.clone();
                let verify = verify.clone();
                thread::Builder::new()
                    .name(format!("{thread_name_prefix}{index}"))
                    .spawn(move || {
                        run_worker(
                            |timeout| request_consumer.read_timeout(timeout),
                            &response_sender,
                            &exit,
                            verify,
                        )
                    })
                    .expect("thread can be spawned")
            })
            .collect();

        Self { worker_handles }
    }

    /// Spawns a worker for each request consumer.
    pub(crate) fn spawn_per_consumer<Request: Send + 'static, Response: Send + 'static>(
        thread_name_prefix: &str,
        shutdown_signal: Arc<AtomicBool>,
        request_consumers: Vec<spsc::Consumer<Request>>,
        response_sender: Sender<Response>,
        verify: impl Fn(Request) -> Response + Clone + Send + 'static,
    ) -> Self {
        let worker_handles = request_consumers
            .into_iter()
            .enumerate()
            .map(|(index, mut request_consumer)| {
                let response_sender = response_sender.clone();
                let exit = shutdown_signal.clone();
                let verify = verify.clone();
                thread::Builder::new()
                    .name(format!("{thread_name_prefix}{index}"))
                    .spawn(move || {
                        run_worker(
                            |timeout| request_consumer.read_timeout(timeout),
                            &response_sender,
                            &exit,
                            verify,
                        )
                    })
                    .expect("thread can be spawned")
            })
            .collect();

        Self { worker_handles }
    }

    /// No worker threads, for tests that act as the workers themselves.
    #[cfg(test)]
    pub(crate) fn none() -> Self {
        Self {
            worker_handles: Vec::new(),
        }
    }

    pub(crate) fn join_workers(self) {
        for worker_handle in self.worker_handles {
            let () = join_thread(worker_handle);
        }
    }
}

fn run_worker<Request, Response>(
    mut read_request: impl FnMut(Duration) -> Result<Request, WaitError>,
    response_sender: &Sender<Response>,
    shutdown_signal: &AtomicBool,
    verify: impl Fn(Request) -> Response,
) {
    while !shutdown_signal.load(Ordering::Relaxed) {
        let verification_request = match read_request(EXIT_POLL_INTERVAL) {
            Ok(request) => request,
            Err(WaitError::Timeout) => continue,
        };

        let verification_response = verify(verification_request);

        let send_result = response_sender.send(verification_response);

        match send_result {
            Ok(()) => {}
            Err(crossbeam_channel::SendError(_)) => {
                return;
            }
        }
    }
}
