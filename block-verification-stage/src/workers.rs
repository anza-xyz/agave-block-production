pub(crate) mod entry_hash_verification;
pub(crate) mod signature_verification;

use {
    crate::join_thread,
    crossbeam_channel::{Receiver, Sender, bounded},
    shaq::{error::WaitError, mpmc},
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

/// A pool of worker threads, each answering requests from a shared queue.
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
        verify: fn(Request) -> Response,
    ) -> Self {
        let worker_handles = (0..num_workers.get())
            .map(|index| {
                let request_consumer = request_consumer.clone();
                let response_sender = response_sender.clone();
                let exit = shutdown_signal.clone();
                thread::Builder::new()
                    .name(format!("{thread_name_prefix}{index}"))
                    .spawn(move || run_worker(&request_consumer, &response_sender, &exit, verify))
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
    request_consumer: &mpmc::Consumer<Request>,
    response_sender: &Sender<Response>,
    shutdown_signal: &AtomicBool,
    verify: fn(Request) -> Response,
) {
    while !shutdown_signal.load(Ordering::Relaxed) {
        let verification_request = match request_consumer.read_timeout(EXIT_POLL_INTERVAL) {
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
