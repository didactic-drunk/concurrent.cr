require "./wait_group"

@[Deprecated("Use Concurrent::WaitGroup")]
class Concurrent::CountDownLatch < Concurrent::WaitGroup
end
