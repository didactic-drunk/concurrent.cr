# A synchronization aid that allows a set of fibers to all wait for each other to reach a common barrier point.
class Concurrent::CyclicBarrier
  class Error < Exception
    class Broken < Error
    end
  end

  @count = 0

  # Returns the number of fibers needed to pass the barrier.
  getter parties : Int32

  @lock = Mutex.new
  getter? broken = false

  def initialize(@parties)
    raise ArgumentError.new("parties must be >= 0") if @parties <= 0

    @count = @parties
    @queue = Deque(Fiber).new @parties
  end

  # Wait until #wait has been called by @parties Fibers.
  #
  # TODO:
  # * Accept a timeout
  def wait
    @lock.lock
    c = @count
    if c > 1
      @count = c - 1
      enqueue
    elsif c == 1
      release
    else
      abort "Impossible condition count=#{c} (memory corruption likely)"
    end
    self
  end

  private def enqueue : Nil
    @queue << Fiber.current
    @lock.unlock
    Crystal::Scheduler.reschedule
  end

  private def release : Nil
    @count = @parties
    @queue.each &.enqueue
    @queue.clear
    @lock.unlock
  end
end
