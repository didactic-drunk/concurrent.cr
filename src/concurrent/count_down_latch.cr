# Allows fibers to wait until a series of operations performed in other fibers complete.
#
# This class has additional capabilities not found in java or other implementations:
# - `wait_count` may be set at initialization or if not provided to initialize() any time after (only once between resets).
# - Reset is allowed after the latch is released.  The object may be reused but is not a cyclic barrier.
#
# A single atomic counter is used.
# This implementation is 30-40% faster than using a Mutex or Channel::Buffered on Crystal 0.29.0-dev and likely to be faster still when
# channels are thread safe.
class Concurrent::CountDownLatch
  class Error < Exception
    class CountExceeded < Error
    end

    class Internal < Error
    end
  end

  @count : Atomic(Int32) = Atomic.new(0_i32)

  getter wait_count = 0 # The current wait count.  0 means not set.
  @saved_wait_count = 0 # Only set in initialize.
  # TODO: Change atomic to fence.
  @error = Atomic(Exception?).new nil

  # Used for release.
  @queue = Channel(Nil).new(1)

  def initialize(@saved_wait_count = 0)
    @wait_count = @saved_wait_count
    @count.set (@wait_count == 0 ? Int32::MAX : @wait_count)
  end

  # Current count.
  def count
    @count.get
  end

  # Wait until count_down has been called wait_count times.
  # TODO: timeout
  def wait : self
    @queue.receive
    self
  rescue Channel::ClosedError
    if ex = @error.get
      raise ex
    end
    self
  end

  def count_down : Nil
    prev = @count.sub 1
    if prev == 0
      ex = Error::CountExceeded.new "#{Fiber.current} counted past 0 wait_count=#{wait_count} saved_wait_count=#{@saved_wait_count}"
      @error.compare_and_set nil, ex
      raise ex
    end
    release if prev == 1
  end

  # Must be set exactly once and only if not supplied to #initialize
  def wait_count=(wait_count : Int32) : Int32
    raise ArgumentError.new("wait_count <= 0") if wait_count <= 0
    raise ArgumentError.new("wait_count already set") if @wait_count != 0
    @wait_count = wait_count

    sub = Int32::MAX - wait_count
    prev = @count.sub sub
    diff = prev - sub
    if diff == 0
      release
    elsif diff < 0
      # Assert
      ex = Error::CountExceeded.new "Count exceeded.  cnt=#{@count.get} wait_count=#{wait_count}"
      @error.compare_and_set nil, ex
      release
      raise ex
    else
      # Still waiting
    end

    wait_count
  end

  # Use instead of count_down.
  # Stores the first error and raises it when #wait is called.
  def error(ex : Exception) : Nil
    @error.compare_and_set nil, ex
    count_down
  end

  # Only call reset after latch is released or after initialize.
  # Undefined behavior if called between use of count_down and release.
  def reset
    raise Error::Internal.new "unknown state" if @count.get != 0
    @queue = Channel(Nil).new(1)
    @wait_count = @saved_wait_count
    @count.set (@wait_count == 0 ? Int32::MAX : @wait_count)
    self
  end

  protected def release : Nil
    @queue.close
  end
end
