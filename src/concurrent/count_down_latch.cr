# Allows **one** fiber to wait until a series of operations performed in other fibers complete.
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

  getter wait_count = 0 # the current wait count.  0 means not set.
  @saved_wait_count = 0 # only set in initialize
  @past0 = Atomic::Flag.new
  @error = Atomic(Exception?).new nil

  # used for release
  @queue = Channel(Nil).new(1)

  def initialize(@saved_wait_count = 0)
    @wait_count = @saved_wait_count
    if @wait_count != 0
      @count.set @wait_count
      @past0.test_and_set
    end
  end

  # Current count
  def count
    @count.get
  end

  # Wait until count_down has been called wait_count times.
  # TODO: timeout
  def wait
    @queue.receive
    self
  rescue Channel::ClosedError
    self
  end

  def count_down : Nil
    prev = @count.sub 1
    if prev == 0
      # Dynamic @count starts at 0.   Only run if counting past 0 twice.
      raise Error::CountExceeded.new "#{Fiber.current} counted past 0 saved_wait_count=#{@saved_wait_count}" unless @past0.test_and_set
    end
    release if prev == 1
  end

  # Must be set exactly once and only if not supplied to #initialize
  def wait_count=(wait_count : Int32) : Int32
    raise ArgumentError.new("wait_count <= 0") if wait_count <= 0
    raise ArgumentError.new("wait_count already set") if @wait_count != 0
    @wait_count = wait_count

    prev = @count.add wait_count
    diff = wait_count + prev # prev should be negative or 0
    if diff == 0
      release
    elsif diff < 0 || !@past0.test_and_set
      # Assert
      raise Error::CountExceeded.new "counted past 0 cnt=#{@count.get} wait_count=#{wait_count}"
    else
      # Still waiting
    end

    wait_count
  end

  # Only call reset after latch is released or after initialize.
  # Undefined behavior if called between use of count_down and release.
  def reset
    raise Error::Internal.new "unknown state" if @count.get != 0
    @queue = Channel(Nil).new(1)
    @wait_count = @saved_wait_count
    @count.set @wait_count
    @past0.clear if @saved_wait_count == 0
    self
  end

  protected def release : Nil
    @queue.close
  end
end
