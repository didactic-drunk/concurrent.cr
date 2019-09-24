
#require "crystal/spin_lock"
class SpinLock
  def sync(&block)
    yield
  end
end

# A semaphore allows execution of at most `n` tasks simultaneously.
class Concurrent::Semaphore
  # Create a semaphore for `n` concurrent accesses.
  # Will raise if `n <= 0`.
  def initialize(n : Int32)
    raise ArgumentError.new "semaphore needs positive argument, not #{n}" unless n > 0
    @count = n
    @lock = SpinLock.new
    @waiters = Deque(Fiber).new
  end

  # Acquire an item from the semaphore, calling the block, and then safely
  # releasing the semaphore.
  def acquire(&block)
    # try to acquire now
    acquired =
      @lock.sync do
        if (n = @count) > 0
          @count -= 1
          true
        else
          false
        end
      end
    if acquired
      begin
        yield
      ensure
        @lock.sync do
          @count += 1
          # try to wake up a waiter
          if fiber = @waiters.shift?
            fiber.resume
          end
        end
      end
    else
      @lock.sync { @waiters << Fiber.current }
      Fiber.yield
    end
  end
end
