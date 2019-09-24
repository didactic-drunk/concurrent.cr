
# TODO: use crystal's spin lock
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
    @count = Atomic(Int32).new n
    @lock = SpinLock.new
    @waiters = Deque(Fiber).new 10
  end

  # Acquire an item from the semaphore, calling the block, and then safely
  # releasing the semaphore.
  def acquire(&block)
    # try to acquire now
    loop do
      n_cur = @count.get

      if n_cur == 0
        @lock.sync { @waiters << Fiber.current }
        Fiber.yield
        break
      end

      n_old, swapped = @count.compare_and_set(n_cur, n_cur-1)
      next if !swapped # race condition

      begin
        yield
      ensure
        @count.add(1)
        # try to wake up a waiter, if any
        if @waiters.size > 1
          if fiber = @lock.sync { @waiters.shift? }
            fiber.resume
          end
        end
      end
      break
    end
    end
end
