
# A semaphore allows execution of at most `n` tasks simultaneously.
class Semaphore
  # Create a semaphore for `n` concurrent accesses.
  # Will raise if `n <= 0`.
  def initialize(n : Int32)
    raise ArgumentError.new "semaphore needs positive argument, not #{n}" unless n>0
    @wait = Channel(Nil).new n
    n.times { @wait.send(nil) }
  end

  # Acquire an item from the semaphore, calling the block, and then safely
  # releasing the semaphore.
  def acquire(&block)
    @wait.receive
    begin
      yield
    ensure
      @wait.send nil
    end
  end
end
