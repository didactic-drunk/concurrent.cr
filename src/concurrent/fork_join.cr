require "atomic"

# A fork-join pool provides an easy way to spawn a bunch of fibers,
# and then wait for all of them to complete.
class Concurrent::ForkJoin
  protected def initialize
    @close = Channel(Nil).new
    @n = Atomic(Int32).new 0
  end

  # wait for all tasks to be done
  protected def join
    return if @n.get == 0
    @close.receive
  end

  # Start a fork-join pool, and call the block with it.
  def self.new(& : ForkJoin ->)
    f = ForkJoin.new
    begin
      yield f
      Fiber.yield
    ensure
      f.join
    end
  end

  def fork(&block)
    @n.add 1
    # start a new fiber
    spawn do
      block.call
    ensure
      x = @n.sub 1
      raise Exception.new "what" if x < 1
      if x == 1
        @close.send nil
      end
    end
  end
end
