require "atomic"
require "./count_down_latch"

# A fork-join pool provides an easy way to spawn a bunch of fibers,
# and then wait for all of them to complete.
class Concurrent::ForkJoin
  protected def initialize
    @wait = Channel(Nil).new 1
    @all_started = false  # becomes true after the block calling `fork` is done
    @n = Atomic(Int32).new 0
  end

  protected def all_started
    @all_started = true
  end

  # wait for all tasks to be done
  protected def join
    return if @all_started && @n.get == 0
    @wait.receive
  end

  # Start a fork-join pool, and call the block with it.
  def self.new(& : ForkJoin ->)
    f = ForkJoin.new
    begin
      yield f
      f.all_started
      Fiber.yield
    ensure
      f.join
    end
  end

  def fork(&block)
    raise Exception.new "fork join: already done" if @all_started
    @n.add 1
    # start a new fiber
    spawn do
      block.call
    ensure
      prev_n = @n.sub 1
      raise Exception.new "fork join: internal error" if prev_n < 1
      if @all_started && prev_n == 1
        @wait.send nil
      end
    end
  end
end
