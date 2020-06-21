require "./wait"

# Influenced by [Ruby parallel](https://github.com/grosser/parallel)
# and [Java streams](https://docs.oracle.com/javase/8/docs/api/java/util/stream/Stream.html).
#
# ## Creating a stream:
# * Channel#parallel creates a Stream::Source reading from the Channel.
# * Enumerable#parallel creates a Channel and Stream::Source reading from it.
#
# ## Stream operations:
# * #map { } - Same as Enumerable#map but runs in a fiber pool.
# * #select { } - Same as Enumerable#select but runs in a fiber pool.
# * #batch(size) { } - Groups results in to chunks up to the given size.
# * #run { } - Runs block in a fiber pool.  Further processing is not possible except for #wait.
# * #tee { } - Runs block in a fiber pool passing the original message to the next Stream.
# * #serial - returns an Enumerable collecting results from a parallel Stream.
#
@[Experimental]
module Concurrent::Stream
  # :nodoc:
  module Receive
    protected def receive_loop(src_vch, src_ech, dst_ech) : Nil
      loop do
        begin
          select
          when msg = src_vch.receive
            begin
              yield msg
            rescue ex
              handle_error ex, src_vch, src_ech, dst_ech
            end
          when ex = src_ech.receive
            handle_error ex, src_vch, src_ech, dst_ech
          end
        rescue Channel::ClosedError
          break
        end
      end

      receive_loop_single_channel(src_vch, src_ech, dst_ech) do |msg|
        yield msg
      end
    end

    # One channel is closed. Handle remaining messages.
    protected def receive_loop_single_channel(src_vch, src_ech, dst_ech) : Nil
      loop do
        msg = begin
          src_vch.receive
        rescue Channel::ClosedError
          break
        end

        begin
          yield msg
        rescue ex
          handle_error ex, src_vch, src_ech, dst_ech
        end
      end

      while ex = src_ech.receive?
        handle_error ex, src_vch, src_ech, dst_ech
      end
    end

    def handle_error(ex, src_vch, src_ech, dst_ech)
      if dst_ech
        begin
          dst_ech.send(ex)
        rescue Channel::ClosedError
          # BUG: possibly signal error elsewhere
          src_vch.close
          src_ech.close
          raise ex
        end
      else
        raise(ex)
      end
    end
  end

  # `map`, `select`, `run` and `tee` run in a fiber pool.
  # `batch` runs in a single fiber.
  # All other methods "join" in the calling fiber.
  #
  # Exceptions are raised in #each when joined.
  #
  # TODO: better error handling.
  abstract class Base(T)
    include Receive

    @dst_vch : Channel(T)
    @dst_ech : Channel(Exception)

    @wait = Concurrent::Wait.new

    delegate :to_a, to: serial

    delegate :wait, to: @wait

    def initialize(*, @fibers : Int32, @dst_vch : Channel(T), dst_ech : Channel(Exception)? = nil)
      @dst_ech = dst_ech ||= Channel(Exception).new
      @fibers_remaining = Atomic(Int32).new -1
    end

    protected def set_waiting_fibers(n)
      last, succeed = @fibers_remaining.compare_and_set -1, n
      unless succeed
        raise "#{self.class} can't use .each more than once last=#{last} n=#{n}"
      end
    end

    protected def spawn_with_close(fibers, src_vch, src_ech : Channel(Exception), &block : -> _)
      set_waiting_fibers fibers

      fibers.times do
        spawn do
          block.call
        rescue ex : Exception
          ex.inspect_with_backtrace STDOUT
          abort "not reached #{ex.inspect}"
        ensure
          # Last fiber closes channel.
          if @fibers_remaining.sub(1) == 1
            close
          end
        end
      end
    end

    def serial
      Serial(T).new @dst_vch, @dst_ech
    end

    #    def each(&block : T -> U) forall U
    #     serial.each &block
    #  end

    # Parallel map.  `&block` is evaluated in a fiber pool.
    def map(*, fibers : Int32? = nil, &block : T -> U) forall U
      output = Map(T, U).new @dst_vch, @dst_ech, fibers: (fibers || @fibers), &block
      output
    end

    # Parallel select.  `&block` is evaluated in a fiber pool.
    def select(*, fibers : Int32? = nil, &block : T -> Bool)
      output = Select(T).new @dst_vch, @dst_ech, fibers: (fibers || @fibers), &block
      output
    end

    # Parallel batch.  Runs in a single fiber.  Multiple fibers would delay further stream processing.
    def batch(size : Int32)
      raise ArgumentError.new("Size must be positive") if size <= 0

      output = Batch(T, typeof(@dst_vch.receive.not_nil!)).new @dst_vch, @dst_ech, batch_size: size
      output
    end

    # Parallel run.  `&block` is evaluated in a fiber pool.
    # Further processing is not possible except for #wait.
    def run(*, fibers : Int32? = nil, &block : T -> _)
      output = Run(T).new @dst_vch, @dst_ech, fibers: (fibers || @fibers), &block
      output
    end

    # Parallel tee.  `&block` is evaluated in a fiber pool.
    # The original message is passed to the next Stream.
    def tee(*, fibers : Int32? = nil, &block : T -> _)
      output = Tee(T).new @dst_vch, @dst_ech, fibers: (fibers || @fibers), &block
      output
    end

    def close : Nil
      return if @closed
      @closed = true

      @dst_vch.close
      @dst_ech.close
    ensure
      @wait.done
    end

    # TODO: Implement cancel.
    # def cancel
    # end
  end

  class Serial(T)
    include ::Enumerable(T)
    include Receive

    def initialize(@src_vch : Channel(T), @src_ech : Channel(Exception))
    end

    def each
      receive_loop @src_vch, @src_ech, nil do |msg|
        yield msg
      end
    end
  end

  # Input from an Enumerable or Channel.
  class Source(T) < Base(T)
    def initialize(*, fibers : Int32, dst_vch : Channel(T), dst_ech : Channel(Exception)? = nil)
      super(fibers: fibers, dst_vch: dst_vch, dst_ech: dst_ech)
      set_waiting_fibers 0
    end
  end

  class Map(S, D) < Base(D)
    def initialize(src_vch : Channel(S), src_ech : Channel(Exception), *, fibers : Int32, &block : S -> D)
      super(fibers: fibers, dst_vch: Channel(D).new)

      spawn_with_close fibers, src_vch, src_ech do
        receive_loop src_vch, src_ech, @dst_ech do |o|
          mo = block.call o # map
          @dst_vch.send mo
        end
      end
    end
  end

  class Select(S) < Base(S)
    def initialize(src_vch : Channel(S), src_ech : Channel(Exception), *, fibers : Int32, &block : S -> Bool)
      super(fibers: fibers, dst_vch: Channel(S).new)

      spawn_with_close fibers, src_vch, src_ech do
        receive_loop src_vch, src_ech, @dst_ech do |o|
          @dst_vch.send(o) if block.call(o) # select
        end
      end
    end
  end

  class Batch(S, D) < Base(Array(D))
    def initialize(src_vch : Channel(S), src_ech : Channel(Exception), *, batch_size : Int32)
      super(fibers: 1, dst_vch: Channel(Array(D)).new)

      spawn_with_close 1, src_vch, src_ech do
        ary = nil
        receive_loop src_vch, src_ech, @dst_ech do |o|
          if o
            ary ||= Array(D).new batch_size
            ary << o
            if ary.size >= batch_size
              @dst_vch.send ary
              ary = nil
            end
          end
        end
      end
    end
  end

  class Run(S) < Base(S)
    def initialize(src_vch : Channel(S), src_ech : Channel(Exception), *, fibers : Int32, &block : S -> _)
      dst_vch = Channel(S).new.tap { |ch| ch.close }
      # todo: leave channel open if error handler provided
      dst_ech = Channel(Exception).new.tap { |ch| ch.close }
      super(fibers: fibers, dst_vch: dst_vch, dst_ech: dst_ech)

      spawn_with_close fibers, src_vch, src_ech do
        receive_loop src_vch, src_ech, @dst_ech do |o|
          block.call o
        end
      end
    end
  end

  class Tee(S) < Base(S)
    def initialize(src_vch : Channel(S), src_ech : Channel(Exception), *, fibers : Int32, &block : S -> _)
      super(fibers: fibers, dst_vch: Channel(S).new)

      spawn_with_close fibers, src_vch, src_ech do
        receive_loop src_vch, src_ech, @dst_ech do |o|
          @dst_vch.send o
          block.call o
        end
      end
    end
  end
end
