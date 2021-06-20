require "log"
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
# ## Final results and error handling
# All method chains should end with #wait, #serial, or #to_a all of which gather errors and end parallel processing.
# You may omit calling #wait when using #run for background tasks where completion is not guaranteed.
# When used in this fashion make sure to catch all exceptions in the run block or the internal exception channel may fill.
# causing the entire pipeline to stop.
#
# ## Error handling
# Use #wait, #serial, or #to_a receive errors or rescue within any blocks.
# Better handling is a WIP.
#
@[Experimental]
module Concurrent::Stream
  Log = ::Log.for self

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

      src_vch_closed

      while ex = src_ech.receive?
        handle_error ex, src_vch, src_ech, dst_ech
      end

      src_ech_closed
    end

    def handle_error(ex, src_vch, src_ech, dst_ech)
      if dst_ech
        begin
          dst_ech.send(ex)
        rescue Channel::ClosedError
          unhandled_error ex
          src_vch.close
          src_ech.close
          # Log.error(exception: ex) { "ech send failed" }
          #          raise ex
        end
      else
        raise(ex)
      end
    end

    protected def src_vch_closed
    end

    protected def src_ech_closed
    end
  end

  abstract class Base
    @wait = Concurrent::Wait.new
    @parent : Base?

    delegate :wait, to: @wait

    def initialize(*, @parent)
    end

    def unhandled_error(ex : Exception) : Nil
      STDERR.puts "unhandled_error #{ex.inspect}"
      ex.inspect_with_backtrace STDERR
      @wait.error ex
      #      @parent.try &.unhandled_error ex
    end
  end

  # `map`, `select`, `run` and `tee` run in a fiber pool.
  # `batch` runs in a single fiber
  # All other methods "join" in the calling fiber.
  #
  # Exceptions are raised in #each when joined.
  #
  # TODO: better error handling.
  abstract class SendRecv(T, SC) < Base
    include Receive

    @dst_vch : Channel(T)
    @dst_ech : Channel(Exception)

    @scope : Proc(SC)?

    delegate :to_a, to: serial

    def initialize(*, @fibers : Int32, @dst_vch : Channel(T), dst_ech : Channel(Exception)? = nil, parent)
      super(parent: parent)
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
      Serial(T).new @dst_vch, @dst_ech, parent: self
    end

    #    def each(&block : T -> U) forall U
    #     serial.each &block
    #  end

    # Parallel map.  `&block` is evaluated in a fiber pool.
    def map(*, fibers : Int32? = nil, &block : T -> U) forall U
      next_scope = nil
      output = Map(T, U, typeof(next_scope)).new @dst_vch, @dst_ech, fibers: (fibers || @fibers), scope: next_scope, parent: self, &block
      output
    end

    # Parallel select.  `&block` is evaluated in a fiber pool.
    def select(*, fibers : Int32? = nil, &block : T -> Bool)
      output = Select(T).new @dst_vch, @dst_ech, fibers: (fibers || @fibers), parent: self, &block
      output
    end

    # Groups results in to chunks up to the given size.
    # Runs in a single fiber.  Multiple fibers would delay further stream processing.
    def batch(size : Int32, *, flush_interval : Float? = nil, flush_empty : Bool = false)
      raise ArgumentError.new("Size must be positive") if size <= 0

      output = Batch(T, typeof(@dst_vch.receive.not_nil!)).new @dst_vch, @dst_ech, batch_size: size, parent: self, flush_interval: flush_interval, flush_empty: flush_empty
      output
    end

    # Parallel run.  `&block` is evaluated in a fiber pool.
    # Further processing is not possible except for #wait.
    def run(*, fibers : Int32? = nil, &block : T -> _)
      output = Run(T).new @dst_vch, @dst_ech, fibers: (fibers || @fibers), parent: self, &block
      output
    end

    # Parallel tee.  `&block` is evaluated in a fiber pool.
    # The original message is passed to the next Stream.
    def tee(*, fibers : Int32? = nil, &block : T -> _)
      output = Tee(T).new @dst_vch, @dst_ech, fibers: (fibers || @fibers), parent: self, &block
      output
    end

    # Further processing is evaluated within the scope of the returned object.
    def scope(&block : -> U) forall U
      output = Scope(T, U).new @dst_vch, @dst_ech, fibers: @fibers, &block
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

  class Serial(T) < Base
    include ::Enumerable(T)
    include Receive

    def initialize(@src_vch : Channel(T), @src_ech : Channel(Exception), parent)
      super(parent: parent)
    end

    def each
      receive_loop @src_vch, @src_ech, nil do |msg|
        yield msg
      end
    end
  end

  # Input from an Enumerable or Channel.
  class Source(T) < SendRecv(T, Nil)
    def initialize(*, fibers : Int32, dst_vch : Channel(T), dst_ech : Channel(Exception)? = nil)
      super(fibers: fibers, dst_vch: dst_vch, dst_ech: dst_ech, parent: nil)
      set_waiting_fibers 0
    end
  end

  class Map(S, D, SC) < SendRecv(D, SC)
    def initialize(src_vch : Channel(S), src_ech : Channel(Exception), *, fibers : Int32, @scope, parent, &block : S -> D)
      super(fibers: fibers, dst_vch: Channel(D).new, parent: parent)

      spawn_with_close fibers, src_vch, src_ech do
        receive_loop src_vch, src_ech, @dst_ech do |o|
          if sc = @scope
            # with scope.call do
            mo = block.call o # map
            @dst_vch.send mo
            # end
          else
            mo = block.call o # map
            @dst_vch.send mo
          end
        end
      end
    end
  end

  class Select(S) < SendRecv(S, Nil)
    def initialize(src_vch : Channel(S), src_ech : Channel(Exception), *, fibers : Int32, parent, &block : S -> Bool)
      super(fibers: fibers, dst_vch: Channel(S).new, parent: parent)

      spawn_with_close fibers, src_vch, src_ech do
        receive_loop src_vch, src_ech, @dst_ech do |o|
          @dst_vch.send(o) if block.call(o) # select
        end
      end
    end
  end

  class Batch(S, D) < SendRecv(Array(D), Nil)
    @batch : Array(D)
    @batch_size : Int32
    @mutex = Mutex.new

    def initialize(src_vch : Channel(S), src_ech : Channel(Exception), *, @batch_size : Int32, parent, flush_interval, flush_empty : Bool)
      super(fibers: 1, dst_vch: Channel(Array(D)).new, parent: parent)

      @batch = Array(D).new @batch_size

      spawn_with_close 1, src_vch, src_ech do
        receive_loop src_vch, src_ech, @dst_ech do |o|
          if o
            append_obj o
          end
        end
      end

      if flush_interval
        spawn do
          loop do
            sleep flush_interval
            break unless flush_at_interval(flush_empty)
          end
        end
      end
    end

    private def flush_at_interval(flush_empty : Bool) : Bool
      @mutex.synchronize do
        ary = @batch
        return true if ary.empty? && !flush_empty
        @batch = Array(D).new @batch_size
        @dst_vch.send ary
      end

      true
    rescue Channel::ClosedError
      false
    end

    private def append_obj(o) : Nil
      @mutex.synchronize do
        ary = @batch
        ary << o
        if ary.size >= @batch_size
          @batch = Array(D).new @batch_size
          @dst_vch.send ary
        end
      end
    end

    protected def src_vch_closed
      super

      @mutex.synchronize do
        ary = @batch
        @dst_vch.send ary unless ary.empty?
        @batch = Array(D).new 0
      end
    end
  end

  class Run(S) < SendRecv(S, Nil)
    def initialize(src_vch : Channel(S), src_ech : Channel(Exception), *, fibers : Int32, parent, &block : S -> _)
      dst_vch = Channel(S).new.tap &.close
      # todo: leave channel open if error handler provided
      dst_ech = Channel(Exception).new.tap &.close
      super(fibers: fibers, dst_vch: dst_vch, dst_ech: dst_ech, parent: parent)

      spawn_with_close fibers, src_vch, src_ech do
        receive_loop src_vch, src_ech, @dst_ech do |o|
          block.call o
        end
      end
    end
  end

  class Tee(S) < SendRecv(S, Nil)
    def initialize(src_vch : Channel(S), src_ech : Channel(Exception), *, fibers : Int32, parent, &block : S -> _)
      super(fibers: fibers, dst_vch: Channel(S).new, parent: parent)

      spawn_with_close fibers, src_vch, src_ech do
        receive_loop src_vch, src_ech, @dst_ech do |o|
          @dst_vch.send o
          block.call o
        end
      end
    end
  end
end
