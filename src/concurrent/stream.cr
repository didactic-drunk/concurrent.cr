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
# * #flatten() - TODO
# * #run { } - Runs block in a fiber pool.  Further processing is not possible except for #wait or #detach.
# * #tee { } - Runs block in a fiber pool passing the original message to the next Stream.
# * #errors { } - Runs accumulated errors through block in a fiber pool
# * #serial - returns an Enumerable collecting results from a parallel Stream.
#
# ## Final results and error handling
# All method chains should end with #wait, #serial, or #to_a all of which gather errors and end parallel processing.
# You may #detach instead of #wait when using #run for background tasks where completion is not guaranteed.
# When used in this fashion make sure to catch all exceptions in the run block or the internal exception channel may fill.
# causing the entire pipeline to stop.
#
# ## Error behavior
#
# Errors are passed through to the end of stream or until the first errors call.
# [`#serial`, `#to_a`, or `#run` + `#wait`] will raise on the first error encountered
# closing the pipeline behind it recursively.
# This **may** raise `Channel::ClosedError` in your source
#
# ## Error handling
#
# ### Option 1
# If the entire pipeline must succeed or fail early:
# Use `#serial`, `#to_a`, or `#run` + `#wait` at the end of the pipeline to receive the first error.
#
# ### Option 2
# Make sure there are no errors by rescuing within any blocks.
#
# ### Option 3
# * The first `errors()` call receives errors for first `select` and `map`.
# * The 2nd `errors` call receives errors for the 2nd `map`.
# * `to_a` is guaranteed to succeed (although it may be empty) because all errors were handled
# ```
# src.parallel.select { raise_randomly }.map { raise_randomly }.errors { |ex, obj| }.map { raise_randomly }.errors { }.to_a
# ```
# ### Option 4
# Use a 3rd party Result type and never raise in any block
#
#
@[Experimental]
module Concurrent::Stream
  Log = ::Log.for self

  class Error < Exception
    class Misuse < Error
    end
  end

  # :nodoc:
  module Receive(E)
    protected def spawn_with_close(fibers, src_vch, src_ech : Channel({Exception, E}), *, name = nil, &block : -> _)
      @fibers_remaining.add fibers

      fibers.times do
        spawn_with_close(block, name: name)
      end

      # Assumes this method is the last to spawn fibers
      fibers_remaining_sub
    end

    protected def receive_loop_v_channel(src_vch, src_ech, dst_ech = nil) : Nil
      loop do
        msg = begin
          src_vch.receive
        rescue Channel::ClosedError
          break
        end

        begin
          yield msg
        rescue ex
          tup = {ex, msg}
          handle_error tup, src_vch, src_ech, dst_ech
        end
      end
    end

    protected def receive_loop_e_channel(src_vch, src_ech) : Nil
      while msg = src_ech.receive?
        yield msg
      end
    end

    protected def handle_error(tup, src_vch, src_ech, dst_ech)
      if dst_ech
        begin
          dst_ech.send(tup)
        rescue Channel::ClosedError # Ignore, closed elsewhere
          src_vch.close
        end
      else
        raise(tup[0])
      end
    end

    # Callback
    protected def src_vch_closed
      @dst_vch.close
      @dst_ech.close
      pass_through_parent_wait
    end

    protected def pass_through_parent_wait
      if par = @parent
        par.@wait.wait
      end
    rescue ex
      @wait.error ex
    else
      @wait.done
    end
  end

  module ErrorHandling(E)
    @error_fibers_remaining = Atomic(Int32).new 1

    protected abstract def handle_ancestor_error(ex : Exception, obj : E) : Nil
    protected abstract def ancestor_errors_close : Nil

    protected def error_fibers_remaining_sub : Bool
      if @error_fibers_remaining.sub(1) == 1
        ancestor_errors_close
        true
      else
        false
      end
    end

    protected def receive_ancestors_errors : Nil
      close_block = ->{ error_fibers_remaining_sub }

      begin
        self.errors_attached = true # error handlers are incapable of producing errors
        par = self
        while par = par.try &.parent
          break unless receive_ancestor_errors(par, close_block)
        end
      ensure
        close_block.call
      end
    end

    # Spawns new Fiber recursively until reaches already handled ancestor
    # Return true => Handled by this instance in new fiber
    # Return false => handled by other instance.  Stop recursion
    private def receive_ancestor_errors(par, close_block) : Bool
      if par.errors_attached?
        return false
      end
      par.errors_attached = true

      # race condition w/ closed irrelevant - for perf only
      if par.responds_to?(:dst_ech) && !par.dst_ech.closed?
        @error_fibers_remaining.add 1
        spawn_with_close(name: :ancestor_errors, close_block: close_block) do
          error_handler_loop par
        end
      end

      true
    end

    protected def error_handler_loop(par) : Nil
      while tup = par.dst_ech.receive?
        ex = tup[0]
        obj = tup[1].as(E)
        handle_ancestor_error ex, obj
      end
    end
  end

  abstract class Base
    protected property? child_attached = false
    protected property? errors_attached = false

    @fibers : Int32
    # Not used with Source
    # spawn_with_close(fibers) subs 1
    # stream_next= subs 1
    @fibers_remaining = Atomic(Int32).new 2

    # current + ancestor streams
    @wait = Concurrent::Wait.new
    protected getter parent : Base?
    protected getter stream_next : Base?

    delegate wait, to: @wait

    @closed = false

    # :nodoc;
    abstract def start : Nil

    def initialize(*, @fibers, @parent)
    end

    # Closes self + all ancestor value sending channels.
    #
    # Only use to:
    # * Close the source to stop sending messages. Example: Stopping new messages from entering a queue
    # * Close further parts of the pipeline for messages that are safe to lose.  Example: cron/periodic job that **reliably** handles unprocessed messages. (Closes faster, but messages are lost)
    def close : Nil
      return if @closed
      @closed = true

      @src_vch.close

      @parent.try &.close
    end

    protected def unhandled_error(tup) : Nil
      ex = tup[0]
      STDERR.puts "#{self.class} unhandled_error #{ex.inspect} #{tup[1].inspect}"
      ex.inspect_with_backtrace STDERR
      puts "-----"
      @wait.error ex
    end

    protected def child_attached!
      raise Error::Misuse.new if child_attached?
      self.child_attached = true
    end

    protected def stream_next=(stream)
      @stream_next = stream
      start
      fibers_remaining_sub
      stream
    end

    alias NilBlock = -> Nil

    protected def spawn_with_close(block : NilBlock, name : Symbol?, close_block = nil)
      close_block ||= ->{ fibers_remaining_sub } # Last Fiber closes channel

      spawn do
        block.call
      rescue ex : Exception
        ex.inspect_with_backtrace STDOUT
        puts ""
        Exception.new("exiting").inspect_with_backtrace STDOUT
        abort "#{self.class} #{name} not reached #{ex.inspect}"
      ensure
        close_block.call
      end
    end

    protected def spawn_with_close(close_block = nil, *, name = nil, &block : NilBlock)
      spawn_with_close block, name: name, close_block: close_block
    end

    # Last fiber closes channel
    protected def fibers_remaining_sub : Nil
      r = @fibers_remaining.sub(1)
      if r > 1
        # NOOP
      elsif r == 1
        src_vch_closed
      else
        raise "impossible condition fibers_remaining <= 0 #{@fibers_remaining}"
      end
    end
  end

  # `map`, `select`, `run` and `tee` run in a fiber pool.
  # `batch` runs in a single fiber
  # All other methods "join" in the calling fiber.
  #
  # Exceptions are raised in #each when joined.
  #
  # TODO: better error handling.
  # B=block V=value E=accumulated_error SC=scope
  abstract class SendRecv(B, V, E, SC) < Base
    include Receive(E)

    @dst_vch : Channel(V)
    protected getter dst_ech : Channel({Exception, E})

    @scope : Proc(SC)?

    delegate :to_a, to: serial

    def initialize(*, fibers : Int32, @dst_vch : Channel(V), dst_ech : Channel({Exception, E})? = nil, parent)
      super(fibers: fibers, parent: parent)
      @dst_ech = dst_ech ||= Channel({Exception, E}).new
    end

    def serial
      child_attached!
      output = Serial(V, E).new @dst_vch, @dst_ech, parent: self
      self.stream_next = output
      output.start
      output
    end

    # Parallel map.  `&block` is evaluated in a fiber pool.
    def map(*, fibers : Int32? = nil, &block : V -> U) forall U
      child_attached!
      next_scope = nil
      output = Map(V, U, E, typeof(next_scope)).new @dst_vch, @dst_ech, fibers: (fibers || @fibers), scope: next_scope, parent: self, block: block
      self.stream_next = output
      output
    end

    # Parallel select.  `&block` is evaluated in a fiber pool.
    def select(*, fibers : Int32? = nil, &block : V -> Bool)
      child_attached!
      output = Select(V, E).new @dst_vch, @dst_ech, fibers: (fibers || @fibers), parent: self, block: block
      self.stream_next = output
      output
    end

    # Groups results in to chunks up to the given size.
    # Runs in a single fiber.  Multiple fibers would delay further stream processing.
    def batch(size : Int32, *, flush_interval : Float? = nil, flush_empty : Bool = false)
      child_attached!
      raise ArgumentError.new("Size must be positive") if size <= 0

      flush_interval = case flush_interval
                       in Number    ; flush_interval.try &.seconds
                       in Time::Span; flush_interval
                       in Nil       ; nil
                       end

      output = Batch(V, typeof(@dst_vch.receive.not_nil!), E).new @dst_vch, @dst_ech, batch_size: size, parent: self, flush_interval: flush_interval, flush_empty: flush_empty
      self.stream_next = output
      output
    end

    # Parallel run.  `&block` is evaluated in a fiber pool.
    # Further processing is not possible except for #wait.
    def run(*, fibers : Int32? = nil, &block : V -> Nil)
      child_attached!
      output = Run(V, E).new @dst_vch, @dst_ech, fibers: (fibers || @fibers), parent: self, block: block
      self.stream_next = output
      output.start
      output
    end

    # Parallel tee.  `&block` is evaluated in a fiber pool.
    # The original message is passed to the next Stream.
    def tee(*, fibers : Int32? = nil, &block : V -> Nil)
      child_attached!
      output = Tee(V, E).new @dst_vch, @dst_ech, fibers: (fibers || @fibers), parent: self, block: block
      self.stream_next = output
      output
    end

    # Further processing is evaluated within the scope of the returned object.
    def scope(&block : -> U) forall U
      child_attached!
      raise "broken"
      output = Scope(V, U).new @dst_vch, @dst_ech, fibers: @fibers, &block
      self.stream_next = output
      output
    end

    def errors(*, fibers : Int32? = nil, &block : (Exception, E) -> Nil)
      child_attached!
      # dst_vch pass through
      output = Errors(E, V).new @dst_vch, fibers: (fibers || @fibers), parent: self, block: block
      self.stream_next = output
      output
    end

    protected def dst_vch_send(msg) : Nil
      @dst_vch.send msg
    rescue Channel::ClosedError # Ignore.  Closed, cancelled or closed by error elsewhere
      @src_vch.close
    end

    # TODO: Implement cancel.
    # def cancel
    # end
  end

  # Input from an Enumerable or Channel.
  class Source(T, E) < SendRecv(T, T, E, Nil)
    def initialize(*, fibers : Int32, dst_vch : Channel(T), dst_ech : Channel({Exception, E})? = nil)
      # 1 for stream_next
      @fibers_remaining.set 2
      @wait.done
      dst_ech ||= Channel({Exception, E}).new.tap &.close # closed by default: no errors possible
      super(fibers: fibers, dst_vch: dst_vch, dst_ech: dst_ech, parent: nil)
    end

    def start : Nil
    end

    def stream_next=(stream)
      super stream
      stream
    end
  end

  # Terminates the stream
  # Always call `#wait` or `#detach` on this object.  Not calling `#wait` or `#detach` is undefined.
  abstract class Destination(E) < Base
    include Receive(E)
    include ErrorHandling(E)

    @errors_fibers_remaining = Atomic(Int32).new 1
    @comb_ech = Channel({Exception, E}).new # All ancestor errors forwarded here

    def initialize(fibers, parent)
      super(fibers: fibers, parent: parent)

      receive_ancestors_errors
    end

    protected def src_vch_closed : Nil
      pass_through_parent_wait
    end

    @[Experimental]
    # Likely to be replaced with a Future or async shard later
    #
    # yields nil on success
    # yields Exception on error
    def detach(&block)
      spawn do
        wait
        block.call nil
      rescue ex
        block.call ex
      end
    end

    @[Experimental]
    # Likely to be replaced with a Future or async shard later
    def detach
      detach { |ex| raise(ex) if ex }
    end
  end

  class Serial(T, E) < Destination(E)
    include ::Enumerable(T)

    @start_wait = Wait.new

    def initialize(@src_vch : Channel(T), @src_ech : Channel({Exception, E}), parent)
      super(fibers: 0, parent: parent)
    end

    # :nodoc:
    def start : Nil
      @start_wait.done
    end

    def each
      @start_wait.wait

      receive_loop_both_channels @src_vch, @comb_ech do |msg|
        yield msg
      end

      pass_through_parent_wait
      @wait.wait # may raise
    end

    protected def src_vch_closed : Nil
      # Do nothing.  All waiting/closing done in #each
    end

    protected def ancestor_errors_close : Nil
      @comb_ech.close
    end

    protected def handle_ancestor_error(ex : Exception, obj : E) : Nil
      tup = {ex, obj}
      @comb_ech.send tup
    rescue Channel::ClosedError # closed by error handling somewhere else
    end

    protected def handle_error(ex : Exception, obj : E) : Nil
      @wait.error ex
      @src_vch.close
      raise ex
    end

    # Only used with single fiber
    protected def receive_loop_both_channels(src_vch, src_ech, dst_ech = nil) : Nil
      loop do
        begin
          select
          when msg = src_vch.receive
            begin
              yield msg
            rescue ex
              tup = {ex, msg}
              handle_error tup, src_vch, src_ech, dst_ech
            end
          when tup = src_ech.receive
            handle_error tup, src_vch, src_ech, dst_ech
          end
        rescue Channel::ClosedError
          break
        end
      end

      receive_loop_v_channel(src_vch, src_ech, dst_ech) do |msg|
        yield msg
      end
      receive_loop_e_channel(src_vch, src_ech) do |tup|
        handle_error tup, src_vch, src_ech, dst_ech
      end
    end
  end

  class Map(S, D, E, SC) < SendRecv(D, D, E | D, SC)
    def initialize(@src_vch : Channel(S), @src_ech : Channel({Exception, E}), *, fibers : Int32, @scope, parent, @block : S -> D)
      super(fibers: fibers, dst_vch: Channel(D).new, parent: parent)
    end

    # :nodoc:
    def start : Nil
      spawn_with_close @fibers, @src_vch, @src_ech, name: :map do
        receive_loop_v_channel @src_vch, @src_ech, @dst_ech do |o|
          if sc = @scope
            # with scope.call do
            mo = @block.call o # map
            dst_vch_send mo
            # end
          else
            mo = @block.call o # map
            dst_vch_send mo
          end
        end
      end
    end
  end

  class Select(S, E) < SendRecv(S, S, E, Nil)
    def initialize(@src_vch : Channel(S), @src_ech : Channel({Exception, E}), *, fibers : Int32, parent, @block : S -> Bool)
      super(fibers: fibers, dst_vch: Channel(S).new, parent: parent)
    end

    # :nodoc:
    def start : Nil
      spawn_with_close @fibers, @src_vch, @src_ech do
        receive_loop_v_channel @src_vch, @src_ech, @dst_ech do |o|
          if @block.call(o) # select
            dst_vch_send(o)
          end
        end
      end
    end
  end

  class Batch(SV, DV, E) < SendRecv(Array(DV), Array(DV), E | Array(DV), Nil)
    @batch : Array(DV)
    @batch_size : Int32
    @mutex = Mutex.new

    def initialize(@src_vch : Channel(SV), @src_ech : Channel({Exception, E}), *, @batch_size : Int32, parent, @flush_interval : Time::Span?, @flush_empty : Bool)
      super(fibers: 1, dst_vch: Channel(Array(DV)).new, parent: parent)

      @batch = Array(DV).new @batch_size
    end

    # :nodoc:
    def start : Nil
      spawn_with_close 1, @src_vch, @src_ech do
        receive_loop_v_channel @src_vch, @src_ech, @dst_ech do |o|
          if o
            append_obj o
          end
        end
      end

      if flush_interval = @flush_interval
        spawn do
          loop do
            sleep flush_interval
            flush_at_interval(@flush_empty)
          end
        rescue Channel::ClosedError
        end
      end
    end

    private def flush_at_interval(flush_empty : Bool) : Nil
      @mutex.synchronize do
        ary = @batch
        return true if ary.empty? && !flush_empty
        @batch = Array(DV).new @batch_size
        dst_vch_send ary
      end
    end

    private def append_obj(o) : Nil
      @mutex.synchronize do
        ary = @batch
        ary << o
        if ary.size >= @batch_size
          @batch = Array(DV).new @batch_size
          dst_vch_send ary
        end
      end
    end

    protected def src_vch_closed : Nil
      # mutex not necessary?
      @mutex.synchronize do
        ary = @batch
        unless ary.empty?
          @dst_vch.send ary
          @batch = Array(DV).new 0
        end
      rescue Channel::ClosedError # Ignore
      end

      super
    end
  end

  class Run(S, E) < Destination(E)
    def initialize(@src_vch : Channel(S), @src_ech : Channel({Exception, E}), *, fibers : Int32, @parent : Base, @block : S -> Nil)
      super(fibers: fibers, parent: parent)
      fibers_remaining_sub # No stream_next
    end

    # :nodoc:
    def start : Nil
      spawn_with_close @fibers, @src_vch, @comb_ech, name: :run do
        receive_loop_v_channel @src_vch, @src_ech do |o|
          begin
            @block.call o
          rescue ex
            handle_ancestor_error ex, o # Not technically an ancestor error, but same action
          end
        end
      end
    end

    protected def handle_ancestor_error(ex : Exception, obj : E) : Nil
      @wait.error ex
      @src_vch.close
    end

    protected def ancestor_errors_close : Nil
      @comb_ech.close
    end
  end

  class Tee(S, E) < SendRecv(S, S, E, Nil)
    def initialize(@src_vch : Channel(S), @src_ech : Channel({Exception, E}), *, fibers : Int32, parent, @block : S -> Nil)
      super(fibers: fibers, dst_vch: Channel(S).new, parent: parent)
    end

    # :nodoc:
    def start : Nil
      spawn_with_close @fibers, @src_vch, @src_ech do
        receive_loop_v_channel @src_vch, @src_ech, @dst_ech do |o|
          dst_vch_send o
          @block.call o
        end
      end
    end
  end

  class Scope(S, E, SC) < SendRecv(S, S, E, SC)
    def initialize(@src_vch : Channel(S), src_ech : Channel({Exception, E}), *, fibers : Int32, wait, @block : S -> _)
      super(fibers: fibers, dst_vch: Channel(S).new, parent: parent)
    end

    # :nodoc:
    def start : Nil
      spawn_with_close fibers, @src_vch, src_ech do
        receive_loop_v_channel @src_vch, src_ech, @dst_ech do |o|
          dst_vch_send o
          # BUG: bypass channels
          #          block.call o
        end
      end
    end
  end

  class Errors(B, S) < SendRecv(B, S, S, Nil)
    include ErrorHandling(B)

    @block : (Exception, B) -> Nil

    def initialize(src_vch : Channel(S), *, fibers : Int32, parent, @block : Exception, B -> Nil)
      dech = Channel({Exception, S}).new.tap &.close # Can't produce errors
      # Pass through src_vch
      super(fibers: fibers, dst_vch: src_vch, dst_ech: dech, parent: parent)

      # Need to run this immediately, not in #start.  Otherwise Serial will take up all error channels
      receive_ancestors_errors
    end

    # :nodoc:
    def start : Nil
    end

    protected def handle_ancestor_error(ex : Exception, obj : B) : Nil
      @block.call ex, obj
    end

    protected def ancestor_errors_close : Nil
      pass_through_parent_wait
    end
  end
end
