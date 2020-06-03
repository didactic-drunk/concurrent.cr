module Concurrent::Enumerable
  module Receive
    protected def receive_loop(src_vch, src_ech) : Nil
      loop do
        msg = begin
          src_vch.receive
        rescue Channel::ClosedError
          break
        end

        yield msg
      end
    end
  end

  abstract class Base(T)
    include ::Enumerable(T)
    include Receive

    def initialize(@fibers : Int32)
      @dst_vch = dst_vch = Channel(T).new
      @dst_ech = dst_ech = Channel(Exception).new
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
        ensure
          # Last fiber closes channel.
          if @fibers_remaining.sub(1) == 1
            @dst_vch.close
            @dst_ech.close
          end
        end
      end
    end

    def serial
      Serial(T).new @dst_vch, @dst_ech
    end

    def each(&block : T -> U) forall U
      serial.each &block
    end

    # Parallel map.  `&block` is evaluated in a fiber pool.
    def map(*, fibers : Int32? = nil, &block : T -> U) forall U
      output = Parallel::Map(T, U).new @dst_vch, @dst_ech, fibers: (fibers || @fibers), &block
      output
    end

    # Parallel select.  `&block` is evaluated in a fiber pool.
    def select(*, fibers : Int32? = nil, &block : T -> Bool)
      output = Parallel::Select(T).new @dst_vch, @dst_ech, fibers: (fibers || @fibers), &block
      output
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
      receive_loop @src_vch, @src_ech do |msg|
        yield msg
      end
    end
  end

  # `map` and `select` run in a fiber pool.  All other methods "join" in the calling fiber.
  class Parallel(T) < Base(T)
    def initialize(obj : ::Enumerable(T), *, fibers : Int32)
      super(fibers: fibers)
      set_waiting_fibers 0

      spawn_send obj
    end

    class Map(S, D) < Base(D)
      def initialize(src_vch : Channel(S), src_ech : Channel(Exception), *, fibers : Int32, &block : S -> D)
        super(fibers)

        spawn_with_close fibers, src_vch, src_ech do
          receive_loop src_vch, src_ech do |o|
            mo = block.call o # map
            @dst_vch.send mo
          end
        end
      end
    end

    class Select(S) < Base(S)
      def initialize(src_vch : Channel(S), src_ech : Channel(Exception), *, fibers : Int32, &block : S -> Bool)
        super(fibers)

        spawn_with_close fibers, src_vch, src_ech do
          receive_loop src_vch, src_ech do |o|
            @dst_vch.send(o) if block.call(o) # select
          end
        end
      end
    end

    private def spawn_send(obj)
      spawn do
        obj.each do |o|
          @dst_vch.send o
        end
      rescue ex
        # BUG: rescue
        p ex
        raise ex
      ensure
        @dst_vch.close
        @dst_ech.close
      end
    end
  end
end

module ::Enumerable(T)
  # TODO: error handling
  # *
  # See `Concurrent::Enumerable::Parallel`
  def parallel(*, fibers : Int32 = System.cpu_count.to_i)
    Concurrent::Enumerable::Parallel(T).new self, fibers: fibers
  end
end
