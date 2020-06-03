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

  abstract struct Base(T)
    include ::Enumerable(T)
    include Receive

    def initialize(@fibers : Int32)
      @dst_vch = dst_vch = Channel(T).new
      @dst_ech = dst_ech = Channel(Exception).new
    end

    def serial
      Serial(T).new @dst_vch, @dst_ech
    end

    def each(&block : T -> U) forall U
      serial.each &block
    end

    def map(*, fibers : Int32? = nil, &block : T -> U) forall U
      output = Parallel::Map(T, U).new @dst_vch, @dst_ech, fibers: (fibers || @fibers), &block
      output
    end
  end

  struct Serial(T)
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

  # `map` runs in parallel.  All other methods "join" in the calling fiber.
  struct Parallel(T) < Base(T)
    def initialize(obj : ::Enumerable(T), *, fibers : Int32)
      super(fibers: fibers)

      spawn_send obj
    end

    struct Map(S, D) < Base(D)
      def initialize(src_vch : Channel(S), src_ech : Channel(Exception), *, fibers : Int32, &block : S -> D)
        super(fibers)

        spawn_relay fibers, src_vch, src_ech, &block
      end

      private def spawn_relay(fibers, src_vch, src_ech : Channel(Exception), &block : S -> D)
        fibers_remaining = Atomic(Int32).new fibers.to_i

        fibers.times do
          spawn do
            receive_loop(src_vch, src_ech) do |o|
              mo = block.call o # map
              @dst_vch.send mo
            end
          ensure
            # Last fiber closes channel.
            if fibers_remaining.sub(1) == 1
              @dst_vch.close
              @dst_ech.close
            end
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
