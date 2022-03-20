require "./stream"

module ::Enumerable(T)
  # TODO: better error handling
  # *
  # See `Concurrent::Stream`
  @[Experimental]
  def parallel(*, fibers : Int32 = System.cpu_count.to_i)
    dst_vch = Channel(T).new
    Concurrent::Stream::Source(T, T | Symbol).new(fibers: fibers, dst_vch: dst_vch).tap do |parallel|
      spawn do
        self.each do |o|
          dst_vch.send o
        end
      rescue Channel::ClosedError # Ignore.  Closed due to error elsewhere
      rescue ex
        tup = {ex, :enumerable_each}
        parallel.@dst_ech.send tup
      ensure
        parallel.close
      end
    end
  end

  def serial : self
  end
end

class Array(T)
  # TODO: better error handling
  # *
  # See `Concurrent::Stream`
  @[Experimental]
  def parallel(*, fibers : Int32 = System.cpu_count.to_i)
    dst_vch = Channel(T).new
    Concurrent::Stream::Source(T, T | Symbol).new(fibers: fibers, dst_vch: dst_vch).tap do |parallel|
      spawn do
        self.each do |o|
          dst_vch.send o
        end
      rescue Channel::ClosedError # Ignore.  Closed due to error elsewhere
      rescue ex
        tup = {ex, :array_each}
        parallel.@dst_ech.send tup
      ensure
        parallel.close
      end
    end
  end
end
