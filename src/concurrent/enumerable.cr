require "./stream"

module ::Enumerable(T)
  # See `Concurrent::Stream`
  @[Experimental]
  def parallel(*, fibers : Int32 = System.cpu_count.to_i)
    dst_vch = Channel(T).new
    dst_ech = Channel({Exception, T | Symbol}).new
    Concurrent::Stream::Source(T, T | Symbol).new(fibers: fibers, dst_vch: dst_vch, dst_ech: dst_ech).tap do |source|
      spawn do
        self.each do |o|
          dst_vch.send o
        end
      rescue Channel::ClosedError # Ignore.  Closed due to error elsewhere
      rescue ex
        tup = {ex, :enumerable_each}
        source.@dst_ech.send(tup) rescue nil
      ensure
        source.@dst_vch.close
        source.@dst_ech.close
      end
    end
  end

  def serial : self
  end
end

class Array(T)
  # See `Concurrent::Stream`
  @[Experimental]
  def parallel(*, fibers : Int32 = System.cpu_count.to_i)
    dst_vch = Channel(T).new
    dst_ech = Channel({Exception, T | Symbol}).new
    Concurrent::Stream::Source(T, T | Symbol).new(fibers: fibers, dst_vch: dst_vch, dst_ech: dst_ech).tap do |source|
      spawn do
        self.each do |o|
          dst_vch.send o
        end
      rescue Channel::ClosedError # Ignore.  Closed due to error elsewhere
      rescue ex
        tup = {ex, :array_each}
        source.@dst_ech.send(tup) rescue nil
      ensure
        source.@dst_vch.close
        source.@dst_ech.close
      end
    end
  end
end
