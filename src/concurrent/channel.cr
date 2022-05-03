require "./stream"

class Channel(T)
  # See `Concurrent::Stream`
  @[Experimental]
  def parallel(*, fibers : Int32 = System.cpu_count.to_i)
    Concurrent::Stream::Source(T, T).new fibers: fibers, dst_vch: self
  end
end
