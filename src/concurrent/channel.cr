require "./stream"

class Channel(T)
  # See `Concurrent::Stream`
  @[Experimental]
  def parallel(*, fibers : Int32 = System.cpu_count.to_i)
    dst_ech = Channel({Exception, T}).new.tap &.close
    Concurrent::Stream::Source(T, T).new fibers: fibers, dst_vch: self, dst_ech: dst_ech
  end
end
