require "./enumerable"

class Channel(T)
  private ECH = Channel(Exception).new.tap { |ch| ch.close }

  # TODO: better error handling
  # *
  # See `Concurrent::Enumerable::Parallel`
  def parallel(*, fibers : Int32 = System.cpu_count.to_i)
    Concurrent::Enumerable::Stream(T).new fibers: fibers, dst_vch: self, dst_ech: ECH
  end
end
