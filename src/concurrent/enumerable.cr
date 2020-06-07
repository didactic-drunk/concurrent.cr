require "./stream"

module ::Enumerable(T)
  # TODO: better error handling
  # *
  # See `Concurrent::Stream`
  def parallel(*, fibers : Int32 = System.cpu_count.to_i)
    dst_vch = Channel(T).new
    Concurrent::Stream::Source(T).new(fibers: fibers, dst_vch: dst_vch).tap do |parallel|
      spawn do
        self.each do |o|
          dst_vch.send o
        end
      rescue ex
        parallel.@dst_ech.send ex
      ensure
        parallel.close
      end
    end
  end
end
