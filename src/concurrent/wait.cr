# Lower resource version of CountDownLatch.  SWMR.
struct Concurrent::Wait
  # TODO: Change to condition variable.
  @ch = Channel(Nil).new

  def initialize
  end

  def wait
    @ch.receive?
  end

  def done
    @ch.close
  end
end
