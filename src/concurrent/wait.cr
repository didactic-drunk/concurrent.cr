# Lower resource version of CountDownLatch.  SWMR.
struct Concurrent::Wait
  # TODO: Change to condition variable.
  @ch = Channel(Nil).new
  @error = Atomic(Exception?).new nil

  def wait : Nil
    @ch.receive?
    if ex = @error.get
      raise ex
    end
  end

  def done : Nil
    @ch.close
  end

  def error(ex : Exception) : Nil
    @error.compare_and_set nil, ex
    @ch.close
  end
end
