require "../spec_helper"
require "../../src/concurrent/semaphore"
require "atomic"

describe "semaphore" do
  it "fails on nonpositive ints" do
    expect_raises(ArgumentError) { Semaphore.new 0 }
    expect_raises(ArgumentError) { Semaphore.new (-2) }
  end

  it "allows at most n concurrent accesses" do
    n = 5
    n_active = Atomic.new 0
    sem = Semaphore.new n

    # spawn a lot of fibers
    2000.times do
      spawn do
        sem.acquire do
          x = n_active.add 1
          x.should(be <= n)
        ensure
          n_active.sub 1
        end
      end
    end
  end
end
