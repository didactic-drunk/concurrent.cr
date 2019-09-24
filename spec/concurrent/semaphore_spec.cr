require "../spec_helper"
require "../../src/concurrent/semaphore"
require "atomic"

include Concurrent

describe Semaphore do
  it "fails on nonpositive ints" do
    expect_raises(ArgumentError) { Semaphore.new 0 }
    expect_raises(ArgumentError) { Semaphore.new (-2) }
  end

  it "allows at most n concurrent accesses" do
    n = 20
    n_active = Atomic.new 0
    n_sum = Atomic.new 0
    sem = Semaphore.new n
    close = Channel(Nil).new

    # spawn a lot of fibers
    3000.times do |i|
      spawn do
        Fiber.yield
        sem.acquire do
          x = n_active.add 1
          x.should(be <= n)
        ensure
          n_active.sub 1
          n_sum.add 1
        end
        close.send(nil)
      end
    end

    3000.times { close.receive }

    n_sum.get.should eq(3000)
  end
end
