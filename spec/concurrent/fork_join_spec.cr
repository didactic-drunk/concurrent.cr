require "../spec_helper"
require "../../src/concurrent/fork_join"
require "atomic"

include Concurrent

describe ForkJoin do

  it "joins after all spawns are done" do
    n = 10
    n_done = Atomic.new 0

    ForkJoin.new do |fj|
      # spawn a lot of fibers
      n.times do
        fj.fork do
          n_done.add 1
        end
      end
    end

    n_done.get.should eq(n)
  end
end

