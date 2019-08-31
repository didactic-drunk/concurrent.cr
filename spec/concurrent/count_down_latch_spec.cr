require "../spec_helper"
require "../../src/concurrent/count_down_latch"

private def test_latch(latch, wait_set = false)
  fiber_count = 10
  finished_count = Atomic.new(0)
  fiber_count.times do |i|
    spawn do
      finished_count.add 1
      latch.count_down
    end
  end

  latch.wait_count = fiber_count if wait_set
  latch.wait

  finished_count.get.should eq fiber_count
end

describe Concurrent::CountDownLatch do
  it "counts down from a fixed number" do
    latch = Concurrent::CountDownLatch.new 10
    WatchDog.open 2 do
      3.times do
        test_latch latch, false
        latch.reset
      end
    end
  end

  it "counts down from a dynamic number" do
    latch = Concurrent::CountDownLatch.new
    WatchDog.open 2 do
      3.times do
        test_latch latch, true
        latch.reset
      end
    end
  end
end
