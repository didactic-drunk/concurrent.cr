require "../spec_helper"
require "../../src/concurrent/count_down_latch"

private def test_latch(latch, fiber_count, wait_set = false)
  # Randomly set the count at the beginning or end.
  if wait_set && rand(2) == 0
    latch.wait_count = fiber_count
    wait_set = false
  end

  finished_count = Atomic.new(0)
  fiber_count.times do |i|
    spawn do
      Fiber.yield if rand(2) == 0
      finished_count.add 1
      latch.count_down
    end
  end

  sleep 0.01 if rand(2) == 0
  latch.wait_count = fiber_count if wait_set
  latch.wait

  finished_count.get.should eq fiber_count
end

fiber_count = 200

describe Concurrent::CountDownLatch do
  it "Counts down from a fixed number" do
    latch = Concurrent::CountDownLatch.new fiber_count
    WatchDog.open 2 do
      5.times do
        test_latch latch, fiber_count, false
        latch.reset
      end
    end
  end

  it "Counts down from a dynamic number" do
    latch = Concurrent::CountDownLatch.new
    WatchDog.open 2 do
      5.times do
        test_latch latch, fiber_count, true
        latch.reset
      end
    end
  end
end
