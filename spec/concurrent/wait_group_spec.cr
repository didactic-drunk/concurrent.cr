require "../spec_helper"
require "../../src/concurrent/wait_group"

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

fiber_count = 20

describe Concurrent::WaitGroup do
  it "Counts down from a fixed number" do
    latch = Concurrent::WaitGroup.new fiber_count
    WatchDog.open 2 do
      5.times do
        test_latch latch, fiber_count, false
        latch.reset
      end
    end
  end

  it "Counts down from a dynamic number" do
    latch = Concurrent::WaitGroup.new
    WatchDog.open 2 do
      5.times do
        test_latch latch, fiber_count, true
        latch.reset
      end
    end
  end

  it "Counts up" do
    latch = Concurrent::WaitGroup.new 1
    WatchDog.open 2 do
      latch.count_up 5
      latch.count.should eq 6
      6.times do
        latch.count_down
      end
      latch.wait
    end
  end

  it "Can't count_up after release" do
    latch = Concurrent::WaitGroup.new 1
    WatchDog.open 2 do
      latch.count_down
      expect_raises(Concurrent::WaitGroup::Error::CountExceeded) do
        latch.count_up
      end
    end
  end

  describe "Raises when counting down too many times" do
    it "In #count_down and #wait" do
      latch = Concurrent::WaitGroup.new 1
      WatchDog.open 2 do
        latch.count_down

        expect_raises(Concurrent::WaitGroup::Error::CountExceeded) do
          latch.count_down
        end
        expect_raises(Concurrent::WaitGroup::Error::CountExceeded) do
          latch.wait
        end
      end
    end

    it "In #wait_count= and #wait" do
      latch = Concurrent::WaitGroup.new
      WatchDog.open 2 do
        latch.count_down
        latch.count_down

        expect_raises(Concurrent::WaitGroup::Error::CountExceeded) do
          latch.wait_count = 1
        end
        expect_raises(Concurrent::WaitGroup::Error::CountExceeded) do
          latch.wait
        end
      end
    end
  end

  describe "Raises when setting #wait_count= more than once" do
    it "Fixed" do
      latch = Concurrent::WaitGroup.new 1
      WatchDog.open 2 do
        expect_raises(ArgumentError) do
          latch.wait_count = 1
        end
      end
    end

    it "Dynamic" do
      latch = Concurrent::WaitGroup.new
      WatchDog.open 2 do
        latch.wait_count = 1
        expect_raises(ArgumentError) do
          latch.wait_count = 1
        end
      end
    end
  end
end
