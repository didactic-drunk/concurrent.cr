require "../spec_helper"
require "../../src/concurrent/cyclic_barrier"

private def test_barrier(barrier1, barrier2, fiber_count)
  barrier1_passed = Atomic.new(0)
  barrier2_passed = Atomic.new(0)

  finished1 = Channel(Bool).new fiber_count
  finished2 = Channel(Bool).new fiber_count

  (fiber_count - 1).times do
    spawn do
      Fiber.yield if rand(2) == 0
      barrier1.wait
      barrier1_passed.add 1
      finished1.send true

      Fiber.yield if rand(2) == 0
      barrier2.wait
      barrier2_passed.add 1
      finished2.send true
    end
  end

  sleep 0.01
  barrier1_passed.get.should eq 0
  barrier2_passed.get.should eq 0

  barrier1.wait
  (fiber_count - 1).times do
    finished1.receive
  end
  finished1.@queue.not_nil!.empty?.should be_true
  finished2.@queue.not_nil!.empty?.should be_true

  barrier1_passed.get.should eq(fiber_count - 1)
  barrier2_passed.get.should eq 0

  barrier2.wait
  (fiber_count - 1).times do
    finished2.receive
  end
  finished1.@queue.not_nil!.empty?.should be_true
  finished2.@queue.not_nil!.empty?.should be_true

  barrier1_passed.get.should eq(fiber_count - 1)
  barrier2_passed.get.should eq(fiber_count - 1)
end

fiber_count = 2000

describe Concurrent::CyclicBarrier do
  it "counts down from a fixed number" do
    barrier1 = Concurrent::CyclicBarrier.new fiber_count
    barrier2 = Concurrent::CyclicBarrier.new fiber_count

    WatchDog.open 2 do
      3.times do
        test_barrier barrier1, barrier2, fiber_count
      end
    end
  end
end
