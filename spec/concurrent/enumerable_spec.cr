require "../spec_helper"
require "../../src/concurrent/enumerable"

private class TestError < Exception
end

private class TestEachFail
  include Enumerable(Int32)

  def each
    yield 1
    raise TestError.new
  end
end

describe Enumerable do
  it "parallel map" do
    WatchDog.open 1 do
      src = (1..10).to_a
      parallel = src.parallel

      map1 = parallel.map { |num| num.to_s }
      map2 = map1.map { |num| num.to_i }
      dst = map2.to_a

      dst.sort.should eq src
    end
  end

  it "parallel map error in map" do
    WatchDog.open 1 do
      src = (1..10).to_a
      parallel = src.parallel(fibers: 2)

      map = parallel.map do |num|
        num.even? ? raise(TestError.new) : num
      end

      expect_raises(TestError) do
        map.each { |num| }
      end
    end
  end

  it "parallel map error in each" do
    WatchDog.open 1 do
      src = TestEachFail.new
      parallel = src.parallel(fibers: 2)

      map = parallel.map do |num|
        num.even? ? raise(TestError.new) : num
      end

      expect_raises(TestError) do
        map.each { |num| }
      end
    end
  end

  it "parallel select" do
    WatchDog.open 1 do
      src = (1..10).to_a
      dst = src.parallel.select(&.even?).select { |n| n > 5 }.to_a

      dst.sort.should eq src.select(&.even?).select { |n| n > 5 }
    end
  end

  it "parallel tee" do
    WatchDog.open 1 do
      src = (1..10).to_a
      tee_sum = Atomic(Int32).new 0
      sum = src.parallel.select(&.even?).tee { |n| tee_sum.add n }.serial.sum

      tee_sum.get.should eq sum
    end
  end

  it "parallel run" do
    WatchDog.open 1 do
      src = (1..10).to_a
      run_sum = Atomic(Int32).new 0
      run = src.parallel.select(&.even?).run { |n| run_sum.add n }
      run.wait

      run_sum.get.should eq 30
    end
  end

  pending "many more methods" do
  end
end
