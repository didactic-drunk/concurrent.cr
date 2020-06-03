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

describe Concurrent::Enumerable do
  it "parallel map" do
    WatchDog.open 1.5 do
      src = (1..10).to_a
      parallel = src.parallel

      map1 = parallel.map { |num| num.to_s }
      map2 = map1.map { |num| num.to_i }
      dst = map2.to_a

      dst.sort.should eq src
    end
  end

  it "parallel map error in map" do
    WatchDog.open 1.5 do
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
    WatchDog.open 1.5 do
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
    WatchDog.open 1.5 do
      src = (1..10).to_a
      dst = src.parallel.select(&.even?).select { |n| n > 5 }.to_a

      dst.sort.should eq src.select(&.even?).select { |n| n > 5 }
    end
  end

  pending "many more methods" do
  end
end
