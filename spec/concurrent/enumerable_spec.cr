require "../spec_helper"
require "../../src/concurrent/enumerable"

private class TestError < Exception
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

#  it "parallel map errors" do
  pending "parallel map errors" do
    WatchDog.open 1.5 do
      src = (1..10).to_a
      parallel = src.parallel

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
      dst = src.parallel.select(&.even?).to_a

      dst.sort.should eq src.select(&.even?)
    end
  end

  pending "many more methods" do
  end
end
