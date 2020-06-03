require "../spec_helper"
require "../../src/concurrent/enumerable"

describe Enumerable do
  it "parallel map" do
    WatchDog.open 1.5 do
      src = (1..10).to_a
      parallel = src.parallel

      map1 = parallel.map do |num|
        num.to_s
      end

      map2 = map1.map do |num|
        num.to_i
      end

      dst = map2.to_a

      dst.sort.should eq src
    end
  end
end
