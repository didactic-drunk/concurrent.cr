require "../spec_helper"
require "../../src/concurrent/channel"

describe Channel do
  it "parallel map" do
    WatchDog.open 1 do
      src = (1..10).to_a
      ch = Channel(Int32).new src.size + 1
      src.each { |i| ch.send i }
      ch.close

      parallel = ch.parallel

      map1 = parallel.map { |num| num.to_s }
      map2 = map1.map { |num| num.to_i }
      dst = map2.to_a

      dst.sort.should eq src
    end
  end
end
