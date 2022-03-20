require "../spec_helper"
require "../../src/concurrent/enumerable"

private class TestScope < Exception
  def foo(bar)
    bar + 1
  end
end

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
        map.serial.each { |num| }
      end
    end
  end

  it "parallel map error in src each" do
    WatchDog.open 1 do
      src = TestEachFail.new
      parallel = src.parallel(fibers: 2)

      map = parallel.map { |num| num }

      expect_raises(TestError) do
        map.serial.each { |num| }
      end
    end
  end

  it "parallel map error handling" do
    WatchDog.open 1 do
      src = (1..10).to_a

      map = src.parallel(fibers: 2).map do |num|
        num.even? ? raise(TestError.new) : num
      end

      error_count = Atomic(Int32).new 0
      continuing = map.errors do |ex|
        sleep 0.001
        error_count.add 1
      end

      continuing.to_a.size.should eq 5
      error_count.get.should eq 5
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

  it "parallel run error in map" do
    WatchDog.open 1 do
      src = (1..10).to_a
      parallel = src.parallel(fibers: 2)

      map = parallel.map do |num|
        num.even? ? raise(TestError.new) : num
      end

      run = map.run { |num| num }

      expect_raises(TestError) do
        run.wait
      end
    end
  end

  it "parallel run error in run" do
    WatchDog.open 1 do
      src = (1..10).to_a
      parallel = src.parallel(fibers: 2)

      run = parallel.run do |num|
        num.even? ? raise(TestError.new) : num
      end

      expect_raises(TestError) do
        run.wait
      end
    end
  end

  it "parallel batch" do
    WatchDog.open 1 do
      src = [1, nil, 2, nil, nil, 3, nil, 4, nil]
      sum = 0
      batch_size = 2
      batch_count = 0
      src.parallel.batch(batch_size).serial.each do |ary|
        ary.size.should eq batch_size
        sum += ary.sum
        batch_count += 1
      end

      batch_count.should eq 2
      sum.should eq 10
    end
  end

  it "parallel batch error" do
    WatchDog.open 1 do
      src = (1..10).to_a
      parallel = src.parallel(fibers: 2)

      batch = parallel.batch(3).map do |nums|
        nums.sum.even? ? raise(TestError.new) : nums
      end

      expect_raises(TestError) do
        batch.to_a
      end
    end
  end

  it "parallel batch error in src each" do
    WatchDog.open 1 do
      src = TestEachFail.new
      parallel = src.parallel(fibers: 2)

      batch = parallel.batch(3).map { |nums| nums }

      expect_raises(TestError) do
        batch.to_a
      end
    end
  end

  it "partial batch" do
    WatchDog.open 1 do
      src = [1]
      sum = 0
      batch_size = 200
      batch_count = 0
      src.parallel.batch(batch_size).serial.each do |ary|
        ary.size.should eq 1
        sum += ary.sum
        batch_count += 1
      end

      batch_count.should eq 1
      sum.should eq 1
    end
  end

  it "flush interval batch" do
    WatchDog.open 1 do
      src = Channel(Int32).new
      batch_size = 200
      batch_count = 0
      run = src.parallel.batch(batch_size, flush_interval: 0.05, flush_empty: true).run(fibers: 1) do |ary|
        ary.size.should eq 0
        batch_count += 1
      end

      sleep 0.4

      batch_count.should be >= 5
      batch_count.should be <= 10
    end
  end

  pending "scope" do
    WatchDog.open 1 do
      src = [1, 2]
      sum = src.parallel.scope { TestScope.new }.map { |n| foo n }.serial.sum

      sum.should eq 5
    end
  end

  pending "many more methods" do
  end
end
