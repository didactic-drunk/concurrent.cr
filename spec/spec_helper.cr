require "spec"

STDOUT.sync = true
STDERR.sync = true

class WatchDog
  @fork_pid : Process?

  def initialize(@timeout : Int32 | Float32)
    @cur_pid = Process.pid
    @fork_pid = fork do
      sleep @timeout
      puts "watchdog killing #{@cur_pid} after #{@timeout}s"
      Process.kill Signal::TERM, @cur_pid
      sleep Math.min(@timeout, 2)
      Process.kill Signal::KILL, @cur_pid rescue nil
    end
  end

  def kill
    @fork_pid.try &.kill(Signal::KILL)
  end

  def self.open(timeout)
    wd = self.new timeout
    yield
  ensure
    wd.try &.kill
  end
end
