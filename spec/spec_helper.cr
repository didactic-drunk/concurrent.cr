require "spec"
require "semantic_version"

STDOUT.sync = true
STDERR.sync = true

class WatchDog
  @fork_proc : Process?

  def initialize(@timeout : Int32 | Float32)
    @cur_pid = Process.pid
    @fork_proc = Process.new "sleep '#{@timeout}' && echo 'watchdog killing #{@cur_pid} after #{@timeout}' && kill '#{@cur_pid}'", shell: true
  end

  def kill
    {% if Process.has_method?(:signal) %}
      @fork_proc.try &.signal(Signal::KILL)
    {% else %}
      @fork_proc.try &.kill(Signal::KILL)
    {% end %}
  end

  def self.open(timeout)
    wd = self.new timeout
    yield
  ensure
    wd.try &.kill
  end
end
