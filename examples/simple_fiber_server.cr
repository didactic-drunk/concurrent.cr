require "log"
require "socket"
require "../src/concurrent/channel"

## Example server with a finite number of fibers
# Use to process a message queue or other process with a limited number of concurrent clients/tasks

## Graceful shutdown
# Signal.trap closes listening server_sock
# After server_sock's last accept, close client_ch
# .parallel fibers consume the channel, exiting when closed
# .wait at eof waits for all .parallel fibers to exit

Log.setup_from_env

server_sock = TCPServer.new 8392
client_ch = Channel(TCPSocket).new

Signal::HUP.trap { server_sock.close }
Signal::QUIT.trap { server_sock.close }
Signal::TERM.trap { server_sock.close }

spawn do
  while sock = server_sock.accept
    client_ch.send sock
  end
ensure
  client_ch.close
end

def handle_client(sock)
  Log.error { sock.inspect }
ensure
  sock.close rescue nil
end

client_ch.parallel(fibers: 10).run { |sock| handle_client(sock) }.wait

