# concurrent.cr

Modern concurrency tools for Crystal.  Inspired by 
Erlang, Clojure, Scala, Haskell, F#, C#, Java, and classic concurrency patterns which inspired 
[Ruby](https://github.com/ruby-concurrency/concurrent-ruby), 
which inspired [this library](https://github.com/didactic-drunk/concurrent.cr).

Right now there's only a atomic count down latch.  Contributions welcome.

## Installation

1. Add the dependency to your `shard.yml`:

   ```yaml
   dependencies:
     concurrent:
       github: didactic-drunk/concurrent.cr
   ```

2. Run `shards install`

## Usage

```crystal
require "concurrent/countdown_latch"

fiber_count = 10
latch = Concurrent::CountDownLatch.new
10.times do
  spawn do
    # Do work
    latch.count_down
  end
end

latch.wait_count = fiber_count
latch.wait
```

## Development

TODO: Write development instructions here

## Contributing

1. Fork it (<https://github.com/didactic-drunk/concurrent.cr/fork>)
2. **Install a formatting check git hook (ln -sf ../../scripts/git/pre-commit .git/hooks)**
3. Create your feature branch (`git checkout -b my-new-feature`)
4. Commit your changes (`git commit -am 'Add some feature'`)
5. Push to the branch (`git push origin my-new-feature`)
6. Create a new Pull Request

## Contributors

- [Click](https://github.com/didactic-drunk/concurrent.cr/graphs/contributors) or Run `git shortlog --summary --numbered --email`
