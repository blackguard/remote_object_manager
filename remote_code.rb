#! /usr/bin/env ruby
require "remote_object_manager"
hostname = ARGV[0]
RemoteObjectManager::host_eval(hostname, <<-'END_CODE'

  ( "from #{`hostname`.strip}:#{Process.pid}:\n" +
    RemoteObjectManager::host_eval('localhost', <<-'END_CODE_2'

      "  from #{`hostname`.strip}:#{Process.pid}:\n" +
      "    hello."

    END_CODE_2
    )
  )

#    class C; end; C  # generate [:internal_error, [:marshaler_load, #<ArgumentError: undefined class/module C>]] on channel nil
#    lambda{}         # generate [:internal_error, [:marshaler_dump, #<TypeError: no marshal_dump is defined for class Proc>]] on channel nil
#    value = (1..10).collect{|x| "#{`hostname`.strip}: #{x*x}"}; puts value.inspect; value
#    $stderr.puts "THIS IS ON THE ERROR CHANNEL"; $stderr.flush#!!!
#    raise

END_CODE
).tap do |result|
  puts "result=#{result.inspect}"
  result.each{|x| puts ">>> #{x}"}
end
