#!/usr/bin/env ruby

# RemoteObjectManager
#
# Copyright 2011 Voltaic
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#!!! ChannelMultiplexer.flush_stream needs to heed max_transfer

<<__SYNTAX_COLOR__
# At least in Aquamacs, this makes the next 'here' document get syntax-colored....
__SYNTAX_COLOR__

( <<'__END_REMOTE_OBJECT_MANAGER_SOURCE__'
require 'thread'
require 'monitor'
require 'stringio'

module RemoteObjectManager
  CM_TRACE = false

  class ChannelMultiplexer
    DEFAULT_LATENCY          = 0.5
    LATENCY_LIMITS           = [0.05, 10.0].freeze
    DEFAULT_BUFFER_SIZE      = 256
    BUFFER_SIZE_LIMITS       = [256, 16384].freeze
    STANDARD_RAW_CHANNELS    = [:underlying].freeze
    STANDARD_DATA_CHANNELS   = [:value, :exception].freeze
    ENDPOINT_SYMBOLS         = [:r, :w].freeze
    ALLOCATED_CHANNEL_PREFIX = 'channel_'  # for channels allocated by add_channel with ch == nil

    def initialize(the_underlying, options={})
      # options: :protocol_class, :latency, :buffer_size
      begin
        opt_protocol_class = options.delete(:protocol_class) || DefaultProtocol
        opt_latency        = [ [ LATENCY_LIMITS.first,     (options.delete(:latency).to_f     || DEFAULT_LATENCY)     ].max, LATENCY_LIMITS.last     ].min
        opt_buffer_size    = [ [ BUFFER_SIZE_LIMITS.first, (options.delete(:buffer_size).to_i || DEFAULT_BUFFER_SIZE) ].max, BUFFER_SIZE_LIMITS.last ].min
        @underlying = the_underlying
        @underlying.flush
        @underlying_endpts = make_channel_endpoints(false)  # we need these now, but add_channel requires the protocol object; add_channel is special-cased for :underlying
        underlying_redirection = Redirection.new(@underlying, @underlying_endpts[:w])
        (@redirections ||= []) << underlying_redirection
        @protocol = opt_protocol_class.new(@underlying, opt_latency, opt_buffer_size)
        underlying_redirection.redirect
        @channel_pipes = {}  # non-nil indicates open
        STANDARD_RAW_CHANNELS.each{|ch| add_channel(ch, true)}
        STANDARD_DATA_CHANNELS.each{|ch| add_channel(ch, false)}
        # we were single-threaded until:
        @bg = Thread.new(self){|cm| cm.thread_proc}
      rescue => e
        close(true) rescue nil
        raise e
      end
    end

    def closed?
      !@channel_pipes
    end
    def add_channel(ch=nil, raw=false, existing_stream=nil)
      # ch: channel symbol; nil => allocate new symbol
      # raw: non-false value => string representation of data is put on multiplexer_stream, otherwise data is marshaled into multiplexer_stream
      # existing_stream: stream to redirect to the multiplexer; nil => no redirection
      # returns: [channel_symbol, multiplexer_stream]
      @protocol.synchronize do
        raise StandardError.new("add_channel(ch=#{ch.inspect}, raw=#{raw.inspect}, existing_stream=#{existing_stream.inspect}): ChannelMultiplexer object is closed") if closed?
        raise StandardError.new("add_channel(ch=#{ch.inspect}, raw=#{raw.inspect}, existing_stream=#{existing_stream.inspect}): existing_stream specified for :underlying channel") if ch == :underlying && !existing_stream.nil?
        raise StandardError.new("add_channel(ch=#{ch.inspect}, raw=#{raw.inspect}, existing_stream=#{existing_stream.inspect}): channel must be a symbol") unless ch.is_a?(Symbol)
        raise StandardError.new("add_channel(ch=#{ch.inspect}, raw=#{raw.inspect}, existing_stream=#{existing_stream.inspect}): existing channel specified") if @channel_pipes.has_key?(ch)
        ch = allocate_channel_id if ch.nil?
        endpts = (ch == :underlying) ? @underlying_endpts : make_channel_endpoints(raw)  # needed @underlying_endpts early in the bootstrap process
        if existing_stream
          redirection = Redirection.new(existing_stream, endpts[:w])
          redirection.redirect
          (@redirections ||= []) << redirection
        end
        @channel_pipes[ch] = endpts
        @protocol.notify_add_channel(ch, raw)
        [ch, endpts[:w]]
      end
    end
    def remove_channel(ch)
      @protocol.synchronize do
        raise StandardError.new("closed: remove_channel(#{ch.inspect})") if closed?
        raise StandardError.new("remove_channel(ch=#{ch.inspect}): standard channel specified") if STANDARD_CHANNELS.include?(ch)
        raise StandardError.new("remove_channel(ch=#{ch.inspect}): undefined channel specified") unless @channel_pipes.has_key?(ch)
        raise StandardError.new("remove_channel(ch=#{ch.inspect}): underlying channel specified") if ch == :underlying
        flush  # desynchronize here?  We've used Monitor instead of Mutex, so we don't have to....
        @channel_pipes.delete(ch).channel_pipes.each{|epsym, ep| ep.close rescue nil}
        @protocol.notify_remove_channel(ch)
      end
      nil
    end
    def send(ch, data)
      (puts ">>> send(ch=#{ch.inspect}, data)"; $stdout.flush) if CM_TRACE
      @protocol.synchronize do
        cpipe = @channel_pipes[ch][:w]
        @protocol.write_to_channel_pipe(cpipe, ch, data)
        cpipe.flush
      end
      nil
    end
    def send_value(v)
      send(:value, v)
    end
    def send_exception(e)
      send(:exception, e)
    end
    def context(*args, &block)
      raise StandardError.new("closed: context") if closed?
      raise StandardError.new("no block specified: context") if block.nil?
      begin
        (puts "<<< calling block for send_value"; $stdout.flush) if CM_TRACE
        value = block.call(*args)
        (puts "<<< send_value(value=#{value.inspect})"; $stdout.flush) if CM_TRACE
        send_value(value)
        (puts "<<< returned from send_value(value=#{value.inspect})"; $stdout.flush) if CM_TRACE
      rescue Exception => e  # is Exception too broad?
        (puts "<<< send_exception(e=#{e.inspect})"; $stdout.flush) if CM_TRACE
        send_exception(e)
        (puts "<<< returned from send_exception(e=#{e.inspect})"; $stdout.flush) if CM_TRACE
      end
      nil
    end
    def flush(force=false)
      raise StandardError.new("closed") if closed?
      self.class.flush_ignoring_closed(@channel_pipes, @protocol, force)
    end
    def close(force=false)
      return nil if closed?
      begin
        # Note: we assume that @protocol is not nil if we ever get here
        @protocol.synchronize do
          # First, set to closed state
          cpipes = @channel_pipes
          @channel_pipes = nil  # closed? will return true now
          # Now close the write endpoints so that no new data will arrive from the channels
          cpipes.each{|ch, eps| eps[:w].close rescue nil} if cpipes
          # Then flush all the channel pipes into the protocol
          self.class.flush_ignoring_closed(cpipes, @protocol, force)
          # Now close the read endpoints; the channel pipe ares done with
          cpipes.each{|ch, eps| eps[:r].close rescue nil}
          cpipes = nil  # done with this now
          # Now flush the protocol to the :underlying stream
          @protocol.flush
        end
        if @bg
          # because @channel_pipes is nil, the thread will exit even if @protocol is nil (which would be strange, but anyway...)
          @bg.join
          @bg = nil
        end
        @protocol.synchronize do
          @protocol.close
          @protocol = nil
        end if @protocol
      rescue => e
        raise e unless force
      ensure
        @channel_pipes = nil
        @redirections.reverse.each{|rc| rc.restore} rescue nil  # restore last-to-first
        @redirections = nil
        # don't close the :underlying pipe endpoints until after we've replaced the orginal stream so the parent process does not get an EOF
        if @underlying_endpts
          @underlying_endpts.each{|epsym, ep| ep.close rescue nil}
          @underlying_endpts = nil
        end
      end
      nil
    end

  protected
    class Redirection
      attr_reader :old_io, :new_io, :state
      def initialize(the_old_io, the_new_io)
        @old_io  = the_old_io
        @orig_io = @old_io.dup
        @new_io  = the_new_io
        @state = :init
      end
      def redirect
        @old_io.flush
        @old_io.reopen(@new_io)
        @state = :redirected
      end
      def restore
        if @state == :redirected
          @old_io.flush
          @old_io.reopen(@orig_io)
        end
        @state = :done
      end
    end

    def self.flush_ignoring_closed(cpipes, protocol, force)
      protocol.synchronize do
        begin
          cpipes.each{|ch, eps| eps[:w].flush unless eps[:w].closed?}
          # Now, flush each of the pipes to the underlying stream.
          # However, do so in a round-robin fashion, transmitting
          # buffer_size-sized buffers.  This will make all streams
          # get serviced promptly even if some streams are sending
          # a lot of data.
          loop do
            finished = cpipes.collect do |ch, eps|
              n = 0
              n = ChannelMultiplexer.flush_stream(eps[:r], protocol.buffer_size, protocol.buffer_size){|data| protocol.send_data(ch, data)} unless eps[:r].closed?
              n <= 0
            end
            break if finished.all?
          end
          protocol.flush
        rescue => e
          raise e unless force
        end
      end
      nil
    end
    def make_channel_endpoints(raw=false)
      Hash[ENDPOINT_SYMBOLS.zip(IO.pipe)].freeze
    end
    def allocate_channel_id
      @protocol.synchronize do
        max_n = @channel_pipes.keys.collect{|k| ks = k.to_s; (ks =~ /^#{ALLOCATED_CHANNEL_PREFIX}/).nil? ? nil : ks[ALLOCATED_CHANNEL_PREFIX.length, ks.length-ALLOCATED_CHANNEL_PREFIX.length]}.compact.select{|ks| ki = (ks.to_i rescue 0); ki.to_s == ks && ki >= 0}.collect{|ks| ks.to_i}.max
        next_n = max_n ? max_n+1 : 0
        ("#{ALLOCATED_CHANNEL_PREFIX}#{max_n}").to_sym
      end
    end
    def thread_proc
      loop do
        break if closed?
        start = Time.now
        flush rescue nil unless closed?  #!!! handle error?
        break if closed?
        timeout = @protocol.latency - (Time.now - start)
        sleep(timeout) unless closed? || timeout <= 0.0
      end
    end

  public
    def self.flush_stream(stream, buffer_size=DEFAULT_BUFFER_SIZE, max_transfer=nil, &block)  # returns number of bytes transferred, or -1 if EOF
#!!! use max_transfer
      raise StandardError.new("flush_stream: no block specified") if block.nil?
      n = 0
      calling_block = false
      begin
        stream = StringIO.new(stream) if stream.is_a?(String)
        if stream.is_a?(StringIO)  # convert stream to a String
          loop do
            data = stream.read(buffer_size)
            data_size = data ? data.size : 0
            return -1 if data_size <= 0 && n <= 0  # indicate: EOF
            break if data_size <= 0
            n += data_size
            (calling_block = true; block.call(data); calling_block = false)
          end
        else
          loop do
            read_eps, ignored_write_eps, ignored_error_eps = IO.select([stream], nil, nil, 0.0)  # note: StringIO objects don't work with select
            break unless read_eps
            read_eps.each do |ep|
              unless ep.closed?
                data = stream.read_nonblock(buffer_size)
                data_size = data ? data.length : 0
                break if data_size <= 0
                n += data_size
                (calling_block = true; block.call(data); calling_block = false)
              end
            end
          end
        end
      rescue EOFError => e
        raise if calling_block  # pass exception on; didn't want to keep establishing exception context, so used this flag
        return -1 unless n > 0  # indicate: EOF
      end
      n
    end
    def self.write_to_stream(stream, data)
      n = 0
      while data.length-n > 0
        begin
          n += stream.write_nonblock(n <= 0 ? data : data[n, data.length-n])
        rescue Errno::EINTR, Errno::EAGAIN#!!!, IO::WaitWritable
          IO.select(nil, [stream])  #!!! timeout?
          retry
        end
      end
    end
    FORKEXEC_ACTIONS = {
      :stdin  => { :init=>lambda{|state| state[:pipe] = IO.pipe}, :parent=>lambda{|state| state[:pipe][0].close rescue nil}, :child=>lambda{|state| $stdin.reopen(state[:pipe][0]); state[:pipe][1].close rescue nil},  :endpt=>lambda{|state| state[:pipe][1]} },
      :stdout => { :init=>lambda{|state| state[:pipe] = IO.pipe}, :parent=>lambda{|state| state[:pipe][1].close rescue nil}, :child=>lambda{|state| $stdout.reopen(state[:pipe][1]); state[:pipe][0].close rescue nil}, :endpt=>lambda{|state| state[:pipe][0]} },
      :stderr => { :init=>lambda{|state| state[:pipe] = IO.pipe}, :parent=>lambda{|state| state[:pipe][1].close rescue nil}, :child=>lambda{|state| $stderr.reopen(state[:pipe][1]); state[:pipe][0].close rescue nil}, :endpt=>lambda{|state| state[:pipe][0]} },
    }
    def self.forkexec_with_redirects(cmd, redirects=nil)
      redirects = (redirects ||= []).uniq
      unknown = redirects-FORKEXEC_ACTIONS.keys
      raise StandardError.new('unknown redirect symbols: #{unknown.inspect}') if unknown.count > 0
      states = {}
      redirects.each{|r| FORKEXEC_ACTIONS[r][:init].call(states[r] ||= {})}
      pid = fork do
        redirects.each{|r| FORKEXEC_ACTIONS[r][:child].call(states[r])}
        exec(cmd)
      end
      redirects.each{|r| FORKEXEC_ACTIONS[r][:parent].call(states[r])}
      [pid, Hash[redirects.collect{|r| [r, FORKEXEC_ACTIONS[r][:endpt].call(states[r])]}]]
    end

    class BaseProtocol
      RESERVED_PROTOCOL_CHANNEL = nil  # the channel on which protocol notifications will be sent; nil is a good value because the client looks for an empty channel id for this channel
      attr_reader :latency, :buffer_size

      def initialize(the_marshaler, the_stream, the_latency, the_buffer_size)
        @lock         = Monitor.new  # get this while we're still single-threaded; lazy creation in synchronize() would leave a race condition; note: Monitor allows the same thread to re-enter
        @marshaler    = the_marshaler
        @stream       = the_stream.dup
        @latency      = the_latency
        @buffer_size  = the_buffer_size
        @buffer       = StringIO.new
        @raw_channels = []
      end

      def synchronize(*args, &protected_block)
        @lock.synchronize(*args, &protected_block)
      end
      def raw_channel?(ch)
        @raw_channels.include?(ch)
      end
      def send_start
        # nothing...
      end
      def notify_add_channel(ch, raw)
        @raw_channels << ch if raw
      end
      def notify_remove_channel(ch)
        @raw_channels.delete(ch)
      end
      def send_data(ch, data)
        (puts "<<< send_data(ch=#{ch.inspect}, data=#{data.inspect})"; $stdout.flush) if CM_TRACE
        send_start unless @start_sent
        # We packetize data from raw streams now.
        # It doesn't really matter how we divide the raw stream data into packets, and,
        # besides, we don't necessarily control what data is placed on the raw channel pipes.
        # However, data streams must be packetized as soon as the data is sent to the channel pipe
        # so that the marshaled data is contiguous in the protocol stream.
        # Special case: data for the RESERVED_PROTOCOL_CHANNEL is packetized because that channel
        # has no channel pipe.  See send_notification.
        data = self.class.packetize(ch, data) if raw_channel?(ch) || ch == RESERVED_PROTOCOL_CHANNEL
        @buffer << data
        (puts "<<< send_data: @buffer.pos=#{@buffer.pos.inspect} data=#{data.inspect}"; $stdout.flush) if CM_TRACE
        flush if @buffer.pos >= @buffer_size      
      end
      def send_end
        # nothing...
      end
      def flush
        if @buffer.pos > 0
          ChannelMultiplexer.write_to_stream(@stream, @buffer.string[0, @buffer.pos])
          @buffer.pos = 0
        end
        @stream.flush
      end
      def close
        flush
        @raw_channels = nil
      end
      def write_to_channel_pipe(cpipe, ch, data)  # implements the bulk of ChannelMultiplexer#send
        (puts "<<< write_to_channel_pipe(cpipe=#{cpipe.inspect}, ch=#{ch.inspect}, data=#{data.inspect})"; $stdout.flush) if CM_TRACE
        data = raw_channel?(ch) ? data.to_s : marshaler_dump(data)
        return nil if data.nil?  # exception in marshaler_dump
        # We packetize data from non-raw (i.e., data) streams now so that
        # the marshaled data is contiguous in the protocol stream.
        # Raw streams are packetized in send_data.
        data = self.class.packetize(ch, data) unless raw_channel?(ch)
        ChannelMultiplexer.write_to_stream(cpipe, data)
        (puts "<<< write_to_channel_pipe(cpipe=#{cpipe.inspect}, ch=#{ch.inspect}, data=#{data.inspect})"; $stdout.flush) if CM_TRACE
        nil
      end
    protected
      # marshaler_dump should be overridden to provide a means of recording marshal errors, if any are possible.
      # if such an error occurs, marshaler_dump must return nil.
      def marshaler_dump(obj)
        @marshaler.dump(obj)
      end

    public
      class BaseConnection
        DEFAULT_BOOTSTRAP_REMOTE_HERE_DOC_TOKEN = '__END_REMOTE_OBJECT_MANAGER_SOURCE__'
        DEFAULT_BOOTSTRAP_WAIT_TIMEOUT          = 0.1
        def process  # returns false iff there is nothing left to process
          raise RuntimeError.new(':process must be overrridden')
        end
        def remote_bootstrap(remote_in, remote_object_manager_source, options={})
          # options: :remote_here_doc_token, :code, :return_immediately, :wait_timeout
          remote_here_doc_token = options.delete(:remote_here_doc_token) || DEFAULT_BOOTSTRAP_REMOTE_HERE_DOC_TOKEN
          code                  = options.delete(:code)
          return_immediately    = options.delete(:return_immediately)
          wait_timeout          = options.delete(:wait_timeout) || DEFAULT_BOOTSTRAP_WAIT_TIMEOUT
          ChannelMultiplexer.write_to_stream(remote_in, "\n( <<'#{remote_here_doc_token}'\n")
          ChannelMultiplexer.write_to_stream(remote_in, remote_object_manager_source)  # Note: this is the variable that contains this definition...
          ChannelMultiplexer.write_to_stream(remote_in, "\n#{remote_here_doc_token}\n")
          ChannelMultiplexer.write_to_stream(remote_in, ").tap do |remote_object_manager_source|\n")
          ChannelMultiplexer.write_to_stream(remote_in, "  eval(remote_object_manager_source)\n")
          ChannelMultiplexer.write_to_stream(remote_in, "  RemoteObjectManager.const_set(:SOURCE, remote_object_manager_source)\n")
          ChannelMultiplexer.write_to_stream(remote_in, "end\n")

          if code && code.is_a?(String)
            ChannelMultiplexer.write_to_stream(remote_in, code)
            code = nil
          end
          return if return_immediately
          if code
            remote_in.flush
          else
            remote_in.close rescue nil
            remote_in = nil
          end
          while process do
            if remote_in
              if code && ChannelMultiplexer.flush_stream(code){|data| ChannelMultiplexer.write_to_stream(remote_in, data)} >= 0  # otherwise, code EOF
                remote_in.flush
              else
                remote_in.close rescue nil
                remote_in = nil
              end
            end
            sleep wait_timeout
          end
          nil
        end
        DEFAULT_UNDERLYING_ENDPT = :stderr
        def self.remote_process_bootstrap(command, remote_object_manager_source, code=nil, options={})
#!!! NEVER RETURNS IF code=nil
          # options: :underlying_endpt, :remote_here_doc_token (for remote_bootstrap), :return_immediately (for remote_bootstrap), :wait_timeout (for remote_bootstrap)
          underlying_endpt = options.delete(:underlying_endpt) || DEFAULT_UNDERLYING_ENDPT
          connection_class = self
          pid = endpts = nil
          conn = nil
          begin
            (puts "remote_process_bootstrap: forkexec: command=|#{command}|"; $stdout.flush) if CM_TRACE
            pid, endpts = ChannelMultiplexer.forkexec_with_redirects(command, [:stdin, underlying_endpt])
            conn = connection_class.new(endpts[underlying_endpt])
            options = options.merge(:code=>code) if code
            conn.remote_bootstrap(endpts[:stdin], remote_object_manager_source, options)
            Process.wait(pid) if pid
            return conn.outputs
          ensure
            endpts.each{|r, ep| ep.close rescue nil if ep} if endpts
          end
        end
      end
    end

    class DefaultProtocol < BaseProtocol
      MAJOR_VERSION = 0
      MINOR_VERSION = 1
      MARSHALER_CLASS = Marshal
      NOTIFICATION_ADD_CHANNEL    = :add
      NOTIFICATION_REMOVE_CHANNEL = :remove
      NOTIFICATION_END            = :end
      NOTIFICATION_INTERNAL_ERROR = :internal_error
      SPECIFIER_RAW  = :raw
      SPECIFIER_DATA = :data

      # PROTOCOL STREAM STRUCTURE:
      #   <PROTOCOL_STREAM>          ::= <PROTOCOL_HEADER> <NON_END_CHANNEL_MESSAGE>* <END_CHANNEL_MESSAGE>
      #   <NON_END_CHANNEL_MESSAGE>  ::= <ADD_CHANNEL_MESSAGE>    |
      #                                  <REMOVE_CHANNEL_MESSAGE> |
      #                                  <INTERNAL_ERROR_MESSAGE> |
      #                                  <CHANNEL_DATA_MESSAGE>   |
      #                                  <CHANNEL_RAW_MESSAGE>
      #   <ADD_CHANNEL_MESSAGE>      ::= ch.to_s "\0" timestamp.to_s "\0" data_length.to_s           "\0" [:add,    [ch, <CHANNEL_FORMAT_SPECIFIER>]]
      #   <REMOVE_CHANNEL_MESSAGE>   ::= ch.to_s "\0" timestamp.to_s "\0" data_length.to_s           "\0" [:remove, ch]
      #   <INTERNAL_ERROR_MESSAGE>   ::= ch.to_s "\0" timestamp.to_s "\0" data_length.to_s           "\0" [:internal_error, [location_symbol, internal_error_exception]]
      #   <END_CHANNEL_MESSAGE>      ::= ch.to_s "\0" timestamp.to_s "\0" data_length.to_s           "\0" [:end,    nil]
      #   <CHANNEL_DATA_MESSAGE>     ::= ch.to_s "\0" timestamp.to_s "\0" marshaled_data_length.to_s "\0" marshaled_data
      #   <CHANNEL_RAW_MESSAGE>      ::= ch.to_s "\0" timestamp.to_s "\0" raw_data_length.to_s       "\0" raw_data
      #   <CHANNEL_FORMAT_SPECIFIER> ::= 'raw' | 'data'

      class << self
        def monicker
          @@monicker ||= "#{self.name}({:version=>#{MAJOR_VERSION}.#{MINOR_VERSION}, :marshaler_class=>#{MARSHALER_CLASS.name}, :marshaler_version=>#{MARSHALER_CLASS::MAJOR_VERSION}.#{MARSHALER_CLASS::MINOR_VERSION}})"
        end
        def protocol_header
          @@protocol_header ||= "\n#{monicker} BEGIN\n"
        end
      end

      def initialize(the_stream, the_latency, the_buffer_size)
        super(MARSHALER_CLASS, the_stream, the_latency, the_buffer_size)
        @start_sent = false
        @end_sent   = false
      end

      def send_start
        @buffer << self.class.protocol_header
        @start_sent = true
        super
      end
      def notify_add_channel(ch, raw)
        send_notification(NOTIFICATION_ADD_CHANNEL, [ch, (raw ? SPECIFIER_RAW : SPECIFIER_DATA)])
        super(ch, raw)
      end
      def notify_remove_channel(ch)
        send_notification(NOTIFICATION_REMOVE_CHANNEL, ch)
        super(ch)
      end
      def send_end
        send_start unless @start_sent
        send_notification(NOTIFICATION_END)
        @end_sent = true
        super
      end
      def close
        send_end unless @end_sent || !@start_sent
        super
      end
    protected
      def marshaler_dump(obj)
        super(obj)
      rescue => e
        send_start unless @start_sent
        send_notification(NOTIFICATION_INTERNAL_ERROR, [:marshaler_dump, e])
        return nil  # indicate: exception thrown
      end
      def send_notification(notification, notification_data=nil)
        # Note that we short-circuit the sending of the notification by sending directly
        # to the prptocol stream and bypassing the channel pipes.  This is because
        # we do not allocate a pipe for the RESERVED_PROTOCOL_CHANNEL.  send_data is
        # special-cased accordingly.
        data = marshaler_dump([notification, notification_data])
        send_data(RESERVED_PROTOCOL_CHANNEL, data) unless data.nil?  # data.nil? ==> exception in marshaler_dump
      end
      def self.packetize(ch, data)
        data ||= ''
        raise StandardError.new('data must be String or nil') unless data.is_a?(String)
        # note that symbols cannot contain "\0"
        # note that BaseProtocol::RESERVED_PROTOCOL_CHANNEL == nil so that channel name will be an empty string
        ( ch.to_s                + "\0" +
          Time.now.utc.to_f.to_s + "\0" +
          data.length.to_s       + "\0" +
          data )
      end

    public
      class Connection < BaseProtocol::BaseConnection
        SERVER_PROTOCOL_CLASS = DefaultProtocol
        attr_reader :outputs

        def initialize(the_protocol_stream)
          @protocol_stream = the_protocol_stream.dup
          @outputs         = { BaseProtocol::RESERVED_PROTOCOL_CHANNEL=>{} }
          @active_channels = []
          @raw_channels    = []
          @buffer          = StringIO.new
          @state           = :get_header
          @channel_id      = nil
          @data_size       = nil
          @protocol_ended  = false
        end

        def process  # returns false iff there is nothing left to process
          return false if @protocol_ended
          @protocol_header ||= SERVER_PROTOCOL_CLASS.protocol_header
          loop do
            return false if @protocol_ended
            return true if IO.select([@protocol_stream], nil, nil, 0.0).nil?
            case @state
            when :get_header:
              @buffer << @protocol_stream.read(@protocol_header.length-@buffer.pos)
              loop do  # skip garbage preceding header
                break if @buffer.pos <= 0
                cmplen = [@buffer.pos, @protocol_header.length].min
                break if @buffer.string[0, cmplen] == @protocol_header[0, cmplen]
                # Delete up to the next occurrence of the first character of the protocol header,
                # or the whole string if there is no such occurrence.  @buffer is not empty
                # because of prior conditions, so progress in the loop will be made.
                marker = @buffer.string.index(@protocol_header[0], (@buffer.string[0] == @protocol_header[0] ? 1 : 0))
                @buffer.string = marker && marker < @buffer.length ? @buffer.string[marker, @buffer.length-marker] : ''
                @buffer.pos = @buffer.length
              end
              if @buffer.pos >= @protocol_header.length
                raise if @buffer.pos > @protocol_header.length
                header = @buffer.string[0, @protocol_header.length]
                raise StandardError.new("bad protocol header") if header != @protocol_header
                #!!! CHECK VERSION, ETC
                @buffer.pos = 0
                @state = :get_channel_id
              end
            when :get_channel_id:
              c = @protocol_stream.getc
              raise EOFError.new if c.nil?
              if c != 0
                @buffer.putc(c)
              else
                if @buffer.pos <= 0
                  @channel_id = BaseProtocol::RESERVED_PROTOCOL_CHANNEL
                else
                  channel_id_str = @buffer.string[0, @buffer.pos]
                  @channel_id = channel_id_str.to_sym rescue nil
                  raise StandardError.new("bad channel id") if @channel_id.to_s != channel_id_str
                end
                @buffer.pos = 0
                @state = :get_timestamp
              end
            when :get_timestamp:
              c = @protocol_stream.getc
              raise EOFError.new if c.nil?
              if c != 0
                @buffer.putc(c)
              else
                timestamp_str = @buffer.string[0, @buffer.pos]
                @timestamp = timestamp_str.to_f rescue nil
                raise StandardError.new("bad timestamp") if @timestamp.to_s != timestamp_str
                @buffer.pos = 0
                @state = :get_data_size
              end
            when :get_data_size:
              c = @protocol_stream.getc
              raise EOFError.new if c.nil?
              if c != 0
                @buffer.putc(c)
              else
                data_size_str = @buffer.string[0, @buffer.pos]
                @data_size = data_size_str.to_i rescue nil
                raise StandardError.new("bad data size") if @data_size.to_s != data_size_str
                @buffer.pos = 0
                @state = :get_data
              end
            when :get_data:
              @buffer << @protocol_stream.read(@data_size-@buffer.pos) if @data_size > 0
              if @buffer.pos == @data_size
                data = @buffer.string[0, @buffer.pos]
                data_ok = false
                begin
                  #!!! dangerous:
                  data = SERVER_PROTOCOL_CLASS::MARSHALER_CLASS.load(data) unless @raw_channels.include?(@channel_id)
                  data_ok = true
                rescue => load_error
                  # put a :marshaler_load :internal_error into the BaseProtocol::RESERVED_PROTOCOL_CHANNEL
                  (@outputs[BaseProtocol::RESERVED_PROTOCOL_CHANNEL][@timestamp] ||= []) << [:internal_error, [:marshaler_load, load_error]]
                end
                if data_ok  # otherwise, we put an [:internal_error, [:marshaler_load, load_error]] entry into BaseProtocol::RESERVED_PROTOCOL_CHANNEL
                  if @channel_id == BaseProtocol::RESERVED_PROTOCOL_CHANNEL
                    handle_notification(@channel_id, data)
                  else
                    raise StandardError.new("unknown channel #{@channel_id.inspect}") unless @outputs.has_key?(@channel_id)
                  end
                  (@outputs[@channel_id][@timestamp] ||= []) << data
                  (puts ">>> @outputs[#{@channel_id.inspect}][#{@timestamp.inspect}] << #{data.inspect}"; $stdout.flush) if CM_TRACE
                end
                @buffer.pos = 0
                @state = :get_channel_id
              end
            else
              raise RuntimeError.new("unexpected state encountered: #{@state.inspect}", @state)
            end
          end
          !@protocol_ended
        end

      protected
        def handle_notification(ch, data)
          raise StandardError.new("bad data for add channel notification") unless data && data.is_a?(Array) && data.count == 2
          notification, notification_data = data
          case notification
          when SERVER_PROTOCOL_CLASS::NOTIFICATION_ADD_CHANNEL:
            raise StandardError.new("bad notification_data for add channel notification") unless notification_data && notification_data.is_a?(Array) && notification_data.count == 2
            ch, raw_spec = notification_data
            raise StandardError.new("bad channel specifier for add channel notification #{ch.inspect}") unless ch && ch.is_a?(Symbol)
            raise StandardError.new("bad raw specifier for add channel notification #{raw_spec.inspect}") unless raw_spec && [SERVER_PROTOCOL_CLASS::SPECIFIER_RAW, SERVER_PROTOCOL_CLASS::SPECIFIER_DATA].include?(raw_spec)
            raise StandardError.new("channel added when already present: #{ch.inspect}; active_channels=#{@active_channels.inspect}") if @active_channels.include?(ch)
            @active_channels << ch
            @raw_channels << ch if raw_spec == SERVER_PROTOCOL_CLASS::SPECIFIER_RAW
            @outputs[ch] ||= {}
          when SERVER_PROTOCOL_CLASS::NOTIFICATION_REMOVE_CHANNEL:
            ch = notification_data
            raise StandardError.new("bad channel specifier for add channel notification: #{ch.inspect}") unless ch && ch.is_a?(Symbol)
            raise StandardError.new("channel removed when not present: #{ch.inspect}") unless @active_channels.include?(ch)
            @active_channels.delete(ch)
            @raw_channels.delete(ch)
          when SERVER_PROTOCOL_CLASS::NOTIFICATION_INTERNAL_ERROR:
            location_symbol, internal_error_exception = notification_data
            # nothing to do...
          when SERVER_PROTOCOL_CLASS::NOTIFICATION_END:
            @protocol_ended = true
          else
            raise StandardError.new("unknown notification received", data)
          end
        end
      end
    end
  end

  def self.remote_process_bootstrap(command, remote_object_manager_source, code=nil, options={})
#!!! NEVER RETURNS IF code=nil
    # options: :underlying_endpt (for ChannelMultiplexer::BaseProtocol::remote_process_bootstrap), :remote_here_doc_token (for ChannelMultiplexer::BaseProtocol::remote_bootstrap), :return_immediately (for ChannelMultiplexer::BaseProtocol::remote_bootstrap), :wait_timeout (for ChannelMultiplexer::BaseProtocol::remote_bootstrap)
    ChannelMultiplexer::DefaultProtocol::Connection.remote_process_bootstrap(command, remote_object_manager_source, code, options)
  end
  # remote_process relies on the value of RemoteObjectManager::SOURCE which is
  # the source code for this module.  It is evaled into this module after the
  # source is evaled, closing the definitional loop (through process, I might add....)
  DEFAULT_RAILS_STARTUP_CODE = 'lambda{|rails_root, rails_env| ENV["RAILS_ENV"] = rails_env if rails_env; Dir.chdir(rails_root); require rails_root+"/config/boot"; require rails_root+"/config/environment"}'
  DEFAULT_RAILS_ROOT         = '/u/app/current'
  def self.start_rails(rails_root=nil, options={})
    # options: :rails_env, :rails_startup_code
    rails_root ||= DEFAULT_RAILS_ROOT
    rails_env          = options.delete(:rails_env)          || ENV['RAILS_ENV']
    rails_startup_code = options.delete(:rails_startup_code) || DEFAULT_RAILS_STARTUP_CODE
    rails_startup = "#{rails_startup_code}.call(#{rails_root.inspect}, #{rails_env.inspect})"
    eval(rails_startup)
  end
  def self.remote_process(command, code=nil, options={})
    # options: :load_rails, :rails_root, :rails_env, :rails_startup_code, :underlying_endpt (for ChannelMultiplexer::BaseProtocol::remote_process_bootstrap), :remote_here_doc_token (for ChannelMultiplexer::BaseProtocol::remote_bootstrap), :return_immediately (for ChannelMultiplexer::BaseProtocol::remote_bootstrap), :wait_timeout (for ChannelMultiplexer::BaseProtocol::remote_bootstrap)
    load_rails         = options.delete(:load_rails)
    rails_root         = options.delete(:rails_root)         || DEFAULT_RAILS_ROOT
    rails_env          = options.delete(:rails_env)          || ENV['RAILS_ENV']
    rails_startup_code = options.delete(:rails_startup_code) || DEFAULT_RAILS_STARTUP_CODE
    rails_startup = load_rails ? "#{rails_startup_code}.call(#{rails_root.inspect}, #{rails_env.inspect})\n" : ''
    prefix = <<-'END_CODE_PREFIX'
      cm = nil
      begin
        cm = RemoteObjectManager::ChannelMultiplexer.new($stderr)
        cm.add_channel(:stdout, true, $stdout)
        cm.context do
    END_CODE_PREFIX
    suffix = <<-'END_CODE_SUFFIX'
        end
      ensure
        cm.close if cm
      end
    END_CODE_SUFFIX
    source = code || ''
    (source = ""; ChannelMultiplexer.flush_stream(code){|data| source += data}) if code.is_a?(IO)
    remote_process_bootstrap(command, RemoteObjectManager::SOURCE, rails_startup+prefix+source+suffix, options.merge(:underlying_endpt=>:stderr))
  end
  class InternalError < StandardError; end
  class RemoteError   < StandardError; end
  def self.process_eval(command, code=nil, options={})
    # options: :channel_value_acceptors, :load_rails (for remote_process), :rails_root (for remote_process), :rails_env (for remote_process), :rails_startup_code (for remote_process), :remote_here_doc_token (for ChannelMultiplexer::BaseProtocol::remote_bootstrap), :return_immediately (for ChannelMultiplexer::BaseProtocol::remote_bootstrap), :wait_timeout (for ChannelMultiplexer::BaseProtocol::remote_bootstrap)
    # channel_value_acceptors: if nil, then the default_channel_value_acceptors are used.
    # Otherwise, channel_value_acceptors must be a hash of channels to procs that accept
    # data for that channel as data is replayed timestamp-wise.  If any so-mapped channel
    # maps to nil, then the default implementation from default_channel_value_acceptors
    # is used.  After defaults substitution, any nil-mapped channels are eliminated.
    result = nil
    default_value_acceptor_channels = [ChannelMultiplexer::BaseProtocol::RESERVED_PROTOCOL_CHANNEL, :value, :exception]
    default_channel_value_acceptors = {
      ChannelMultiplexer::BaseProtocol::RESERVED_PROTOCOL_CHANNEL =>
        lambda{|ts, (notification, notification_data)| raise InternalError, [notification, notification_data] if notification == :internal_error},#!!! could be better...
      :value      => lambda{|ts, v| result = v},
      :exception  => lambda{|ts, e| raise RemoteError, e},
      :stderr     => lambda{|ts, v| $stderr.puts(v); $stderr.flush},
      :underlying => lambda{|ts, v| $stdout.puts(v); $stdout.flush},
    }.freeze
    channel_value_acceptors = options.delete(:channel_value_acceptors)
    value_acceptor_channels = channel_value_acceptors ? channel_value_acceptors.keys : default_value_acceptor_channels
    channel_value_acceptors = Hash[value_acceptor_channels.collect{|ch| [ch, channel_value_acceptors && channel_value_acceptors[ch] ? channel_value_acceptors[ch] : default_channel_value_acceptors[ch]]}]
    channel_value_acceptors.reject!{|ch, acceptor| acceptor.nil?}
    remote_process(command, code, options).inject([]) do |history, (ch, timestamped_values)|
      timestamped_values.each do |ts, values|
        values.each do |v|
          history << [ts, [ch, v]]
        end
      end
      history
    end.sort.each do |ts, (ch, v)|
      acceptor = channel_value_acceptors[ch]
      acceptor.call(ts, v) if acceptor
    end
    block_given? ? (yield result) : (return result)
  end
  def self.rails_process_eval(command, code=nil, options={})
    # options: :channel_value_acceptors, :rails_root (for remote_process), :rails_env (for remote_process), :rails_startup_code (for remote_process), :remote_here_doc_token (for ChannelMultiplexer::BaseProtocol::remote_bootstrap), :return_immediately (for ChannelMultiplexer::BaseProtocol::remote_bootstrap), :wait_timeout (for ChannelMultiplexer::BaseProtocol::remote_bootstrap)
    process_eval(command, code, options.merge(:load_rails=>true))
  end
  def self.host_eval(hostname, code=nil, options={})
    # options: :command_generator, :channel_value_acceptors (for process_eval), :load_rails (for remote_process), :rails_root (for remote_process), :rails_env (for remote_process), :rails_startup_code (for remote_process), :remote_here_doc_token (for ChannelMultiplexer::BaseProtocol::remote_bootstrap), :return_immediately (for ChannelMultiplexer::BaseProtocol::remote_bootstrap), :wait_timeout (for ChannelMultiplexer::BaseProtocol::remote_bootstrap)
    command = (options.delete(:command_generator) || lambda{|hostname| "ssh #{hostname.inspect} /usr/bin/env ruby"}).call(hostname)
    process_eval(command, code, options)
  end
  def self.rails_host_eval(hostname, code=nil, options={})
    # options: :command_generator, :channel_value_acceptors (for process_eval), :rails_root (for remote_process), :rails_env (for remote_process), :rails_startup_code (for remote_process), :remote_here_doc_token (for ChannelMultiplexer::BaseProtocol::remote_bootstrap), :return_immediately (for ChannelMultiplexer::BaseProtocol::remote_bootstrap), :wait_timeout (for ChannelMultiplexer::BaseProtocol::remote_bootstrap)
    host_eval(hostname, code, options.merge(:load_rails=>true))
  end
end
__END_REMOTE_OBJECT_MANAGER_SOURCE__
).tap do |remote_object_manager_source|
  eval(remote_object_manager_source)
  RemoteObjectManager.const_set(:SOURCE, remote_object_manager_source)
end

=begin
# EXAMPLE REMOTE CODE
cm = nil
begin
  cm = ChannelMultiplexer.new($stderr)
  cm.add_channel(:stdout, true, $stdout)
  cm.context(10, 11){|*args| args.each{|a| puts "*** #{a}"}; 'hi'}
  cm.context(1, 2, 3){|*args| args.each{|a| puts "+++ #{a}"}; raise StandardError.new('an exception')}
  cm.context{`grep xyzzy xyzzy`; puts "this is on $stdout"; $stderr.puts "this is on $stderr"; 'this is the return value'}
rescue => e
  puts e
  puts e.backtrace
ensure
  cm.close if cm
end
=end

=begin
#RemoteObjectManager::remote_process_bootstrap(command, RemoteObjectManager::SOURCE, $stdin)
=end

=begin
#! /usr/bin/env ruby
# === EXAMPLE REMOTE CODE INVOKATION ===
require "remote_object_manager"
hostname = ARGV[0]
( RemoteObjectManager::host_eval hostname, <<-'END_CODE'
  value = (1..10).collect{|x| "#{`hostname`.strip} / #{`uname -a`.strip}: #{x*x}"}
  puts value.inspect
  value
#  lambda{}
END_CODE
).tap do |result|
  result.each{|v| puts ">>> #{v}"} if result
end
=end
