require_relative './hash'

class MessageQueue
  #Accepts the Connection Settings hash defined for message_queue in config/settings.yml
  def initialize(config_hash)
    begin
      config_hash = config_hash.symbolize_keys
      @conn = Bunny.new(config_hash)
      @conn.start
    rescue Bunny::PossibleAuthenticationFailureError => e
      puts 'Could not authenticate'
    rescue Bunny::TCPConnectionFailed => e
      puts "Connection to #{@conn.host} failed"
    end
  end

  def connection
    @conn
  end

end