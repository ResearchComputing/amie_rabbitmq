require 'daemons'

client = 'amie'
daemons_options = {app_name: client + '_rabbit_client'}
Daemons.run('/usr/local/amie_rabbitmq/rabbit_transfer.rb',daemons_options)




