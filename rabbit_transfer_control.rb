require 'daemons'

# Daemonizes the rabbit_transfer.rb script
# Run from cron with something similar to:
# 0 * * * * cd /usr/local/amie_rabbitmq && /usr/local/rvm/wrappers/ruby-2.4.1/ruby rabbit_transfer_control.rb restart -- -c /usr/local/amie_rabbitmq/config/settings.yml
# Restarting regularly from cron as above allows sending and receiving failures due to rabbitmq connection issues to be retried.

client = 'amie'
daemons_options = {app_name: client + '_rabbit_client'}
Daemons.run('/usr/local/amie_rabbitmq/rabbit_transfer.rb',daemons_options)
