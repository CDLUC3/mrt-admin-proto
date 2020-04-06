require 'yaml'

def load_config(name)
  path = File.join(Rails.root, 'config', name)
  raise Exception, "Config file #{name} not found!" unless File.exist?(path)
  raise Exception, "Config file #{name} is empty!" if File.size(path) == 0

  conf     = YAML.load_file(path)
  conf_env = conf[Rails.env]
  conf_env.class == String ? conf[conf_env] : conf_env
end

APP_CONFIG = load_config('app_config.yml')
LDAP_CONFIG = load_config('ldap.yml')
