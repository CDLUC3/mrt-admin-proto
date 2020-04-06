class MenuController < ApplicationController
  before_action :set_up_class

  class MyLdap
    include LdapMixin
  end

  def index
    puts(222)
  end

  def test
    puts(111)
    render template: 'menu/test',
      status: 200,
      locals: {
        title: 'my title',
        foo: 3
      }
  end

  def set_up_class
    @LDAP = MyLdap.new(
      host: LDAP_CONFIG['host'],
      port: LDAP_CONFIG['port'],
      base: LDAP_CONFIG['group_base'],
      admin_user: LDAP_CONFIG['admin_user'],
      admin_password: LDAP_CONFIG['admin_password'],
      connect_timeout: LDAP_CONFIG['connect_timeout'],
      minter: LDAP_CONFIG['ark_minter_url']
    )
    puts(@LDAP)
  end

  private


end
