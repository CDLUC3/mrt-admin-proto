module QueryHelper

  def arkLink(ark)
    ark = URI.escape(ark, Regexp.new("[^#{URI::PATTERN::UNRESERVED}]"))
    return '' if ENV['MERRITT_URL'] == '' || ENV['MERRITT_URL'] == NIL
    "#{ENV['MERRITT_URL']}/m/#{ark}"
  end

  def collLink(mnemonic)
    return '' if ENV['MERRITT_URL'] == ''
    return '' if mnemonic == '' || mnemonic == NIL
    "#{ENV['MERRITT_URL']}/m/#{mnemonic}"
  end
end
